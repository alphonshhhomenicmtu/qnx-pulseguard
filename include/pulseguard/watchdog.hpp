#pragma once

#include "pulseguard/histogram.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>

namespace pulseguard {

enum class WorkerState : uint8_t {
    OK = 0,
    WARN = 1,
    BREACH = 2,
    RECOVERING = 3,
    RESTARTED = 4,
};

enum class FaultReason : uint8_t {
    NONE = 0,
    WARN_JITTER = 1,
    PRIORITY_INVERSION = 2,
    DEADLINE_BREACH = 3,
    CPU_STALL = 4,
    MISSED_PULSE = 5,
    RESPAWN_COMPLETE = 6,
    HYSTERESIS_CLEAR = 7,
};

inline constexpr const char* worker_state_name(WorkerState s) noexcept {
    switch (s) {
        case WorkerState::OK:
            return "OK";
        case WorkerState::WARN:
            return "WARN";
        case WorkerState::BREACH:
            return "BREACH";
        case WorkerState::RECOVERING:
            return "RECOVERING";
        case WorkerState::RESTARTED:
            return "RESTARTED";
    }
    return "UNKNOWN";
}

inline constexpr const char* fault_reason_name(FaultReason r) noexcept {
    switch (r) {
        case FaultReason::NONE:
            return "NONE";
        case FaultReason::WARN_JITTER:
            return "WARN_JITTER";
        case FaultReason::PRIORITY_INVERSION:
            return "PRIORITY_INVERSION";
        case FaultReason::DEADLINE_BREACH:
            return "DEADLINE_BREACH";
        case FaultReason::CPU_STALL:
            return "CPU_STALL";
        case FaultReason::MISSED_PULSE:
            return "MISSED_PULSE";
        case FaultReason::RESPAWN_COMPLETE:
            return "RESPAWN_COMPLETE";
        case FaultReason::HYSTERESIS_CLEAR:
            return "HYSTERESIS_CLEAR";
    }
    return "UNKNOWN";
}

template <std::size_t N>
inline void copy_cstr(char (&dst)[N], const char* src) noexcept {
    if (N == 0) {
        return;
    }
    if (!src) {
        dst[0] = '\0';
        return;
    }
    std::size_t i = 0;
    for (; i + 1 < N && src[i] != '\0'; ++i) {
        dst[i] = src[i];
    }
    dst[i] = '\0';
}

struct WorkerConfig {
    uint8_t worker_id{0};
    char name[32]{};
    int nominal_priority{20};
    int ceiling_priority{45};
    uint64_t period_us{2000};
    uint64_t warn_deadline_us{3500};
    uint64_t breach_deadline_us{6000};
    uint32_t warn_hysteresis{3};
    uint32_t breach_consecutive_limit{1};
};

struct WatchdogEvent {
    uint64_t ts_ns{0};
    uint8_t worker_id{0};
    char worker_name[32]{};
    WorkerState from_state{WorkerState::OK};
    WorkerState to_state{WorkerState::OK};
    FaultReason reason{FaultReason::NONE};
    uint64_t latency_us{0};
    uint64_t recovery_us{0};
    uint32_t epoch{0};
    bool inversion_active{false};
};

struct WorkerStatus {
    bool registered{false};
    bool armed{false};
    WorkerConfig config{};
    WorkerState state{WorkerState::OK};
    int current_priority{20};
    bool priority_inversion_detected{false};
    uint32_t epoch{1};
    uint32_t expected_epoch{1};
    uint64_t pulses_received{0};
    uint64_t missed_deadlines{0};
    uint64_t warn_events{0};
    uint64_t breach_events{0};
    uint64_t restarts{0};
    uint64_t completed_recoveries{0};
    uint64_t inversion_escalations{0};
    uint64_t last_latency_us{0};
    uint64_t last_gap_us{0};
    uint64_t last_recovery_us{0};
    uint64_t max_recovery_us{0};
    uint64_t last_pulse_ns{0};
    uint64_t fault_detected_ns{0};
    uint32_t consecutive_ok{0};
    uint32_t consecutive_breach{0};
    JitterHistogram histogram{};
};

// Fixed-capacity deterministic multi-worker deadline watchdog.
// Supports OK -> WARN -> BREACH -> RECOVERING -> RESTARTED -> OK transitions,
// hysteresis damping, priority-inversion ceiling escalation, and sub-10ms recovery tracking.
class DeadlineWatchdog {
public:
    static constexpr std::size_t kMaxWorkers = 16;
    static constexpr std::size_t kEventHistoryCap = 64;

    using RespawnCallback = bool (*)(uint8_t worker_id, uint32_t next_epoch, void* ctx);
    using EscalateCallback = bool (*)(uint8_t worker_id, int target_prio, void* ctx);

    DeadlineWatchdog() noexcept = default;

    void set_callbacks(RespawnCallback respawn_cb, EscalateCallback escalate_cb,
                       void* ctx) noexcept {
        respawn_cb_ = respawn_cb;
        escalate_cb_ = escalate_cb;
        cb_ctx_ = ctx;
    }

    bool register_worker(const WorkerConfig& cfg) noexcept {
        if (cfg.worker_id >= kMaxWorkers) {
            return false;
        }
        WorkerStatus& w = workers_[cfg.worker_id];
        if (!w.registered) {
            ++worker_count_;
        }
        w = WorkerStatus{};
        w.registered = true;
        w.armed = false;
        w.config = cfg;
        if (w.config.name[0] == '\0') {
            copy_cstr(w.config.name, "worker");
        }
        w.current_priority = cfg.nominal_priority;
        w.epoch = 1;
        w.expected_epoch = 1;
        w.state = WorkerState::OK;
        return true;
    }

    void on_pulse(uint8_t worker_id, uint64_t ipc_latency_us, uint64_t recv_ns,
                  uint32_t epoch, bool is_restart_handshake,
                  bool inversion_signal) noexcept {
        if (worker_id >= kMaxWorkers) {
            return;
        }
        WorkerStatus& w = workers_[worker_id];
        if (!w.registered) {
            return;
        }

        // Ignore stale pre-restart pulses while awaiting the next epoch handshake.
        // Full-width compare: epoch 17 must not accept stale epoch 1 just because
        // both share the low nibble 0x1.
        if (w.state == WorkerState::RECOVERING && !is_restart_handshake) {
            return;
        }
        if (w.state == WorkerState::RECOVERING && is_restart_handshake &&
            epoch != w.expected_epoch) {
            return;
        }

        uint64_t gap_us = ipc_latency_us;
        if (w.armed && w.last_pulse_ns > 0 && recv_ns >= w.last_pulse_ns) {
            gap_us = (recv_ns - w.last_pulse_ns) / 1000ULL;
        }
        w.armed = true;
        w.last_pulse_ns = recv_ns;
        w.last_latency_us = ipc_latency_us;
        w.last_gap_us = gap_us;
        ++w.pulses_received;
        w.histogram.record(ipc_latency_us);

        if (w.state == WorkerState::RECOVERING && is_restart_handshake) {
            const uint64_t rec_us = (recv_ns > w.fault_detected_ns)
                                        ? (recv_ns - w.fault_detected_ns) / 1000ULL
                                        : 0ULL;
            w.epoch = w.expected_epoch;
            ++w.completed_recoveries;
            w.last_recovery_us = rec_us;
            w.max_recovery_us = std::max(w.max_recovery_us, rec_us);
            w.priority_inversion_detected = false;
            w.current_priority = w.config.nominal_priority;
            w.consecutive_breach = 0;
            w.consecutive_ok = 1;
            transition(w, WorkerState::RESTARTED, FaultReason::RESPAWN_COMPLETE, recv_ns,
                       ipc_latency_us, rec_us);
            return;
        }

        if (inversion_signal) {
            escalate_worker(w, recv_ns, ipc_latency_us);
        }

        const uint64_t effective_delay_us = std::max(ipc_latency_us, gap_us);
        const bool is_breach = effective_delay_us > w.config.breach_deadline_us;
        const bool is_warn = !is_breach && (effective_delay_us > w.config.warn_deadline_us ||
                                            inversion_signal);

        if (is_breach) {
            ++w.missed_deadlines;
            ++w.consecutive_breach;
            w.consecutive_ok = 0;
            if (w.consecutive_breach >= std::max<uint32_t>(1U, w.config.breach_consecutive_limit)) {
                trigger_breach_and_recovery(
                    w, recv_ns, effective_delay_us,
                    ipc_latency_us > w.config.breach_deadline_us ? FaultReason::CPU_STALL
                                                                 : FaultReason::DEADLINE_BREACH);
            }
            return;
        }

        w.consecutive_breach = 0;

        if (is_warn) {
            w.consecutive_ok = 0;
            if (w.state == WorkerState::OK || w.state == WorkerState::RESTARTED) {
                ++w.warn_events;
                transition(w, WorkerState::WARN,
                           inversion_signal ? FaultReason::PRIORITY_INVERSION
                                            : FaultReason::WARN_JITTER,
                           recv_ns, effective_delay_us, 0);
            }
            return;
        }

        // Healthy pulse
        ++w.consecutive_ok;
        if (w.state == WorkerState::WARN || w.state == WorkerState::RESTARTED) {
            const uint32_t hyst = std::max<uint32_t>(1U, w.config.warn_hysteresis);
            if (w.consecutive_ok >= hyst) {
                w.priority_inversion_detected = false;
                w.current_priority = w.config.nominal_priority;
                transition(w, WorkerState::OK, FaultReason::HYSTERESIS_CLEAR, recv_ns,
                           ipc_latency_us, 0);
            }
        }
    }

    void report_priority_inversion(uint8_t worker_id, uint64_t now_ns,
                                   int low_prio_holder) noexcept {
        if (worker_id >= kMaxWorkers) {
            return;
        }
        WorkerStatus& w = workers_[worker_id];
        if (!w.registered) {
            return;
        }
        if (low_prio_holder < w.config.nominal_priority) {
            escalate_worker(w, now_ns, w.last_latency_us);
            if (w.state == WorkerState::OK) {
                ++w.warn_events;
                w.consecutive_ok = 0;
                transition(w, WorkerState::WARN, FaultReason::PRIORITY_INVERSION, now_ns,
                           w.last_latency_us, 0);
            }
        }
    }

    std::size_t tick(uint64_t now_ns) noexcept {
        std::size_t actions = 0;
        for (std::size_t i = 0; i < kMaxWorkers; ++i) {
            WorkerStatus& w = workers_[i];
            if (!w.registered || !w.armed || w.last_pulse_ns == 0) {
                continue;
            }
            if (now_ns <= w.last_pulse_ns) {
                continue;
            }
            const uint64_t silent_us = (now_ns - w.last_pulse_ns) / 1000ULL;
            if (w.state == WorkerState::OK || w.state == WorkerState::WARN ||
                w.state == WorkerState::RESTARTED) {
                if (silent_us > w.config.breach_deadline_us) {
                    ++w.missed_deadlines;
                    trigger_breach_and_recovery(w, now_ns, silent_us, FaultReason::MISSED_PULSE);
                    ++actions;
                } else if (silent_us > w.config.warn_deadline_us &&
                           w.state == WorkerState::OK) {
                    ++w.warn_events;
                    w.consecutive_ok = 0;
                    transition(w, WorkerState::WARN, FaultReason::WARN_JITTER, now_ns,
                               silent_us, 0);
                    ++actions;
                }
            }
        }
        return actions;
    }

    const WorkerStatus* worker(uint8_t worker_id) const noexcept {
        if (worker_id >= kMaxWorkers || !workers_[worker_id].registered) {
            return nullptr;
        }
        return &workers_[worker_id];
    }

    std::size_t worker_count() const noexcept { return worker_count_; }

    uint64_t total_restarts() const noexcept {
        uint64_t sum = 0;
        for (const auto& w : workers_) {
            if (w.registered) {
                sum += w.restarts;
            }
        }
        return sum;
    }

    uint64_t total_completed_recoveries() const noexcept {
        uint64_t sum = 0;
        for (const auto& w : workers_) {
            if (w.registered) {
                sum += w.completed_recoveries;
            }
        }
        return sum;
    }

    uint64_t total_inversions() const noexcept {
        uint64_t sum = 0;
        for (const auto& w : workers_) {
            if (w.registered) {
                sum += w.inversion_escalations;
            }
        }
        return sum;
    }

    uint64_t last_recovery_us() const noexcept {
        uint64_t latest_ts = 0;
        uint64_t rec = 0;
        for (const auto& w : workers_) {
            if (w.registered && w.restarts > 0 && w.fault_detected_ns >= latest_ts) {
                latest_ts = w.fault_detected_ns;
                rec = w.last_recovery_us;
            }
        }
        return rec;
    }

    uint64_t event_total() const noexcept { return event_write_idx_; }

    std::size_t copy_recent_events(WatchdogEvent* out, std::size_t max_out) const noexcept {
        if (!out || max_out == 0) {
            return 0;
        }
        const std::size_t avail = static_cast<std::size_t>(
            std::min<uint64_t>(event_write_idx_, kEventHistoryCap));
        const std::size_t count = std::min(avail, max_out);
        const uint64_t start = event_write_idx_ - count;
        for (std::size_t i = 0; i < count; ++i) {
            out[i] = events_[(start + i) % kEventHistoryCap];
        }
        return count;
    }

private:
    void escalate_worker(WorkerStatus& w, uint64_t now_ns, uint64_t latency_us) noexcept {
        if (!w.priority_inversion_detected) {
            w.priority_inversion_detected = true;
            ++w.inversion_escalations;
            if (w.current_priority < w.config.ceiling_priority) {
                w.current_priority = w.config.ceiling_priority;
                if (escalate_cb_) {
                    (void)escalate_cb_(w.config.worker_id, w.config.ceiling_priority, cb_ctx_);
                }
            }
            record_event(now_ns, w, w.state, w.state, FaultReason::PRIORITY_INVERSION,
                         latency_us, 0);
        }
    }

    void trigger_breach_and_recovery(WorkerStatus& w, uint64_t now_ns, uint64_t latency_us,
                                     FaultReason reason) noexcept {
        ++w.breach_events;
        w.fault_detected_ns = now_ns;
        transition(w, WorkerState::BREACH, reason, now_ns, latency_us, 0);

        ++w.restarts;
        w.expected_epoch = w.epoch + 1;
        w.consecutive_ok = 0;
        w.consecutive_breach = 0;

        if (escalate_cb_ && w.current_priority < w.config.ceiling_priority) {
            w.current_priority = w.config.ceiling_priority;
            (void)escalate_cb_(w.config.worker_id, w.config.ceiling_priority, cb_ctx_);
        }
        if (respawn_cb_) {
            (void)respawn_cb_(w.config.worker_id, w.expected_epoch, cb_ctx_);
        }
        transition(w, WorkerState::RECOVERING, reason, now_ns, latency_us, 0);
    }

    void transition(WorkerStatus& w, WorkerState next, FaultReason reason, uint64_t now_ns,
                    uint64_t latency_us, uint64_t recovery_us) noexcept {
        const WorkerState prev = w.state;
        w.state = next;
        record_event(now_ns, w, prev, next, reason, latency_us, recovery_us);
    }

    void record_event(uint64_t now_ns, const WorkerStatus& w, WorkerState from, WorkerState to,
                      FaultReason reason, uint64_t latency_us,
                      uint64_t recovery_us) noexcept {
        WatchdogEvent ev{};
        ev.ts_ns = now_ns;
        ev.worker_id = w.config.worker_id;
        copy_cstr(ev.worker_name, w.config.name);
        ev.from_state = from;
        ev.to_state = to;
        ev.reason = reason;
        ev.latency_us = latency_us;
        ev.recovery_us = recovery_us;
        ev.epoch = (to == WorkerState::RECOVERING) ? w.expected_epoch : w.epoch;
        ev.inversion_active = w.priority_inversion_detected;
        events_[event_write_idx_ % kEventHistoryCap] = ev;
        ++event_write_idx_;
    }

    std::array<WorkerStatus, kMaxWorkers> workers_{};
    std::size_t worker_count_{0};
    std::array<WatchdogEvent, kEventHistoryCap> events_{};
    uint64_t event_write_idx_{0};
    RespawnCallback respawn_cb_{nullptr};
    EscalateCallback escalate_cb_{nullptr};
    void* cb_ctx_{nullptr};
};

}  // namespace pulseguard
