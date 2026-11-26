#pragma once

#include "pulseguard/clock.hpp"
#include "pulseguard/histogram.hpp"
#include "pulseguard/resmgr.hpp"
#include "pulseguard/ring_buffer.hpp"
#include "pulseguard/telemetry_json.hpp"
#include "pulseguard/watchdog.hpp"
#include "qnx_ipc_bridge.hpp"

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <pthread.h>
#include <unistd.h>

namespace pulseguard {

struct PulseSample {
    uint8_t worker_id{0};
    PulseKind kind{PulseKind::Heartbeat};
    uint16_t epoch{1};
    uint64_t ipc_latency_us{0};
    uint64_t recv_ns{0};
    int32_t priority{20};
    bool inversion_signal{false};
};

static_assert(std::is_trivially_copyable_v<PulseSample>,
              "PulseSample must be trivially copyable for lock-free ring");

enum class FaultInjectionType : uint8_t {
    None = 0,
    CpuStall = 1,
    MissedPulse = 2,
    PriorityInversion = 3,
};

struct EngineConfig {
    uint8_t num_workers{3};
    uint64_t period_us{2000};
    uint64_t warn_deadline_us{3500};
    uint64_t breach_deadline_us{6000};
    uint32_t warn_hysteresis{3};
    uint64_t telemetry_interval_ms{50};
    ResMgrConfig resmgr{};
};

struct EngineSummary {
    uint64_t total_pulses{0};
    uint64_t dropped_pulses{0};
    uint64_t p50_us{0};
    uint64_t p95_us{0};
    uint64_t p99_us{0};
    uint64_t p999_us{0};
    uint64_t max_us{0};
    double mean_us{0.0};
    uint64_t total_restarts{0};
    uint64_t completed_recoveries{0};
    uint64_t total_inversions{0};
    uint64_t last_recovery_us{0};
    uint64_t max_recovery_us{0};
    uint64_t watchdog_events{0};
    bool sched_fifo_applied{false};
    bool pi_mutex_ready{false};
    bool pi_protocol_applied{false};
};

// End-to-end real-time IPC + lock-free ring + HDR histogram + deadline watchdog engine.
// Threads:
//   - N worker threads sending timed pulses over PulseTransport (SCHED_FIFO when permitted)
//   - 1 high-priority IPC receiver thread pushing PulseSamples into wait-free SPSC ring
//   - 1 watchdog/telemetry thread draining SPSC ring, evaluating deadlines, and publishing
//     to ResourceManagerHost (/dev/pulseguard + JSONL stream)
class PulseGuardEngine {
public:
    static constexpr std::size_t kRingCapacity = 4096;
    static constexpr std::size_t kMaxWorkers = 8;

    PulseGuardEngine() noexcept = default;
    ~PulseGuardEngine() noexcept { stop(); }

    PulseGuardEngine(const PulseGuardEngine&) = delete;
    PulseGuardEngine& operator=(const PulseGuardEngine&) = delete;

    bool start(const EngineConfig& cfg) noexcept {
        stop();
        cfg_ = cfg;
        if (cfg_.num_workers == 0) {
            cfg_.num_workers = 1;
        }
        if (cfg_.num_workers > kMaxWorkers) {
            cfg_.num_workers = static_cast<uint8_t>(kMaxWorkers);
        }

        ring_.reset();
        global_hist_.reset();
        watchdog_ = DeadlineWatchdog{};
        watchdog_.set_callbacks(&PulseGuardEngine::on_respawn_cb,
                                &PulseGuardEngine::on_escalate_cb, this);
        total_pulses_.store(0, std::memory_order_relaxed);
        telemetry_seq_.store(0, std::memory_order_relaxed);
        last_emitted_event_idx_ = 0;

        const PiMutexStatus pi_st = init_pi_mutex(&shared_bus_mtx_, 45, false);
        pi_mutex_initialized_ = pi_st.initialized;
        pi_mutex_ready_ = pi_st.protocol_applied || pi_st.initialized;
        pi_protocol_applied_ = pi_st.protocol_applied;

        if (!transport_.open_server()) {
            return false;
        }
        if (!resmgr_.start(cfg_.resmgr)) {
            transport_.close();
            return false;
        }

        static constexpr const char* kWorkerNames[kMaxWorkers] = {
            "lidar_fusion",
            "brake_actuator",
            "imu_ekf",
            "steer_torque",
            "radar_track",
            "can_gateway",
            "path_planner",
            "safety_mon",
        };

        for (uint8_t id = 0; id < cfg_.num_workers; ++id) {
            WorkerConfig wc{};
            wc.worker_id = id;
            copy_cstr(wc.name, kWorkerNames[id]);
            wc.nominal_priority = 24 - static_cast<int>(id * 2U);
            wc.ceiling_priority = 45;
            wc.period_us = cfg_.period_us;
            wc.warn_deadline_us = cfg_.warn_deadline_us;
            wc.breach_deadline_us = cfg_.breach_deadline_us;
            wc.warn_hysteresis = cfg_.warn_hysteresis;
            wc.breach_consecutive_limit = 1;
            watchdog_.register_worker(wc);

            auto& slot = slots_[id];
            slot.worker_id = id;
            slot.engine = this;
            slot.active_epoch.store(1, std::memory_order_relaxed);
            slot.priority.store(wc.nominal_priority, std::memory_order_relaxed);
            slot.injected_fault.store(FaultInjectionType::None, std::memory_order_relaxed);
            slot.injected_stall_us.store(0, std::memory_order_relaxed);
            slot.inversion_pending.store(false, std::memory_order_relaxed);
            slot.send_handshake_epoch.store(0, std::memory_order_relaxed);
            slot.stop_flag.store(false, std::memory_order_relaxed);
            slot.thread_alive.store(false, std::memory_order_relaxed);
        }

        stop_requested_.store(false, std::memory_order_release);

        // Start IPC pulse receiver thread (high priority producer into SPSC ring).
        pthread_attr_t rx_attr;
        pthread_attr_init(&rx_attr);
        const SchedStatus rx_sched = configure_realtime_thread(&rx_attr, 40);
        sched_fifo_applied_ = rx_sched.applied;
        if (pthread_create(&rx_tid_, &rx_attr, &PulseGuardEngine::rx_thread_entry, this) != 0) {
            pthread_attr_destroy(&rx_attr);
            // Retry with default thread attributes if RT creation fails.
            if (pthread_create(&rx_tid_, nullptr, &PulseGuardEngine::rx_thread_entry, this) != 0) {
                resmgr_.stop();
                transport_.close();
                return false;
            }
        } else {
            pthread_attr_destroy(&rx_attr);
        }
        rx_started_ = true;

        // Start watchdog / telemetry consumer thread.
        if (pthread_create(&wd_tid_, nullptr, &PulseGuardEngine::wd_thread_entry, this) != 0) {
            stop();
            return false;
        }
        wd_started_ = true;

        // Start worker threads.
        for (uint8_t id = 0; id < cfg_.num_workers; ++id) {
            if (!spawn_worker_thread(id, 1, false)) {
                stop();
                return false;
            }
        }

        running_.store(true, std::memory_order_release);
        return true;
    }

    void stop() noexcept {
        if (!running_.load(std::memory_order_acquire) && !rx_started_ && !wd_started_) {
            return;
        }
        stop_requested_.store(true, std::memory_order_release);
        running_.store(false, std::memory_order_release);

        for (uint8_t id = 0; id < cfg_.num_workers; ++id) {
            slots_[id].stop_flag.store(true, std::memory_order_release);
        }
        // Wake receiver thread cleanly with a shutdown pulse.
        PulseFrame bye{};
        bye.worker_id = 0;
        bye.kind = PulseKind::Shutdown;
        bye.epoch = 0;
        bye.send_ts_us24 = pack_ts_us24(now_us());
        (void)transport_.send_pulse(bye);

        for (uint8_t id = 0; id < cfg_.num_workers; ++id) {
            join_worker_thread(id);
        }
        join_retired_threads();

        if (rx_started_) {
            pthread_join(rx_tid_, nullptr);
            rx_started_ = false;
        }
        if (wd_started_) {
            pthread_join(wd_tid_, nullptr);
            wd_started_ = false;
        }

        resmgr_.stop();
        transport_.close();
        if (pi_mutex_initialized_) {
            pthread_mutex_destroy(&shared_bus_mtx_);
            pi_mutex_initialized_ = false;
        }
    }

    void inject_cpu_stall(uint8_t worker_id, uint64_t stall_us) noexcept {
        if (worker_id >= cfg_.num_workers) {
            return;
        }
        slots_[worker_id].injected_stall_us.store(stall_us, std::memory_order_relaxed);
        slots_[worker_id].injected_fault.store(FaultInjectionType::CpuStall,
                                               std::memory_order_release);
    }

    void inject_missed_pulse(uint8_t worker_id, uint64_t silence_us) noexcept {
        if (worker_id >= cfg_.num_workers) {
            return;
        }
        slots_[worker_id].injected_stall_us.store(silence_us, std::memory_order_relaxed);
        slots_[worker_id].injected_fault.store(FaultInjectionType::MissedPulse,
                                               std::memory_order_release);
    }

    void inject_priority_inversion(uint8_t worker_id, uint64_t hold_us) noexcept {
        if (worker_id >= cfg_.num_workers) {
            return;
        }
        // Spawn a short-lived low-priority contender holding the PI mutex, then signal worker.
        slots_[worker_id].injected_stall_us.store(hold_us, std::memory_order_relaxed);
        slots_[worker_id].inversion_pending.store(true, std::memory_order_release);
        slots_[worker_id].injected_fault.store(FaultInjectionType::PriorityInversion,
                                               std::memory_order_release);
    }

    EngineSummary summary() const noexcept {
        std::lock_guard<std::mutex> lock(state_mtx_);
        EngineSummary s{};
        s.total_pulses = total_pulses_.load(std::memory_order_relaxed);
        s.dropped_pulses = ring_.dropped();
        s.p50_us = global_hist_.p50_us();
        s.p95_us = global_hist_.p95_us();
        s.p99_us = global_hist_.p99_us();
        s.p999_us = global_hist_.p999_us();
        s.max_us = global_hist_.max_us();
        s.mean_us = global_hist_.mean_us();
        s.total_restarts = watchdog_.total_restarts();
        s.completed_recoveries = watchdog_.total_completed_recoveries();
        s.total_inversions = watchdog_.total_inversions();
        s.last_recovery_us = watchdog_.last_recovery_us();
        s.watchdog_events = watchdog_.event_total();
        for (uint8_t id = 0; id < cfg_.num_workers; ++id) {
            const WorkerStatus* ws = watchdog_.worker(id);
            if (ws && ws->max_recovery_us > s.max_recovery_us) {
                s.max_recovery_us = ws->max_recovery_us;
            }
        }
        s.sched_fifo_applied = sched_fifo_applied_;
        s.pi_mutex_ready = pi_mutex_ready_;
        s.pi_protocol_applied = pi_protocol_applied_;
        return s;
    }

    const ResourceManagerHost& resmgr() const noexcept { return resmgr_; }

private:
    struct WorkerSlot {
        uint8_t worker_id{0};
        PulseGuardEngine* engine{nullptr};
        pthread_t tid{};
        bool started{false};
        std::atomic<bool> thread_alive{false};
        std::atomic<bool> stop_flag{false};
        std::atomic<uint32_t> active_epoch{1};
        std::atomic<uint32_t> send_handshake_epoch{0};
        std::atomic<int> priority{20};
        std::atomic<FaultInjectionType> injected_fault{FaultInjectionType::None};
        std::atomic<uint64_t> injected_stall_us{0};
        std::atomic<bool> inversion_pending{false};
    };

    struct WorkerLaunchArg {
        WorkerSlot* slot;
        uint32_t epoch;
        bool is_respawn;
    };

    static bool on_respawn_cb(uint8_t worker_id, uint32_t next_epoch, void* ctx) noexcept {
        auto* self = static_cast<PulseGuardEngine*>(ctx);
        return self->respawn_worker(worker_id, next_epoch);
    }

    static bool on_escalate_cb(uint8_t worker_id, int target_prio, void* ctx) noexcept {
        auto* self = static_cast<PulseGuardEngine*>(ctx);
        if (worker_id < self->cfg_.num_workers) {
            self->slots_[worker_id].priority.store(target_prio, std::memory_order_release);
        }
        return true;
    }

    bool spawn_worker_thread(uint8_t worker_id, uint32_t epoch, bool is_respawn) noexcept {
        WorkerSlot& slot = slots_[worker_id];
        slot.active_epoch.store(epoch, std::memory_order_release);
        slot.send_handshake_epoch.store(is_respawn ? epoch : 0, std::memory_order_release);
        slot.stop_flag.store(false, std::memory_order_release);

        auto* arg = &launch_args_[static_cast<std::size_t>(worker_id) * 16U +
                                 (epoch % 16U)];
        arg->slot = &slot;
        arg->epoch = epoch;
        arg->is_respawn = is_respawn;

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        (void)configure_realtime_thread(&attr, slot.priority.load(std::memory_order_relaxed));
        pthread_t tid{};
        int rc = pthread_create(&tid, &attr, &PulseGuardEngine::worker_thread_entry, arg);
        pthread_attr_destroy(&attr);
        if (rc != 0) {
            rc = pthread_create(&tid, nullptr, &PulseGuardEngine::worker_thread_entry, arg);
        }
        if (rc != 0) {
            return false;
        }
        slot.tid = tid;
        slot.started = true;
        slot.thread_alive.store(true, std::memory_order_release);
        return true;
    }

    void join_worker_thread(uint8_t worker_id) noexcept {
        WorkerSlot& slot = slots_[worker_id];
        if (slot.started) {
            slot.stop_flag.store(true, std::memory_order_release);
            pthread_join(slot.tid, nullptr);
            slot.started = false;
            slot.thread_alive.store(false, std::memory_order_release);
        }
    }

    void retire_worker_thread(uint8_t worker_id) noexcept {
        WorkerSlot& slot = slots_[worker_id];
        if (!slot.started) {
            return;
        }
        slot.stop_flag.store(true, std::memory_order_release);
        std::lock_guard<std::mutex> lock(retired_mtx_);
        if (retired_count_ < retired_tids_.size()) {
            retired_tids_[retired_count_++] = slot.tid;
        } else {
            pthread_detach(slot.tid);
        }
        slot.started = false;
    }

    void join_retired_threads() noexcept {
        std::lock_guard<std::mutex> lock(retired_mtx_);
        for (std::size_t i = 0; i < retired_count_; ++i) {
            pthread_join(retired_tids_[i], nullptr);
        }
        retired_count_ = 0;
    }

    bool respawn_worker(uint8_t worker_id, uint32_t next_epoch) noexcept {
        if (stop_requested_.load(std::memory_order_acquire) ||
            worker_id >= cfg_.num_workers) {
            return false;
        }
        retire_worker_thread(worker_id);
        return spawn_worker_thread(worker_id, next_epoch, true);
    }

    static void* worker_thread_entry(void* raw_arg) noexcept {
        auto* arg = static_cast<WorkerLaunchArg*>(raw_arg);
        WorkerSlot* slot = arg->slot;
        const uint32_t my_epoch = arg->epoch;
        const bool send_handshake = arg->is_respawn;
        PulseGuardEngine* eng = slot->engine;
        eng->run_worker_loop(*slot, my_epoch, send_handshake);
        return nullptr;
    }

    void run_worker_loop(WorkerSlot& slot, uint32_t my_epoch,
                         bool send_handshake) noexcept {
        uint32_t seq = 0;
        if (send_handshake) {
            PulseFrame hs{};
            hs.worker_id = slot.worker_id;
            hs.kind = PulseKind::RestartHandshake;
            hs.epoch = static_cast<uint16_t>(my_epoch);
            hs.priority = slot.priority.load(std::memory_order_relaxed);
            hs.seq = ++seq;
            hs.send_ts_us24 = pack_ts_us24(now_us());
            (void)transport_.send_pulse(hs);
        }

        while (!stop_requested_.load(std::memory_order_acquire) &&
               !slot.stop_flag.load(std::memory_order_acquire) &&
               slot.active_epoch.load(std::memory_order_acquire) == my_epoch) {
            const FaultInjectionType fault =
                slot.injected_fault.exchange(FaultInjectionType::None,
                                             std::memory_order_acq_rel);

            if (fault == FaultInjectionType::CpuStall ||
                fault == FaultInjectionType::MissedPulse) {
                const uint64_t stall_us =
                    slot.injected_stall_us.load(std::memory_order_relaxed);
                // Sleep in short slices so stop/respawn remains prompt.
                const uint64_t end_ns = now_ns() + stall_us * 1000ULL;
                while (now_ns() < end_ns &&
                       !stop_requested_.load(std::memory_order_acquire) &&
                       !slot.stop_flag.load(std::memory_order_acquire) &&
                       slot.active_epoch.load(std::memory_order_acquire) == my_epoch) {
                    ::usleep(500);
                }
                if (stop_requested_.load(std::memory_order_acquire) ||
                    slot.stop_flag.load(std::memory_order_acquire) ||
                    slot.active_epoch.load(std::memory_order_acquire) != my_epoch) {
                    break;
                }
            } else if (fault == FaultInjectionType::PriorityInversion) {
                const uint64_t hold_us =
                    slot.injected_stall_us.load(std::memory_order_relaxed);
                // Emit inversion alert pulse, acquire PI mutex briefly, escalate priority.
                PulseFrame inv{};
                inv.worker_id = slot.worker_id;
                inv.kind = PulseKind::InversionAlert;
                inv.epoch = static_cast<uint16_t>(my_epoch);
                inv.priority = slot.priority.load(std::memory_order_relaxed);
                inv.seq = ++seq;
                inv.send_ts_us24 = pack_ts_us24(now_us());
                (void)transport_.send_pulse(inv);

                if (pi_mutex_initialized_) {
                    pthread_mutex_lock(&shared_bus_mtx_);
                    ::usleep(static_cast<useconds_t>(std::min<uint64_t>(hold_us, 10000ULL)));
                    pthread_mutex_unlock(&shared_bus_mtx_);
                } else {
                    ::usleep(static_cast<useconds_t>(std::min<uint64_t>(hold_us, 10000ULL)));
                }
            }

            PulseFrame frame{};
            frame.worker_id = slot.worker_id;
            frame.kind = PulseKind::Heartbeat;
            frame.epoch = static_cast<uint16_t>(my_epoch);
            frame.priority = slot.priority.load(std::memory_order_relaxed);
            frame.seq = ++seq;
            frame.send_ts_us24 = pack_ts_us24(now_us());
            (void)transport_.send_pulse(frame);

            ::usleep(static_cast<useconds_t>(cfg_.period_us));
        }
    }

    static void* rx_thread_entry(void* arg) noexcept {
        static_cast<PulseGuardEngine*>(arg)->run_rx_loop();
        return nullptr;
    }

    void run_rx_loop() noexcept {
        while (!stop_requested_.load(std::memory_order_acquire)) {
            PulseFrame frame{};
            const auto st = transport_.receive_pulse(frame, 5);
            if (st == PulseTransport::RecvStatus::Timeout ||
                st == PulseTransport::RecvStatus::SystemPulse) {
                continue;
            }
            if (st == PulseTransport::RecvStatus::Error) {
                if (stop_requested_.load(std::memory_order_acquire)) {
                    break;
                }
                ::usleep(100);
                continue;
            }
            if (frame.kind == PulseKind::Shutdown) {
                break;
            }

            const uint64_t recv_ns = now_ns();
            const uint32_t recv_us24 = pack_ts_us24(recv_ns / 1000ULL);
            const uint64_t ipc_us = delta_ts_us24(recv_us24, frame.send_ts_us24);

            PulseSample sample{};
            sample.worker_id = frame.worker_id;
            sample.kind = frame.kind;
            sample.epoch = frame.epoch;
            sample.ipc_latency_us = ipc_us;
            sample.recv_ns = recv_ns;
            sample.priority = frame.priority;
            sample.inversion_signal = (frame.kind == PulseKind::InversionAlert);
            (void)ring_.try_push(sample);
        }
    }

    static void* wd_thread_entry(void* arg) noexcept {
        static_cast<PulseGuardEngine*>(arg)->run_wd_loop();
        return nullptr;
    }

    void run_wd_loop() noexcept {
        uint64_t last_publish_ns = now_ns();
        const uint64_t publish_period_ns = cfg_.telemetry_interval_ms * 1'000'000ULL;

        while (!stop_requested_.load(std::memory_order_acquire)) {
            bool processed_any = false;
            {
                std::lock_guard<std::mutex> lock(state_mtx_);
                ring_.drain([&](const PulseSample& s) {
                    processed_any = true;
                    total_pulses_.fetch_add(1, std::memory_order_relaxed);
                    global_hist_.record(s.ipc_latency_us);
                    watchdog_.on_pulse(s.worker_id, s.ipc_latency_us, s.recv_ns, s.epoch,
                                       s.kind == PulseKind::RestartHandshake,
                                       s.inversion_signal);
                });

                const uint64_t tick_ns = now_ns();
                if (!stop_requested_.load(std::memory_order_acquire)) {
                    (void)watchdog_.tick(tick_ns);
                }

                emit_new_events_locked();

                if ((tick_ns - last_publish_ns) >= publish_period_ns) {
                    publish_snapshot_locked(tick_ns);
                    last_publish_ns = tick_ns;
                }
            }
            ::usleep(processed_any ? 200 : 400);
        }

        // Final drain & publish before exit.
        std::lock_guard<std::mutex> lock(state_mtx_);
        ring_.drain([&](const PulseSample& s) {
            total_pulses_.fetch_add(1, std::memory_order_relaxed);
            global_hist_.record(s.ipc_latency_us);
            watchdog_.on_pulse(s.worker_id, s.ipc_latency_us, s.recv_ns, s.epoch,
                               s.kind == PulseKind::RestartHandshake, s.inversion_signal);
        });
        emit_new_events_locked();
        publish_snapshot_locked(now_ns());
    }

    void emit_new_events_locked() noexcept {
        const uint64_t total_ev = watchdog_.event_total();
        if (total_ev <= last_emitted_event_idx_) {
            return;
        }
        std::array<WatchdogEvent, DeadlineWatchdog::kEventHistoryCap> buf{};
        const std::size_t count = watchdog_.copy_recent_events(buf.data(), buf.size());
        const uint64_t first_idx = (total_ev > count) ? (total_ev - count) : 0;
        char line[512]{};
        for (std::size_t i = 0; i < count; ++i) {
            const uint64_t ev_idx = first_idx + i + 1;
            if (ev_idx > last_emitted_event_idx_) {
                const uint64_t seq = telemetry_seq_.fetch_add(1, std::memory_order_relaxed) + 1;
                const std::size_t n = format_fault_event_jsonl(line, sizeof(line), seq, buf[i]);
                if (n > 0) {
                    resmgr_.publish_event(line, n);
                }
                last_emitted_event_idx_ = ev_idx;
            }
        }
    }

    void publish_snapshot_locked(uint64_t ts_ns) noexcept {
        const uint64_t seq = telemetry_seq_.fetch_add(1, std::memory_order_relaxed) + 1;
        const uint64_t pulses = total_pulses_.load(std::memory_order_relaxed);
        const uint64_t dropped = ring_.dropped();
        char jsonl[ResourceManagerHost::kSnapshotCap]{};
        const std::size_t n = format_telemetry_jsonl(
            jsonl, sizeof(jsonl), seq, ts_ns / 1000ULL, global_hist_, pulses, dropped, watchdog_);
        if (n > 0) {
            const TelemetryBinaryFrame bin = build_binary_frame(
                seq, ts_ns / 1000ULL, global_hist_, pulses, dropped, watchdog_);
            resmgr_.publish_telemetry(jsonl, n, bin, true);
        }
    }

    EngineConfig cfg_{};
    PulseTransport transport_{};
    SpscRingBuffer<PulseSample, kRingCapacity> ring_{};
    ResourceManagerHost resmgr_{};

    mutable std::mutex state_mtx_{};
    JitterHistogram global_hist_{};
    DeadlineWatchdog watchdog_{};
    uint64_t last_emitted_event_idx_{0};

    std::array<WorkerSlot, kMaxWorkers> slots_{};
    std::array<WorkerLaunchArg, kMaxWorkers * 16> launch_args_{};

    std::mutex retired_mtx_{};
    std::array<pthread_t, 64> retired_tids_{};
    std::size_t retired_count_{0};

    pthread_t rx_tid_{};
    pthread_t wd_tid_{};
    bool rx_started_{false};
    bool wd_started_{false};

    pthread_mutex_t shared_bus_mtx_{};
    bool pi_mutex_initialized_{false};
    bool pi_mutex_ready_{false};
    bool pi_protocol_applied_{false};
    bool sched_fifo_applied_{false};

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<uint64_t> total_pulses_{0};
    std::atomic<uint64_t> telemetry_seq_{0};
};

}  // namespace pulseguard
