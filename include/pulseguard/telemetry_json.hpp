#pragma once

#include "pulseguard/clock.hpp"
#include "pulseguard/histogram.hpp"
#include "pulseguard/watchdog.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <type_traits>

namespace pulseguard {

struct TelemetryBinaryFrame {
    uint32_t magic{0x504C5347U};  // "PLSG"
    uint16_t version{1};
    uint16_t active_workers{0};
    uint64_t seq{0};
    uint64_t ts_us{0};
    uint64_t total_pulses{0};
    uint64_t dropped_pulses{0};
    uint64_t p50_us{0};
    uint64_t p95_us{0};
    uint64_t p99_us{0};
    uint64_t p999_us{0};
    uint64_t max_us{0};
    uint64_t mean_us_x100{0};
    uint64_t total_restarts{0};
    uint64_t total_inversions{0};
    uint64_t last_recovery_us{0};
    uint32_t worker_state_mask{0};
    uint32_t crc32{0};
};

static_assert(std::is_trivially_copyable_v<TelemetryBinaryFrame>,
              "TelemetryBinaryFrame must be trivially copyable");

inline uint32_t compute_crc32(const uint8_t* data, std::size_t len) noexcept {
    uint32_t crc = 0xFFFFFFFFU;
    for (std::size_t i = 0; i < len; ++i) {
        crc ^= static_cast<uint32_t>(data[i]);
        for (int bit = 0; bit < 8; ++bit) {
            const uint32_t mask = static_cast<uint32_t>(-static_cast<int32_t>(crc & 1U));
            crc = (crc >> 1U) ^ (0xEDB88320U & mask);
        }
    }
    return ~crc;
}

inline TelemetryBinaryFrame build_binary_frame(uint64_t seq, uint64_t ts_us,
                                               const JitterHistogram& hist,
                                               uint64_t total_pulses,
                                               uint64_t dropped_pulses,
                                               const DeadlineWatchdog& wd) noexcept {
    TelemetryBinaryFrame frame{};
    frame.magic = 0x504C5347U;
    frame.version = 1;
    frame.active_workers = static_cast<uint16_t>(wd.worker_count());
    frame.seq = seq;
    frame.ts_us = ts_us;
    frame.total_pulses = total_pulses;
    frame.dropped_pulses = dropped_pulses;
    frame.p50_us = hist.p50_us();
    frame.p95_us = hist.p95_us();
    frame.p99_us = hist.p99_us();
    frame.p999_us = hist.p999_us();
    frame.max_us = hist.max_us();
    frame.mean_us_x100 = static_cast<uint64_t>(hist.mean_us() * 100.0 + 0.5);
    frame.total_restarts = wd.total_restarts();
    frame.total_inversions = wd.total_inversions();
    frame.last_recovery_us = wd.last_recovery_us();

    uint32_t mask = 0;
    for (uint8_t id = 0; id < DeadlineWatchdog::kMaxWorkers && id < 8U; ++id) {
        const WorkerStatus* ws = wd.worker(id);
        if (ws) {
            mask |= (static_cast<uint32_t>(ws->state) & 0x0FU) << (id * 4U);
        }
    }
    frame.worker_state_mask = mask;
    frame.crc32 = 0;
    frame.crc32 = compute_crc32(reinterpret_cast<const uint8_t*>(&frame),
                                sizeof(TelemetryBinaryFrame) - sizeof(uint32_t));
    return frame;
}

inline std::size_t escape_json_cstr(char* out, std::size_t cap, const char* src) noexcept {
    if (!out || cap == 0) {
        return 0;
    }
    std::size_t pos = 0;
    for (std::size_t i = 0; src && src[i] != '\0'; ++i) {
        const unsigned char ch = static_cast<unsigned char>(src[i]);
        if (ch == '"' || ch == '\\') {
            if (pos + 2 >= cap) {
                break;
            }
            out[pos++] = '\\';
            out[pos++] = static_cast<char>(ch);
        } else if (ch >= 0x20U && ch <= 0x7EU) {
            if (pos + 1 >= cap) {
                break;
            }
            out[pos++] = static_cast<char>(ch);
        }
    }
    out[pos] = '\0';
    return pos;
}

// Formats one telemetry JSONL snapshot into `out` with zero heap allocations.
// Returns bytes written (excluding trailing NUL), or 0 if `cap` is insufficient.
inline std::size_t format_telemetry_jsonl(char* out, std::size_t cap, uint64_t seq,
                                          uint64_t ts_us, const JitterHistogram& hist,
                                          uint64_t total_pulses, uint64_t dropped_pulses,
                                          const DeadlineWatchdog& wd) noexcept {
    if (!out || cap < 256) {
        return 0;
    }

    const uint64_t mean_x100 = static_cast<uint64_t>(hist.mean_us() * 100.0 + 0.5);
    std::size_t used = 0;
    const int head_n = std::snprintf(
        out, cap,
        "{\"type\":\"telemetry\",\"seq\":%llu,\"ts_us\":%llu,\"platform\":\"%s\","
        "\"total_pulses\":%llu,\"dropped_pulses\":%llu,"
        "\"p50_us\":%llu,\"p95_us\":%llu,\"p99_us\":%llu,\"p999_us\":%llu,"
        "\"max_us\":%llu,\"mean_us\":%llu.%02llu,\"restarts\":%llu,\"inversions\":%llu,"
        "\"last_recovery_us\":%llu,\"workers\":[",
        static_cast<unsigned long long>(seq),
        static_cast<unsigned long long>(ts_us),
        target_platform_tag(),
        static_cast<unsigned long long>(total_pulses),
        static_cast<unsigned long long>(dropped_pulses),
        static_cast<unsigned long long>(hist.p50_us()),
        static_cast<unsigned long long>(hist.p95_us()),
        static_cast<unsigned long long>(hist.p99_us()),
        static_cast<unsigned long long>(hist.p999_us()),
        static_cast<unsigned long long>(hist.max_us()),
        static_cast<unsigned long long>(mean_x100 / 100ULL),
        static_cast<unsigned long long>(mean_x100 % 100ULL),
        static_cast<unsigned long long>(wd.total_restarts()),
        static_cast<unsigned long long>(wd.total_inversions()),
        static_cast<unsigned long long>(wd.last_recovery_us()));
    if (head_n < 0 || static_cast<std::size_t>(head_n) >= cap) {
        return 0;
    }
    used = static_cast<std::size_t>(head_n);

    bool first = true;
    char safe_name[64]{};
    for (uint8_t id = 0; id < DeadlineWatchdog::kMaxWorkers; ++id) {
        const WorkerStatus* ws = wd.worker(id);
        if (!ws) {
            continue;
        }
        escape_json_cstr(safe_name, sizeof(safe_name), ws->config.name);
        const int item_n = std::snprintf(
            out + used, cap - used,
            "%s{\"id\":%u,\"name\":\"%s\",\"state\":\"%s\",\"prio\":%d,"
            "\"epoch\":%u,\"pulses\":%llu,\"missed\":%llu,\"restarts\":%llu,"
            "\"last_us\":%llu,\"p99_us\":%llu,\"recovery_us\":%llu,\"inversion\":%s}",
            first ? "" : ",",
            static_cast<unsigned>(ws->config.worker_id),
            safe_name,
            worker_state_name(ws->state),
            ws->current_priority,
            static_cast<unsigned>(ws->epoch),
            static_cast<unsigned long long>(ws->pulses_received),
            static_cast<unsigned long long>(ws->missed_deadlines),
            static_cast<unsigned long long>(ws->restarts),
            static_cast<unsigned long long>(ws->last_latency_us),
            static_cast<unsigned long long>(ws->histogram.p99_us()),
            static_cast<unsigned long long>(ws->last_recovery_us),
            ws->priority_inversion_detected ? "true" : "false");
        if (item_n < 0 || used + static_cast<std::size_t>(item_n) + 4 >= cap) {
            return 0;
        }
        used += static_cast<std::size_t>(item_n);
        first = false;
    }

    const int tail_n = std::snprintf(out + used, cap - used, "]}\n");
    if (tail_n < 0 || used + static_cast<std::size_t>(tail_n) >= cap) {
        return 0;
    }
    used += static_cast<std::size_t>(tail_n);
    return used;
}

inline std::size_t format_fault_event_jsonl(char* out, std::size_t cap, uint64_t seq,
                                            const WatchdogEvent& ev) noexcept {
    if (!out || cap < 160) {
        return 0;
    }
    char safe_name[64]{};
    escape_json_cstr(safe_name, sizeof(safe_name), ev.worker_name);
    const int n = std::snprintf(
        out, cap,
        "{\"type\":\"fault_event\",\"seq\":%llu,\"ts_us\":%llu,\"platform\":\"%s\","
        "\"worker_id\":%u,\"worker\":\"%s\",\"from\":\"%s\",\"to\":\"%s\","
        "\"reason\":\"%s\",\"latency_us\":%llu,\"recovery_us\":%llu,"
        "\"epoch\":%u,\"inversion\":%s}\n",
        static_cast<unsigned long long>(seq),
        static_cast<unsigned long long>(ev.ts_ns / 1000ULL),
        target_platform_tag(),
        static_cast<unsigned>(ev.worker_id),
        safe_name,
        worker_state_name(ev.from_state),
        worker_state_name(ev.to_state),
        fault_reason_name(ev.reason),
        static_cast<unsigned long long>(ev.latency_us),
        static_cast<unsigned long long>(ev.recovery_us),
        static_cast<unsigned>(ev.epoch),
        ev.inversion_active ? "true" : "false");
    if (n < 0 || static_cast<std::size_t>(n) >= cap) {
        return 0;
    }
    return static_cast<std::size_t>(n);
}

}  // namespace pulseguard
