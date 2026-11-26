#pragma once

#include "pulseguard/telemetry_json.hpp"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <pthread.h>

#ifdef __QNXNTO__
#include <devctl.h>
#define DCMD_PULSEGUARD_GET_BINARY __DIOF(_DCMD_MISC, 0x41, pulseguard::TelemetryBinaryFrame)
#else
inline constexpr unsigned int DCMD_PULSEGUARD_GET_BINARY = 0x40484D41U;
#endif

namespace pulseguard {

struct ResMgrConfig {
    const char* device_path{"/dev/pulseguard"};
    const char* host_snapshot_path{"pulseguard_dev.json"};
    const char* host_jsonl_path{"pulseguard_telemetry.jsonl"};
    const char* host_binary_path{"pulseguard_dev.bin"};
    bool write_host_files{true};
};

// Dual-target virtual device / resource manager host.
// On QNX 8.0 (__QNXNTO__), registers `/dev/pulseguard` via resmgr_attach(),
// serving live JSONL telemetry on read() and TelemetryBinaryFrame on devctl().
// On POSIX hosts, maintains atomic snapshot/stream endpoints with identical APIs.
class ResourceManagerHost {
public:
    static constexpr std::size_t kSnapshotCap = 8192;

    ResourceManagerHost() noexcept = default;
    ~ResourceManagerHost() noexcept { stop(); }

    ResourceManagerHost(const ResourceManagerHost&) = delete;
    ResourceManagerHost& operator=(const ResourceManagerHost&) = delete;

    bool start(const ResMgrConfig& cfg) noexcept;
    void stop() noexcept;

    // Publishes latest telemetry snapshot and optional JSONL line.
    // Non-blocking with respect to the real-time IPC producer (called on consumer/reporter path).
    void publish_telemetry(const char* jsonl_line, std::size_t jsonl_len,
                           const TelemetryBinaryFrame& binary_frame,
                           bool append_stream = true) noexcept;

    // Appends an asynchronous fault/recovery event JSONL record to the device ring & host stream.
    void publish_event(const char* event_jsonl, std::size_t len) noexcept;

    std::size_t read_latest_jsonl(char* out, std::size_t cap) const noexcept;
    TelemetryBinaryFrame read_latest_binary() const noexcept;

    uint64_t publish_count() const noexcept {
        return publish_count_.load(std::memory_order_relaxed);
    }

    bool is_running() const noexcept {
        return running_.load(std::memory_order_acquire);
    }

    const ResMgrConfig& config() const noexcept { return cfg_; }

    // Internal dispatch-thread helper exposed for QNX io_read callback.
    std::size_t latch_dispatch_snapshot() noexcept;
    const char* dispatch_scratch() const noexcept { return dispatch_scratch_; }
    std::size_t dispatch_scratch_len() const noexcept { return dispatch_scratch_len_; }

private:
    static void* dispatch_thread_entry(void* arg) noexcept;
    void run_qnx_dispatch_loop() noexcept;
    void write_atomic_file(const char* path, const void* data, std::size_t len) noexcept;
    void append_file(const char* path, const void* data, std::size_t len) noexcept;

    ResMgrConfig cfg_{};
    mutable std::mutex mtx_{};
    char snapshot_buf_[kSnapshotCap]{};
    std::size_t snapshot_len_{0};
    TelemetryBinaryFrame binary_snapshot_{};

    char dispatch_scratch_[kSnapshotCap]{};
    std::size_t dispatch_scratch_len_{0};

    std::atomic<bool> running_{false};
    std::atomic<bool> stop_requested_{false};
    std::atomic<uint64_t> publish_count_{0};

#ifdef __QNXNTO__
    pthread_t dispatch_tid_{};
    bool dispatch_thread_started_{false};
    void* dispatch_ptr_{nullptr};
    int resmgr_id_{-1};
#endif
};

}  // namespace pulseguard
