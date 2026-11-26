#include "pulseguard/engine.hpp"

#include <csignal>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <unistd.h>

namespace {

volatile std::sig_atomic_t g_stop = 0;

void handle_signal(int) {
    g_stop = 1;
}

void print_usage(const char* prog) {
    std::cout
        << "Usage: " << prog << " [options]\n"
        << "  --workers N          Number of real-time worker threads (1..8, default 3)\n"
        << "  --duration-ms N      Run duration in milliseconds (default 1500; 0 = until SIGINT)\n"
        << "  --period-us N        Worker pulse period in microseconds (default 1000)\n"
        << "  --warn-us N          Watchdog warn threshold in microseconds (default 3500)\n"
        << "  --breach-us N        Watchdog breach threshold in microseconds (default 6500)\n"
        << "  --interval-ms N      Telemetry emit interval in ms (default 50)\n"
        << "  --device-path PATH   QNX resmgr device mount (default /dev/pulseguard)\n"
        << "  --jsonl-out PATH     Host JSONL stream output path (default pulseguard_telemetry.jsonl)\n"
        << "  --snapshot-out PATH  Host latest snapshot path (default pulseguard_dev.json)\n"
        << "  --inject-at-ms N     Optionally inject a synthetic CPU stall at N ms (default 0=off)\n"
        << "  --quiet              Suppress per-interval stdout lines\n";
}

}  // namespace

int main(int argc, char** argv) {
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 3;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3500;
    cfg.breach_deadline_us = 6500;
    cfg.warn_hysteresis = 3;
    cfg.telemetry_interval_ms = 50;

    int duration_ms = 1500;
    int inject_at_ms = 0;
    bool quiet = false;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            cfg.num_workers = static_cast<uint8_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--duration-ms") == 0 && i + 1 < argc) {
            duration_ms = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--period-us") == 0 && i + 1 < argc) {
            cfg.period_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--warn-us") == 0 && i + 1 < argc) {
            cfg.warn_deadline_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--breach-us") == 0 && i + 1 < argc) {
            cfg.breach_deadline_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--interval-ms") == 0 && i + 1 < argc) {
            cfg.telemetry_interval_ms = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--device-path") == 0 && i + 1 < argc) {
            cfg.resmgr.device_path = argv[++i];
        } else if (std::strcmp(argv[i], "--jsonl-out") == 0 && i + 1 < argc) {
            cfg.resmgr.host_jsonl_path = argv[++i];
        } else if (std::strcmp(argv[i], "--snapshot-out") == 0 && i + 1 < argc) {
            cfg.resmgr.host_snapshot_path = argv[++i];
        } else if (std::strcmp(argv[i], "--inject-at-ms") == 0 && i + 1 < argc) {
            inject_at_ms = std::atoi(argv[++i]);
        } else if (std::strcmp(argv[i], "--quiet") == 0) {
            quiet = true;
        } else {
            std::cerr << "Unknown argument: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    pulseguard::PulseGuardEngine engine;
    if (!engine.start(cfg)) {
        std::cerr << "Failed to start PulseGuard daemon\n";
        return 1;
    }

    if (!quiet) {
        std::cout << "[pulseguard_daemon] platform=" << pulseguard::target_platform_tag()
                  << " workers=" << static_cast<int>(cfg.num_workers)
                  << " period_us=" << cfg.period_us
                  << " warn_us=" << cfg.warn_deadline_us
                  << " breach_us=" << cfg.breach_deadline_us
                  << " device=" << cfg.resmgr.device_path << "\n";
    }

    const uint64_t start_ns = pulseguard::now_ns();
    bool injected = false;
    uint64_t last_print_ms = 0;

    while (!g_stop) {
        const uint64_t elapsed_ms = (pulseguard::now_ns() - start_ns) / 1'000'000ULL;
        if (duration_ms > 0 && elapsed_ms >= static_cast<uint64_t>(duration_ms)) {
            break;
        }
        if (inject_at_ms > 0 && !injected &&
            elapsed_ms >= static_cast<uint64_t>(inject_at_ms)) {
            injected = true;
            engine.inject_cpu_stall(0, cfg.breach_deadline_us + 8000ULL);
            if (!quiet) {
                std::cout << "[pulseguard_daemon] injected CPU stall on worker 0 at "
                          << elapsed_ms << " ms\n";
            }
        }
        if (!quiet && elapsed_ms >= last_print_ms + 250ULL) {
            last_print_ms = elapsed_ms;
            const auto s = engine.summary();
            std::cout << "[live] pulses=" << s.total_pulses
                      << " dropped=" << s.dropped_pulses
                      << " p50=" << s.p50_us << "us"
                      << " p99=" << s.p99_us << "us"
                      << " p999=" << s.p999_us << "us"
                      << " max=" << s.max_us << "us"
                      << " restarts=" << s.total_restarts << "\n";
        }
        ::usleep(10'000);
    }

    engine.stop();
    const auto s = engine.summary();
    std::cout << "[summary] platform=" << pulseguard::target_platform_tag()
              << " total_pulses=" << s.total_pulses
              << " dropped=" << s.dropped_pulses
              << " p50_us=" << s.p50_us
              << " p95_us=" << s.p95_us
              << " p99_us=" << s.p99_us
              << " p999_us=" << s.p999_us
              << " max_us=" << s.max_us
              << " mean_us=" << s.mean_us
              << " restarts=" << s.total_restarts
              << " inversions=" << s.total_inversions
              << " last_recovery_us=" << s.last_recovery_us
              << " max_recovery_us=" << s.max_recovery_us
              << " sched_fifo=" << (s.sched_fifo_applied ? "true" : "false")
              << " pi_mutex=" << (s.pi_mutex_ready ? "true" : "false") << "\n";
    return 0;
}
