#include "pulseguard/engine.hpp"

#include <cstdlib>
#include <cstring>
#include <iostream>
#include <unistd.h>

namespace {

void print_usage(const char* prog) {
    std::cout
        << "Usage: " << prog << " [options]\n"
        << "  --mode MODE          cpu_stall | missed_pulse | priority_inversion | all (default all)\n"
        << "  --workers N          Number of workers (default 3)\n"
        << "  --period-us N        Pulse cadence in us (default 1000)\n"
        << "  --warn-us N          Warn threshold in us (default 3500)\n"
        << "  --breach-us N        Breach threshold in us (default 6500)\n"
        << "  --stall-ms N         Injected stall duration in ms (default 12)\n"
        << "  --jsonl-out PATH     JSONL telemetry output path (default pulseguard_telemetry.jsonl)\n";
}

bool wait_for_recovery_complete(pulseguard::PulseGuardEngine& engine, uint64_t target_recoveries,
                                uint64_t timeout_ms) {
    const uint64_t deadline_ns = pulseguard::now_ns() + timeout_ms * 1'000'000ULL;
    while (pulseguard::now_ns() < deadline_ns) {
        if (engine.summary().completed_recoveries >= target_recoveries) {
            return true;
        }
        ::usleep(500);
    }
    return engine.summary().completed_recoveries >= target_recoveries;
}

bool wait_for_inversion_count(pulseguard::PulseGuardEngine& engine, uint64_t target_inversions,
                              uint64_t timeout_ms) {
    const uint64_t deadline_ns = pulseguard::now_ns() + timeout_ms * 1'000'000ULL;
    while (pulseguard::now_ns() < deadline_ns) {
        if (engine.summary().total_inversions >= target_inversions) {
            return true;
        }
        ::usleep(500);
    }
    return engine.summary().total_inversions >= target_inversions;
}

}  // namespace

int main(int argc, char** argv) {
    const char* mode = "all";
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 3;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3500;
    cfg.breach_deadline_us = 6500;
    cfg.warn_hysteresis = 3;
    cfg.telemetry_interval_ms = 25;
    uint64_t stall_ms = 14;

    for (int i = 1; i < argc; ++i) {
        if (std::strcmp(argv[i], "--help") == 0 || std::strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else if (std::strcmp(argv[i], "--mode") == 0 && i + 1 < argc) {
            mode = argv[++i];
        } else if (std::strcmp(argv[i], "--workers") == 0 && i + 1 < argc) {
            cfg.num_workers = static_cast<uint8_t>(std::atoi(argv[++i]));
        } else if (std::strcmp(argv[i], "--period-us") == 0 && i + 1 < argc) {
            cfg.period_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--warn-us") == 0 && i + 1 < argc) {
            cfg.warn_deadline_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--breach-us") == 0 && i + 1 < argc) {
            cfg.breach_deadline_us = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--stall-ms") == 0 && i + 1 < argc) {
            stall_ms = static_cast<uint64_t>(std::atoll(argv[++i]));
        } else if (std::strcmp(argv[i], "--jsonl-out") == 0 && i + 1 < argc) {
            cfg.resmgr.host_jsonl_path = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << argv[i] << "\n";
            print_usage(argv[0]);
            return 1;
        }
    }

    pulseguard::PulseGuardEngine engine;
    if (!engine.start(cfg)) {
        std::cerr << "Failed to start PulseGuard engine\n";
        return 1;
    }

    std::cout << "[fault_injector] baseline warmup 250ms...\n";
    ::usleep(250'000);

    const bool run_all = (std::strcmp(mode, "all") == 0);
    bool ok = true;

    if (run_all || std::strcmp(mode, "cpu_stall") == 0) {
        const uint64_t prev_rec = engine.summary().completed_recoveries;
        std::cout << "[fault_injector] injecting cpu_stall (" << stall_ms
                  << "ms) on worker 0...\n";
        engine.inject_cpu_stall(0, stall_ms * 1000ULL);
        const bool recovered = wait_for_recovery_complete(engine, prev_rec + 1, 500);
        const auto s = engine.summary();
        std::cout << "[fault_injector] cpu_stall recovered=" << (recovered ? "PASS" : "FAIL")
                  << " restarts=" << s.total_restarts
                  << " completed_recoveries=" << s.completed_recoveries
                  << " last_recovery_us=" << s.last_recovery_us << "\n";
        ok = ok && recovered && (s.last_recovery_us < 10'000ULL);
        ::usleep(80'000);
    }

    if (run_all || std::strcmp(mode, "priority_inversion") == 0) {
        const uint64_t prev_inv = engine.summary().total_inversions;
        std::cout << "[fault_injector] injecting priority_inversion on worker 1...\n";
        engine.inject_priority_inversion(1 % cfg.num_workers, 2000ULL);
        const bool escalated = wait_for_inversion_count(engine, prev_inv + 1, 500);
        const auto s = engine.summary();
        std::cout << "[fault_injector] priority_inversion escalated="
                  << (escalated ? "PASS" : "FAIL")
                  << " inversions=" << s.total_inversions << "\n";
        ok = ok && escalated;
        ::usleep(80'000);
    }

    if (run_all || std::strcmp(mode, "missed_pulse") == 0) {
        const uint64_t prev_rec = engine.summary().completed_recoveries;
        const uint8_t target_w = static_cast<uint8_t>(2 % cfg.num_workers);
        std::cout << "[fault_injector] injecting missed_pulse (" << stall_ms
                  << "ms) on worker " << static_cast<int>(target_w) << "...\n";
        engine.inject_missed_pulse(target_w, stall_ms * 1000ULL);
        const bool recovered = wait_for_recovery_complete(engine, prev_rec + 1, 500);
        const auto s = engine.summary();
        std::cout << "[fault_injector] missed_pulse recovered=" << (recovered ? "PASS" : "FAIL")
                  << " restarts=" << s.total_restarts
                  << " completed_recoveries=" << s.completed_recoveries
                  << " last_recovery_us=" << s.last_recovery_us << "\n";
        ok = ok && recovered && (s.last_recovery_us < 10'000ULL);
        ::usleep(100'000);
    }

    engine.stop();
    const auto final_s = engine.summary();
    std::cout << "[fault_injector] summary"
              << " pulses=" << final_s.total_pulses
              << " dropped=" << final_s.dropped_pulses
              << " p50_us=" << final_s.p50_us
              << " p95_us=" << final_s.p95_us
              << " p99_us=" << final_s.p99_us
              << " p999_us=" << final_s.p999_us
              << " max_us=" << final_s.max_us
              << " restarts=" << final_s.total_restarts
              << " inversions=" << final_s.total_inversions
              << " last_recovery_us=" << final_s.last_recovery_us
              << " max_recovery_us=" << final_s.max_recovery_us
              << " verdict=" << (ok ? "PASS" : "FAIL") << "\n";

    return ok ? 0 : 1;
}
