#include "pulseguard/clock.hpp"
#include "pulseguard/engine.hpp"
#include "pulseguard/histogram.hpp"
#include "pulseguard/resmgr.hpp"
#include "pulseguard/ring_buffer.hpp"
#include "pulseguard/telemetry_json.hpp"
#include "pulseguard/watchdog.hpp"
#include "qnx_ipc_bridge.hpp"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <new>
#include <pthread.h>
#include <unistd.h>

// Thread-local allocation guard to verify zero dynamic memory allocation on hot paths.
namespace {

thread_local bool g_track_hot_allocs = false;
thread_local std::size_t g_hot_alloc_calls = 0;

}  // namespace

void* operator new(std::size_t sz) {
    if (g_track_hot_allocs) {
        ++g_hot_alloc_calls;
    }
    void* p = std::malloc(sz == 0 ? 1 : sz);
    if (!p) {
        throw std::bad_alloc();
    }
    return p;
}

void operator delete(void* p) noexcept {
    std::free(p);
}

void operator delete(void* p, std::size_t) noexcept {
    std::free(p);
}

namespace {

int g_tests_run = 0;
int g_tests_passed = 0;
int g_assertions = 0;

#define PG_ASSERT(cond)                                                              \
    do {                                                                             \
        ++g_assertions;                                                              \
        if (!(cond)) {                                                               \
            std::fprintf(stderr, "  [FAIL] %s:%d: assertion failed: %s\n", __FILE__, \
                         __LINE__, #cond);                                           \
            return false;                                                            \
        }                                                                            \
    } while (0)

#define RUN_TEST(fn)                                                    \
    do {                                                                \
        ++g_tests_run;                                                  \
        if (fn()) {                                                     \
            ++g_tests_passed;                                           \
            std::printf("[PASS] %s\n", #fn);                            \
        } else {                                                        \
            std::printf("[FAIL] %s\n", #fn);                            \
        }                                                               \
    } while (0)

bool test_clock_monotonic_and_us24_wrap() {
    PG_ASSERT(pulseguard::cycles_per_sec() > 0);
    const uint64_t t1 = pulseguard::now_ns();
    const uint64_t c1 = pulseguard::cycles_now();
    ::usleep(1000);
    const uint64_t t2 = pulseguard::now_ns();
    const uint64_t c2 = pulseguard::cycles_now();
    PG_ASSERT(t2 > t1);
    PG_ASSERT(c2 >= c1);
    PG_ASSERT(pulseguard::cycles_to_ns(c2 - c1) >= 500'000ULL);
    PG_ASSERT(pulseguard::delta_ts_us24(0x00000AU, 0xFFFFFFU) == 11ULL);
    PG_ASSERT(pulseguard::delta_ts_us24(100U, 100U) == 0ULL);
    return true;
}

bool test_pulse_bitpack_roundtrip() {
    uint8_t wid = 0;
    uint16_t epoch = 0;
    uint32_t ts24 = 0;

    const int32_t p1 = pulseguard::pack_pulse_value(0, 1, 0x123456U);
    pulseguard::unpack_pulse_value(p1, wid, epoch, ts24);
    PG_ASSERT(wid == 0 && epoch == 1 && ts24 == 0x123456U);

    const int32_t p2 = pulseguard::pack_pulse_value(15, 15, 0xFFFFFFU);
    pulseguard::unpack_pulse_value(p2, wid, epoch, ts24);
    PG_ASSERT(wid == 15 && epoch == 15 && ts24 == 0xFFFFFFU);
    return true;
}

bool test_spsc_basic_fifo_order() {
    pulseguard::SpscRingBuffer<uint32_t, 8> rb;
    PG_ASSERT(rb.empty());
    PG_ASSERT(rb.capacity() == 8);

    for (uint32_t i = 10; i < 15; ++i) {
        PG_ASSERT(rb.try_push(i));
    }
    PG_ASSERT(!rb.empty());
    PG_ASSERT(rb.size_approx() == 5);

    for (uint32_t expected = 10; expected < 15; ++expected) {
        uint32_t v = 0;
        PG_ASSERT(rb.try_pop(v));
        PG_ASSERT(v == expected);
    }
    PG_ASSERT(rb.empty());
    uint32_t dummy = 0;
    PG_ASSERT(!rb.try_pop(dummy));
    return true;
}

bool test_spsc_full_drop_accounting() {
    pulseguard::SpscRingBuffer<uint32_t, 8> rb;
    for (uint32_t i = 0; i < 8; ++i) {
        PG_ASSERT(rb.try_push(i));
    }
    PG_ASSERT(!rb.try_push(999));
    PG_ASSERT(!rb.try_push(1000));
    PG_ASSERT(rb.dropped() == 2);

    uint32_t out = 0;
    PG_ASSERT(rb.try_pop(out) && out == 0);
    PG_ASSERT(rb.try_push(42));
    return true;
}

bool test_spsc_wraparound_integrity() {
    pulseguard::SpscRingBuffer<uint64_t, 16> rb;
    for (uint64_t round = 0; round < 2000; ++round) {
        for (uint64_t j = 0; j < 11; ++j) {
            const uint64_t val = round * 100ULL + j;
            PG_ASSERT(rb.try_push(val));
        }
        for (uint64_t j = 0; j < 11; ++j) {
            uint64_t out = 0;
            PG_ASSERT(rb.try_pop(out));
            PG_ASSERT(out == round * 100ULL + j);
        }
    }
    PG_ASSERT(rb.empty());
    PG_ASSERT(rb.dropped() == 0);
    return true;
}

struct SpscThreadCtx {
    pulseguard::SpscRingBuffer<uint64_t, 1024> rb{};
    uint64_t total{200000};
    bool ok{true};
};

void* spsc_producer_fn(void* arg) {
    auto* ctx = static_cast<SpscThreadCtx*>(arg);
    for (uint64_t i = 1; i <= ctx->total; ++i) {
        while (!ctx->rb.try_push(i)) {
            sched_yield();
        }
    }
    return nullptr;
}

bool test_spsc_concurrent_producer_consumer() {
    SpscThreadCtx ctx{};
    pthread_t prod{};
    PG_ASSERT(pthread_create(&prod, nullptr, spsc_producer_fn, &ctx) == 0);

    uint64_t expected = 1;
    while (expected <= ctx.total) {
        uint64_t val = 0;
        if (ctx.rb.try_pop(val)) {
            if (val != expected) {
                ctx.ok = false;
                break;
            }
            ++expected;
        } else {
            sched_yield();
        }
    }
    pthread_join(prod, nullptr);
    PG_ASSERT(ctx.ok);
    PG_ASSERT(expected == ctx.total + 1);
    PG_ASSERT(ctx.rb.empty());
    return true;
}

bool test_spsc_drain_limit() {
    pulseguard::SpscRingBuffer<int, 16> rb;
    for (int i = 0; i < 10; ++i) {
        PG_ASSERT(rb.try_push(i));
    }
    int sum = 0;
    const std::size_t n1 = rb.drain([&](int v) { sum += v; }, 4);
    PG_ASSERT(n1 == 4);
    PG_ASSERT(sum == 0 + 1 + 2 + 3);
    const std::size_t n2 = rb.drain([&](int v) { sum += v; });
    PG_ASSERT(n2 == 6);
    PG_ASSERT(sum == 45);
    return true;
}

bool test_zero_alloc_hot_path() {
    pulseguard::SpscRingBuffer<pulseguard::PulseSample, 64> rb;
    pulseguard::JitterHistogram hist;
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    pulseguard::copy_cstr(wc.name, "hot_worker");
    wd.register_worker(wc);

    char jsonl_buf[2048]{};
    char ev_buf[512]{};

    g_hot_alloc_calls = 0;
    g_track_hot_allocs = true;

    for (uint64_t i = 0; i < 128; ++i) {
        pulseguard::PulseSample s{};
        s.worker_id = 0;
        s.ipc_latency_us = 12 + (i % 20);
        s.recv_ns = 1'000'000ULL + i * 1'000'000ULL;
        (void)rb.try_push(s);
        pulseguard::PulseSample out{};
        (void)rb.try_pop(out);
        hist.record(out.ipc_latency_us);
        wd.on_pulse(out.worker_id, out.ipc_latency_us, out.recv_ns, 1, false, false);
        (void)wd.tick(out.recv_ns + 500'000ULL);
    }
    const std::size_t jn = pulseguard::format_telemetry_jsonl(
        jsonl_buf, sizeof(jsonl_buf), 1, 123456, hist, 128, 0, wd);
    const auto bin = pulseguard::build_binary_frame(1, 123456, hist, 128, 0, wd);
    pulseguard::WatchdogEvent ev{};
    ev.worker_id = 0;
    pulseguard::copy_cstr(ev.worker_name, "hot_worker");
    const std::size_t en = pulseguard::format_fault_event_jsonl(ev_buf, sizeof(ev_buf), 2, ev);

    g_track_hot_allocs = false;

    PG_ASSERT(g_hot_alloc_calls == 0);
    PG_ASSERT(jn > 50);
    PG_ASSERT(en > 30);
    PG_ASSERT(bin.magic == 0x504C5347U);
    return true;
}

bool test_histogram_empty_and_single() {
    pulseguard::JitterHistogram h;
    PG_ASSERT(h.count() == 0);
    PG_ASSERT(h.p50_us() == 0);
    PG_ASSERT(h.p99_us() == 0);
    PG_ASSERT(h.min_us() == 0);
    PG_ASSERT(h.max_us() == 0);

    h.record(42);
    PG_ASSERT(h.count() == 1);
    PG_ASSERT(h.min_us() == 42);
    PG_ASSERT(h.max_us() == 42);
    PG_ASSERT(h.p50_us() == 42);
    PG_ASSERT(h.p95_us() == 42);
    PG_ASSERT(h.p99_us() == 42);
    PG_ASSERT(h.p999_us() == 42);
    return true;
}

bool test_histogram_exact_1us_low_range() {
    pulseguard::JitterHistogram h;
    for (uint64_t v = 1; v <= 60; ++v) {
        h.record(v);
    }
    PG_ASSERT(h.count() == 60);
    PG_ASSERT(h.min_us() == 1);
    PG_ASSERT(h.max_us() == 60);
    PG_ASSERT(h.p50_us() == 30);
    PG_ASSERT(h.p95_us() == 57);
    PG_ASSERT(h.p99_us() == 60);
    return true;
}

bool test_histogram_high_dynamic_range() {
    pulseguard::JitterHistogram h;
    for (int i = 0; i < 990; ++i) {
        h.record(10);
    }
    for (int i = 0; i < 9; ++i) {
        h.record(500);
    }
    h.record(50000);

    PG_ASSERT(h.count() == 1000);
    PG_ASSERT(h.p50_us() == 10);
    PG_ASSERT(h.p95_us() == 10);
    PG_ASSERT(h.p99_us() == 10);
    PG_ASSERT(h.p999_us() >= 48000 && h.p999_us() <= 52000);
    PG_ASSERT(h.max_us() == 50000);
    return true;
}

bool test_histogram_overflow_bucket() {
    pulseguard::JitterHistogram h;
    h.record(2'500'000ULL);
    PG_ASSERT(h.max_us() == 2'500'000ULL);
    PG_ASSERT(h.p99_us() == 2'500'000ULL);
    return true;
}

bool test_histogram_merge() {
    pulseguard::JitterHistogram a;
    pulseguard::JitterHistogram b;
    for (int i = 0; i < 50; ++i) {
        a.record(15);
    }
    for (int i = 0; i < 50; ++i) {
        b.record(45);
    }
    a.merge(b);
    PG_ASSERT(a.count() == 100);
    PG_ASSERT(a.min_us() == 15);
    PG_ASSERT(a.max_us() == 45);
    PG_ASSERT(a.p50_us() == 15);
    PG_ASSERT(a.p95_us() == 45);
    return true;
}

struct MockWatchdogCallbacks {
    int respawn_calls{0};
    uint8_t last_respawn_id{255};
    uint32_t last_next_epoch{0};
    int escalate_calls{0};
    int last_escalated_prio{0};
};

bool mock_respawn(uint8_t id, uint32_t next_epoch, void* ctx) {
    auto* m = static_cast<MockWatchdogCallbacks*>(ctx);
    ++m->respawn_calls;
    m->last_respawn_id = id;
    m->last_next_epoch = next_epoch;
    return true;
}

bool mock_escalate(uint8_t, int prio, void* ctx) {
    auto* m = static_cast<MockWatchdogCallbacks*>(ctx);
    ++m->escalate_calls;
    m->last_escalated_prio = prio;
    return true;
}

bool test_watchdog_register_bounds() {
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 15;
    PG_ASSERT(wd.register_worker(wc));
    wc.worker_id = 16;
    PG_ASSERT(!wd.register_worker(wc));
    PG_ASSERT(wd.worker_count() == 1);
    return true;
}

bool test_watchdog_warn_and_hysteresis() {
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    wc.warn_deadline_us = 2000;
    wc.breach_deadline_us = 5000;
    wc.warn_hysteresis = 3;
    wd.register_worker(wc);

    uint64_t t = 1'000'000'000ULL;
    wd.on_pulse(0, 15, t, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::OK);

    // Warning pulse (jitter > warn_deadline_us)
    t += 2'500'000ULL;
    wd.on_pulse(0, 2500, t, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::WARN);

    // Two healthy pulses: hysteresis requires 3, so remain WARN.
    t += 1'000'000ULL;
    wd.on_pulse(0, 20, t, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::WARN);

    t += 1'000'000ULL;
    wd.on_pulse(0, 20, t, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::WARN);

    // 3rd healthy pulse clears WARN -> OK.
    t += 1'000'000ULL;
    wd.on_pulse(0, 20, t, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::OK);
    return true;
}

bool test_watchdog_breach_stale_epoch_and_handshake() {
    MockWatchdogCallbacks cb{};
    pulseguard::DeadlineWatchdog wd;
    wd.set_callbacks(mock_respawn, mock_escalate, &cb);

    pulseguard::WorkerConfig wc{};
    wc.worker_id = 2;
    wc.warn_deadline_us = 2000;
    wc.breach_deadline_us = 4000;
    wc.warn_hysteresis = 2;
    wd.register_worker(wc);

    uint64_t t = 10'000'000'000ULL;
    wd.on_pulse(2, 10, t, 1, false, false);

    // Breach via delayed pulse (> 4000 us)
    t += 6'000'000ULL;
    wd.on_pulse(2, 6000, t, 1, false, false);
    PG_ASSERT(wd.worker(2)->state == pulseguard::WorkerState::RECOVERING);
    PG_ASSERT(cb.respawn_calls == 1);
    PG_ASSERT(cb.last_respawn_id == 2);
    PG_ASSERT(cb.last_next_epoch == 2);

    // Stale pre-crash pulse (epoch 1, non-handshake) must be ignored while RECOVERING.
    wd.on_pulse(2, 10, t + 100'000ULL, 1, false, false);
    PG_ASSERT(wd.worker(2)->state == pulseguard::WorkerState::RECOVERING);

    // New epoch 2 RestartHandshake arrives 450 us after fault detection.
    wd.on_pulse(2, 12, t + 450'000ULL, 2, true, false);
    PG_ASSERT(wd.worker(2)->state == pulseguard::WorkerState::RESTARTED);
    PG_ASSERT(wd.worker(2)->last_recovery_us == 450ULL);
    PG_ASSERT(wd.worker(2)->epoch == 2);

    // Next healthy pulse satisfies hysteresis=2 -> OK.
    wd.on_pulse(2, 12, t + 1'450'000ULL, 2, false, false);
    PG_ASSERT(wd.worker(2)->state == pulseguard::WorkerState::OK);
    return true;
}

bool test_watchdog_tick_missed_pulse() {
    MockWatchdogCallbacks cb{};
    pulseguard::DeadlineWatchdog wd;
    wd.set_callbacks(mock_respawn, mock_escalate, &cb);

    pulseguard::WorkerConfig wc{};
    wc.worker_id = 1;
    wc.warn_deadline_us = 2000;
    wc.breach_deadline_us = 4000;
    wd.register_worker(wc);

    const uint64_t t0 = 5'000'000'000ULL;
    wd.on_pulse(1, 15, t0, 1, false, false);

    PG_ASSERT(wd.tick(t0 + 2'500'000ULL) == 1);
    PG_ASSERT(wd.worker(1)->state == pulseguard::WorkerState::WARN);

    PG_ASSERT(wd.tick(t0 + 4'500'000ULL) == 1);
    PG_ASSERT(wd.worker(1)->state == pulseguard::WorkerState::RECOVERING);
    PG_ASSERT(cb.respawn_calls == 1);
    return true;
}

bool test_watchdog_priority_inversion_escalation() {
    MockWatchdogCallbacks cb{};
    pulseguard::DeadlineWatchdog wd;
    wd.set_callbacks(mock_respawn, mock_escalate, &cb);

    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    wc.nominal_priority = 24;
    wc.ceiling_priority = 48;
    wc.warn_hysteresis = 2;
    wd.register_worker(wc);

    const uint64_t t0 = 2'000'000'000ULL;
    wd.on_pulse(0, 10, t0, 1, false, false);
    wd.report_priority_inversion(0, t0 + 500'000ULL, 10);

    PG_ASSERT(cb.escalate_calls == 1);
    PG_ASSERT(cb.last_escalated_prio == 48);
    PG_ASSERT(wd.worker(0)->priority_inversion_detected);
    PG_ASSERT(wd.worker(0)->current_priority == 48);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::WARN);

    wd.on_pulse(0, 10, t0 + 1'500'000ULL, 1, false, false);
    wd.on_pulse(0, 10, t0 + 2'500'000ULL, 1, false, false);
    PG_ASSERT(wd.worker(0)->state == pulseguard::WorkerState::OK);
    PG_ASSERT(!wd.worker(0)->priority_inversion_detected);
    PG_ASSERT(wd.worker(0)->current_priority == 24);
    return true;
}

bool test_watchdog_event_ring_wrap() {
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    wc.warn_deadline_us = 100;
    wc.breach_deadline_us = 5000;
    wc.warn_hysteresis = 1;
    wd.register_worker(wc);

    uint64_t t = 1'000'000'000ULL;
    for (int i = 0; i < 45; ++i) {
        t += 200'000ULL;
        wd.on_pulse(0, 200, t, 1, false, false);  // WARN
        t += 50'000ULL;
        wd.on_pulse(0, 20, t, 1, false, false);   // OK
    }
    PG_ASSERT(wd.event_total() >= 80);
    pulseguard::WatchdogEvent recent[pulseguard::DeadlineWatchdog::kEventHistoryCap]{};
    const std::size_t n = wd.copy_recent_events(
        recent, pulseguard::DeadlineWatchdog::kEventHistoryCap);
    PG_ASSERT(n == pulseguard::DeadlineWatchdog::kEventHistoryCap);
    for (std::size_t i = 1; i < n; ++i) {
        PG_ASSERT(recent[i].ts_ns >= recent[i - 1].ts_ns);
    }
    return true;
}

bool test_json_telemetry_and_event_schema() {
    pulseguard::JitterHistogram hist;
    hist.record(12);
    hist.record(24);
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    pulseguard::copy_cstr(wc.name, "lidar\"fusion\\0");
    wd.register_worker(wc);

    char buf[2048]{};
    const std::size_t n = pulseguard::format_telemetry_jsonl(
        buf, sizeof(buf), 7, 999000, hist, 2, 0, wd);
    PG_ASSERT(n > 0 && buf[n - 1] == '\n');
    PG_ASSERT(std::strstr(buf, "\"type\":\"telemetry\"") != nullptr);
    PG_ASSERT(std::strstr(buf, "\"p50_us\":12") != nullptr);
    PG_ASSERT(std::strstr(buf, "\"p99_us\":24") != nullptr);
    PG_ASSERT(std::strstr(buf, "lidar\\\"fusion\\\\0") != nullptr);

    pulseguard::WatchdogEvent ev{};
    ev.ts_ns = 123456000ULL;
    ev.worker_id = 0;
    pulseguard::copy_cstr(ev.worker_name, "brake_actuator");
    ev.from_state = pulseguard::WorkerState::RECOVERING;
    ev.to_state = pulseguard::WorkerState::RESTARTED;
    ev.reason = pulseguard::FaultReason::RESPAWN_COMPLETE;
    ev.recovery_us = 380;
    ev.epoch = 2;
    char ev_buf[512]{};
    const std::size_t en = pulseguard::format_fault_event_jsonl(ev_buf, sizeof(ev_buf), 8, ev);
    PG_ASSERT(en > 0 && ev_buf[en - 1] == '\n');
    PG_ASSERT(std::strstr(ev_buf, "\"type\":\"fault_event\"") != nullptr);
    PG_ASSERT(std::strstr(ev_buf, "\"recovery_us\":380") != nullptr);
    PG_ASSERT(std::strstr(ev_buf, "\"reason\":\"RESPAWN_COMPLETE\"") != nullptr);
    return true;
}

bool test_binary_frame_crc32() {
    pulseguard::JitterHistogram hist;
    hist.record(18);
    pulseguard::DeadlineWatchdog wd;
    pulseguard::WorkerConfig wc{};
    wc.worker_id = 0;
    wd.register_worker(wc);

    auto frame = pulseguard::build_binary_frame(5, 424242, hist, 1, 0, wd);
    PG_ASSERT(frame.magic == 0x504C5347U);
    const uint32_t expected_crc = pulseguard::compute_crc32(
        reinterpret_cast<const uint8_t*>(&frame),
        sizeof(frame) - sizeof(uint32_t));
    PG_ASSERT(frame.crc32 == expected_crc);

    frame.p99_us ^= 0x01U;
    const uint32_t corrupted_crc = pulseguard::compute_crc32(
        reinterpret_cast<const uint8_t*>(&frame),
        sizeof(frame) - sizeof(uint32_t));
    PG_ASSERT(corrupted_crc != expected_crc);
    return true;
}

bool test_ipc_transport_roundtrip_and_timeout() {
    pulseguard::PulseTransport tx;
    PG_ASSERT(tx.open_server());

    pulseguard::PulseFrame empty{};
    PG_ASSERT(tx.receive_pulse(empty, 5) == pulseguard::PulseTransport::RecvStatus::Timeout);

    pulseguard::PulseFrame send_f{};
    send_f.worker_id = 3;
    send_f.kind = pulseguard::PulseKind::RestartHandshake;
    send_f.epoch = 7;
    send_f.send_ts_us24 = 0x00AABBCCU;
    send_f.priority = 30;
    send_f.seq = 99;
    PG_ASSERT(tx.send_pulse(send_f));

    pulseguard::PulseFrame recv_f{};
    PG_ASSERT(tx.receive_pulse(recv_f, 50) == pulseguard::PulseTransport::RecvStatus::Received);
    PG_ASSERT(recv_f.worker_id == 3);
    PG_ASSERT(recv_f.kind == pulseguard::PulseKind::RestartHandshake);
    PG_ASSERT(recv_f.epoch == 7);
    PG_ASSERT(recv_f.send_ts_us24 == 0x00AABBCCU);
    tx.close();
    return true;
}

bool test_resmgr_host_snapshot_and_binary() {
    pulseguard::ResourceManagerHost rm;
    pulseguard::ResMgrConfig cfg{};
    cfg.write_host_files = true;
    cfg.host_snapshot_path = "test_pg_snap.json";
    cfg.host_jsonl_path = "test_pg_stream.jsonl";
    cfg.host_binary_path = "test_pg_snap.bin";

    PG_ASSERT(rm.start(cfg));

    const char* line = "{\"type\":\"telemetry\",\"seq\":1}\n";
    pulseguard::TelemetryBinaryFrame bin{};
    bin.magic = 0x504C5347U;
    bin.seq = 1;
    bin.p99_us = 33;
    rm.publish_telemetry(line, std::strlen(line), bin, true);

    char read_buf[256]{};
    const std::size_t rn = rm.read_latest_jsonl(read_buf, sizeof(read_buf));
    PG_ASSERT(rn == std::strlen(line));
    PG_ASSERT(std::strcmp(read_buf, line) == 0);
    PG_ASSERT(rm.read_latest_binary().p99_us == 33);
    PG_ASSERT(rm.publish_count() == 1);

    rm.stop();
    ::unlink("test_pg_snap.json");
    ::unlink("test_pg_stream.jsonl");
    ::unlink("test_pg_snap.bin");
    return true;
}

bool test_e2e_engine_healthy_run() {
    pulseguard::PulseGuardEngine eng;
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 3;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 6000;
    cfg.breach_deadline_us = 15000;
    cfg.resmgr.write_host_files = false;

    PG_ASSERT(eng.start(cfg));
    ::usleep(180'000);
    eng.stop();

    const auto s = eng.summary();
    PG_ASSERT(s.total_pulses >= 200);
    PG_ASSERT(s.dropped_pulses == 0);
    PG_ASSERT(s.total_restarts == 0);
    PG_ASSERT(s.p99_us < 5000);
    return true;
}

bool test_e2e_cpu_stall_sub10ms_recovery() {
    pulseguard::PulseGuardEngine eng;
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 2;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3000;
    cfg.breach_deadline_us = 5000;
    cfg.telemetry_interval_ms = 20;
    cfg.resmgr.write_host_files = false;

    PG_ASSERT(eng.start(cfg));
    ::usleep(60'000);
    eng.inject_cpu_stall(0, 15'000);

    const uint64_t deadline = pulseguard::now_ns() + 500'000'000ULL;
    while (pulseguard::now_ns() < deadline && eng.summary().total_restarts == 0) {
        ::usleep(500);
    }
    eng.stop();

    const auto s = eng.summary();
    PG_ASSERT(s.total_restarts >= 1);
    PG_ASSERT(s.last_recovery_us > 0);
    PG_ASSERT(s.last_recovery_us < 10'000ULL);
    return true;
}

bool test_e2e_missed_pulse_sub10ms_recovery() {
    pulseguard::PulseGuardEngine eng;
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 2;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3000;
    cfg.breach_deadline_us = 5000;
    cfg.resmgr.write_host_files = false;

    PG_ASSERT(eng.start(cfg));
    ::usleep(60'000);
    eng.inject_missed_pulse(1, 15'000);

    const uint64_t deadline = pulseguard::now_ns() + 500'000'000ULL;
    while (pulseguard::now_ns() < deadline && eng.summary().total_restarts == 0) {
        ::usleep(500);
    }
    eng.stop();

    const auto s = eng.summary();
    PG_ASSERT(s.total_restarts >= 1);
    PG_ASSERT(s.last_recovery_us < 10'000ULL);
    return true;
}

bool test_e2e_priority_inversion_and_jsonl_file() {
    pulseguard::PulseGuardEngine eng;
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 2;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3500;
    cfg.breach_deadline_us = 12000;
    cfg.telemetry_interval_ms = 20;
    cfg.resmgr.write_host_files = true;
    cfg.resmgr.host_snapshot_path = "test_e2e_snap.json";
    cfg.resmgr.host_jsonl_path = "test_e2e_stream.jsonl";
    cfg.resmgr.host_binary_path = "test_e2e_snap.bin";

    PG_ASSERT(eng.start(cfg));
    ::usleep(50'000);
    eng.inject_priority_inversion(0, 2000);

    const uint64_t deadline = pulseguard::now_ns() + 400'000'000ULL;
    while (pulseguard::now_ns() < deadline && eng.summary().total_inversions == 0) {
        ::usleep(500);
    }
    ::usleep(40'000);
    eng.stop();

    const auto s = eng.summary();
    PG_ASSERT(s.total_inversions >= 1);
    PG_ASSERT(s.pi_mutex_ready);

    // Validate JSONL file lines are non-empty balanced JSON objects.
    std::FILE* fp = std::fopen("test_e2e_stream.jsonl", "r");
    PG_ASSERT(fp != nullptr);
    char line[4096]{};
    int line_count = 0;
    while (std::fgets(line, sizeof(line), fp) != nullptr) {
        const std::size_t len = std::strlen(line);
        PG_ASSERT(len >= 10 && line[0] == '{' && line[len - 1] == '\n');
        ++line_count;
    }
    std::fclose(fp);
    PG_ASSERT(line_count >= 2);

    ::unlink("test_e2e_snap.json");
    ::unlink("test_e2e_stream.jsonl");
    ::unlink("test_e2e_snap.bin");
    return true;
}

bool test_histogram_p999_stress_10k() {
    pulseguard::JitterHistogram h;
    for (uint64_t i = 0; i < 9990; ++i) {
        h.record(8 + (i % 5));
    }
    for (uint64_t i = 0; i < 9; ++i) {
        h.record(200);
    }
    h.record(8000);
    PG_ASSERT(h.count() == 10000);
    PG_ASSERT(h.p50_us() >= 8 && h.p50_us() <= 12);
    PG_ASSERT(h.p99_us() == 12);
    PG_ASSERT(h.p999_us() >= 190 && h.p999_us() <= 8000);
    PG_ASSERT(h.max_us() == 8000);
    return true;
}

bool test_binary_frame_size_and_layout() {
    PG_ASSERT(sizeof(pulseguard::TelemetryBinaryFrame) <= 128);
    PG_ASSERT(DCMD_PULSEGUARD_GET_BINARY != 0U);
    return true;
}

bool test_e2e_multiworker_simultaneous_recovery() {
    pulseguard::PulseGuardEngine eng;
    pulseguard::EngineConfig cfg{};
    cfg.num_workers = 4;
    cfg.period_us = 1000;
    cfg.warn_deadline_us = 3000;
    cfg.breach_deadline_us = 5000;
    cfg.resmgr.write_host_files = false;

    PG_ASSERT(eng.start(cfg));
    ::usleep(50'000);
    eng.inject_cpu_stall(0, 14'000);
    eng.inject_missed_pulse(2, 14'000);

    const uint64_t deadline = pulseguard::now_ns() + 600'000'000ULL;
    while (pulseguard::now_ns() < deadline && eng.summary().total_restarts < 2) {
        ::usleep(500);
    }
    eng.stop();

    const auto s = eng.summary();
    PG_ASSERT(s.total_restarts >= 2);
    PG_ASSERT(s.max_recovery_us > 0);
    PG_ASSERT(s.max_recovery_us < 10'000ULL);
    return true;
}

}  // namespace

int main() {
    RUN_TEST(test_clock_monotonic_and_us24_wrap);
    RUN_TEST(test_pulse_bitpack_roundtrip);
    RUN_TEST(test_spsc_basic_fifo_order);
    RUN_TEST(test_spsc_full_drop_accounting);
    RUN_TEST(test_spsc_wraparound_integrity);
    RUN_TEST(test_spsc_concurrent_producer_consumer);
    RUN_TEST(test_spsc_drain_limit);
    RUN_TEST(test_zero_alloc_hot_path);
    RUN_TEST(test_histogram_empty_and_single);
    RUN_TEST(test_histogram_exact_1us_low_range);
    RUN_TEST(test_histogram_high_dynamic_range);
    RUN_TEST(test_histogram_overflow_bucket);
    RUN_TEST(test_histogram_merge);
    RUN_TEST(test_histogram_p999_stress_10k);
    RUN_TEST(test_watchdog_register_bounds);
    RUN_TEST(test_watchdog_warn_and_hysteresis);
    RUN_TEST(test_watchdog_breach_stale_epoch_and_handshake);
    RUN_TEST(test_watchdog_tick_missed_pulse);
    RUN_TEST(test_watchdog_priority_inversion_escalation);
    RUN_TEST(test_watchdog_event_ring_wrap);
    RUN_TEST(test_json_telemetry_and_event_schema);
    RUN_TEST(test_binary_frame_crc32);
    RUN_TEST(test_binary_frame_size_and_layout);
    RUN_TEST(test_ipc_transport_roundtrip_and_timeout);
    RUN_TEST(test_resmgr_host_snapshot_and_binary);
    RUN_TEST(test_e2e_engine_healthy_run);
    RUN_TEST(test_e2e_cpu_stall_sub10ms_recovery);
    RUN_TEST(test_e2e_missed_pulse_sub10ms_recovery);
    RUN_TEST(test_e2e_priority_inversion_and_jsonl_file);
    RUN_TEST(test_e2e_multiworker_simultaneous_recovery);

    std::printf("\nSummary: %d/%d tests passed (%d assertions verified)\n",
                g_tests_passed, g_tests_run, g_assertions);
    return (g_tests_passed == g_tests_run) ? 0 : 1;
}
