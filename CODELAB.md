# QNX-PulseGuard Codelab: Building a Deterministic IPC Watchdog & `/dev/pulseguard` Resource Manager in QNX 8.0

**Target Audience:** Embedded C++ developers getting started with **QNX Everywhere (QNX SDP 8.0)** on **QEMU x86_64 / aarch64** (no physical board required).  
**Time Required:** ~45 minutes  
**What You Will Build:** A real-time microkernel heartbeat monitor using `ChannelCreate_r`, `MsgSendPulse_r`, `ClockCycles()`, a wait-free SPSC ring buffer, an O(1) latency histogram, a `/dev/pulseguard` QNX Resource Manager, and a fault-injection harness proving sub-10ms worker recovery.

---

## Architecture Overview

Unlike monolithic kernels where drivers and file nodes live inside kernel space, **QNX Neutrino RTOS 8.0** keeps only scheduling, synchronization, and message-passing inside the `procnto` microkernel. Everything else — including virtual device nodes like `/dev/pulseguard` — runs as an isolated user-space **Resource Manager**.

`QNX-PulseGuard` separates the hard real-time pulse receive path from device I/O so client reads never perturb worker scheduling jitter:

```text
+--------------------+     MsgSendPulse_r()      +------------------------+
| Worker 0 (prio 24) | ------------------------> | RX Thread (SCHED_FIFO) |
| Worker 1 (prio 22) |   struct _pulse (32-bit   | MsgReceivePulse_r()    |
| Worker 2 (prio 20) |   worker|epoch|ts_us24)   | ClockCycles() delta    |
+--------------------+                           +-----------+------------+
                                                             |
                                            wait-free try_push() (0 alloc)
                                                             v
+--------------------+     try_pop() + tick()    +------------------------+
|  /dev/pulseguard   | <------------------------ | SPSC Ring Buffer (4K)  |
|  resmgr_attach()   |   JSONL read() / devctl() +------------------------+
|  (Dispatch Thread) |             |
+--------------------+             v
                       +------------------------+
                       | JitterHistogram (4 KB) |
                       | DeadlineWatchdog FSM   |
                       | Sub-10ms Respawn Hook  |
                       +------------------------+
```

---

## Module 1 — Environment Setup & Dual-Target Compilation

To iterate quickly on any laptop before booting a QNX VM, `QNX-PulseGuard` compiles cleanly against both native QNX SDP 8.0 (`__QNXNTO__`) and standard POSIX hosts (macOS/Linux) with `-std=c++20 -O2 -Wall -Wextra -Werror`.

### 1.1 Verify Host Build & Tests First

```sh
git clone <your-repo-url> qnx-2026-pulseguard
cd qnx-2026-pulseguard
make CXX=clang++ test
```

You should see `30/30 tests passed` with zero compiler warnings.

### 1.2 Cross-Compile for QNX SDP 8.0

If you have QNX SDP 8.0 installed via QNX Everywhere:

```sh
source ~/qnx800/qnxsdp-env.sh
make qnx-x86_64    # compiles build/pulseguard_daemon & build/fault_injector with q++ -Vgcc_ntox86_64
```

For ARM64 QEMU targets (`qemu-system-aarch64`), run:

```sh
make qnx-aarch64
```

---

## Module 2 — Microsecond Hardware Timing with `ClockCycles()`

Measuring IPC scheduling latency with `gettimeofday()` or coarse tick timers introduces syscall overhead and NTP slew artifacts. On QNX 8.0, `ClockCycles()` reads the synchronized CPU cycle counter directly (`RDTSC` on x86_64 or `CNTVCT_EL0` on AArch64), and `SYSPAGE_ENTRY(qtime)->cycles_per_sec` converts raw cycles into nanoseconds.

Open [include/pulseguard/clock.hpp](include/pulseguard/clock.hpp):

```cpp
inline uint64_t cycles_now() noexcept {
#ifdef __QNXNTO__
    return ClockCycles();
#elif defined(__APPLE__)
    return mach_continuous_time();
#else
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
#endif
}
```

To convert cycle deltas without 64-bit integer overflow over multi-day uptimes, `cycles_to_ns()` promotes the multiply to 128-bit arithmetic (`__uint128_t`) before dividing by `cycles_per_sec()`.

### Bit-Packing Timestamps Inside a 32-Bit QNX Pulse

A QNX pulse (`struct _pulse`) delivers an 8-bit `code` and a 32-bit `value.sival_int` non-blockingly — no memory copy, no `MsgReply()` blocking, and priority-ordered queueing in the kernel.

Look at how [src/qnx_ipc_bridge.hpp](src/qnx_ipc_bridge.hpp) packs all metadata required by the watchdog into a single 32-bit integer:

| Bit Range | Width | Field | Purpose |
|---|---|---|---|
| `31 .. 28` | 4 bits | `worker_id` (`0..15`) | Identifies which real-time worker emitted the heartbeat |
| `27 .. 24` | 4 bits | `epoch & 0x0F` | Prevents stale pre-crash pulses from fooling recovery |
| `23 .. 0` | 24 bits | `ts_us & 0xFFFFFF` | Microsecond send timestamp (16.77s wrap window) |

```cpp
inline constexpr int32_t pack_pulse_value(uint8_t worker_id, uint16_t epoch,
                                          uint32_t ts_us24) noexcept {
    const uint32_t packed =
        ((static_cast<uint32_t>(worker_id) & 0x0FU) << 28U) |
        ((static_cast<uint32_t>(epoch) & 0x0FU) << 24U) |
        (ts_us24 & 0x00FFFFFFU);
    return static_cast<int32_t>(packed);
}
```

Subtracting 24-bit unsigned timestamps modulo `2^24` recovers exact IPC transit latency even across wrap-around boundaries:

```cpp
inline constexpr uint64_t delta_ts_us24(uint32_t recv_us24, uint32_t send_us24) noexcept {
    return static_cast<uint64_t>((recv_us24 - send_us24) & 0x00FFFFFFU);
}
```

---

## Module 3 — QNX Channels, Connections, and `_r` Kernel Calls

In QNX Neutrino, kernel calls ending in `_r` (reentrant) never mutate thread-local `errno`. Instead, they return:

- `>= 0` on success (e.g., `chid` from `ChannelCreate_r()`, `coid` from `ConnectAttach_r()`, or `EOK` `0` from `MsgSendPulse_r()`)
- `-errno` (negative error code) on failure

In [src/qnx_ipc_bridge.hpp](src/qnx_ipc_bridge.hpp):

1. **Server creates a channel** with disconnect/unblock notifications enabled:
   ```cpp
   const int ch = ChannelCreate_r(_NTO_CHF_DISCONNECT | _NTO_CHF_COID_DISCONNECT |
                                  _NTO_CHF_UNBLOCK);
   ```
2. **Sender attaches a side-channel connection** using `_NTO_SIDE_CHANNEL` so `coid` lives outside the POSIX file-descriptor table (preventing conflicts with `open()`/`close()`):
   ```cpp
   const int co = ConnectAttach_r(ND_LOCAL_NODE, pid, chid, _NTO_SIDE_CHANNEL, 0);
   ```
3. **Worker emits non-blocking pulses** with `MsgSendPulse_r()` using user codes starting at `_PULSE_CODE_MINAVAIL`:
   ```cpp
   const int code = _PULSE_CODE_MINAVAIL + static_cast<int>(frame.kind);
   const int rc = MsgSendPulse_r(coid_, frame.priority, code, packed);
   ```
4. **Receiver blocks with deterministic timeout** using `TimerTimeout_r(CLOCK_MONOTONIC, _NTO_TIMEOUT_RECEIVE, nullptr, &timeout_ns, nullptr)` (note `timeout_ns` is a `uint64_t` in nanoseconds, not a `timespec`), followed by `MsgReceivePulse_r()`. Any pulse with `pulse.code < 0` is a kernel system pulse (such as `_PULSE_CODE_COIDDEATH`), handled cleanly without crashing the receiver loop.

---

## Module 4 — Wait-Free Ring Buffer & O(1) HDR Jitter Histogram

Calling `malloc()` or `std::vector::push_back()` inside a real-time loop risks heap lock contention and unbounded worst-case execution time (WCET). `QNX-PulseGuard` enforces zero dynamic allocation on the entire telemetry hot path (verified in `test_zero_alloc_hot_path` via a global `operator new` counter).

### 4.1 Wait-Free SPSC Ring Buffer ([include/pulseguard/ring_buffer.hpp](include/pulseguard/ring_buffer.hpp))

- `SpscRingBuffer<PulseSample, 4096>` stores producer (`head_`) and consumer (`tail_`) atomic cursors on distinct `alignas(64)` cache lines to eliminate false sharing across CPU cores.
- Each side caches the opposite cursor locally (`tail_cache_` / `head_cache_`), reading the remote atomic with `std::memory_order_acquire` only when the local cache indicates full or empty.
- When full, `try_push()` increments an atomic drop counter (`dropped_`) and returns `false` immediately in constant time rather than blocking the receiver thread.

### 4.2 Fixed-Memory HDR Histogram ([include/pulseguard/histogram.hpp](include/pulseguard/histogram.hpp))

Instead of storing raw samples and sorting them for percentiles, `JitterHistogram` maps microsecond latencies into **513 `uint64_t` buckets** (`4.1 KB` total — fits inside L1 data cache):

- `0 .. 63 us`: **exact 1 us resolution** (every integer microsecond has its own bucket)
- `64 .. 1,048,575 us`: 15 power-of-2 octaves with 32 sub-buckets each (`<= 3.125%` worst-case bucket width) via C++20 `std::bit_width()`
- `>= 1,048,576 us`: overflow bucket clamped to exact tracked `max_us_`

Inserting a sample takes O(1) time with zero branches beyond the low-range fast path.

---

## Module 5 — Exposing `/dev/pulseguard` as a QNX Resource Manager

A QNX Resource Manager registers a virtual pathname inside `procnto`'s global namespace so any tool (`cat /dev/pulseguard`, `head`, monitoring scripts, or C clients calling `open()`/`read()`/`devctl()`) reads live telemetry through standard POSIX syscalls.

Walk through [src/resmgr_pulseguard.cpp](src/resmgr_pulseguard.cpp):

1. **Initialize default POSIX callbacks** with `iofunc_func_init(_RESMGR_CONNECT_NFUNCS, &connect_funcs, _RESMGR_IO_NFUNCS, &io_funcs)`.
2. **Override `read`, `read64`, and `devctl`** handlers in `resmgr_io_funcs_t`:
   - `pulseguard_io_read`: verifies read permissions with `iofunc_read_verify()`. When `ocb->offset == 0`, it latches a consistent copy of the latest JSONL snapshot under a short mutex into `dispatch_scratch_`, releases the mutex, and responds using `SETIOV()` + `_IO_SET_READ_NBYTES()` + `_RESMGR_NPARTS(1)`.
   - `pulseguard_io_devctl`: delegates unknown commands to `iofunc_devctl_default()` and responds to `DCMD_PULSEGUARD_GET_BINARY` (`__DIOF(_DCMD_MISC, 0x41, TelemetryBinaryFrame)`) by writing the 128-byte packed, CRC32-checksummed binary struct into `_DEVCTL_DATA(msg->i)`.
3. **Mount the device path** with `resmgr_attach(dpp, &resmgr_attr, "/dev/pulseguard", _FTYPE_ANY, 0, &connect_funcs, &io_funcs, &pg_attr.base)` and enter the `dispatch_block()` / `dispatch_handler()` loop on a dedicated non-real-time thread.

On POSIX hosts (macOS/Linux), `ResourceManagerHost` mirrors the exact same `publish_telemetry()` / `read_latest_jsonl()` / `read_latest_binary()` contract using atomic `rename()` snapshots (`pulseguard_dev.json`, `pulseguard_dev.bin`) and an append-only JSONL stream (`pulseguard_telemetry.jsonl`).

---

## Module 6 — Watchdog State Machine, Priority Inversion & Sub-10ms Recovery

Open [include/pulseguard/watchdog.hpp](include/pulseguard/watchdog.hpp) to inspect the worker finite-state machine:

```text
          jitter > warn_us OR PI
    +--------------------------------+
    |                                v
[  OK  ] <---- hysteresis (N) ---- [ WARN ]
    ^                                |
    |                                | silence/stall > breach_us
    | hysteresis (N)                 v
[RESTARTED] <-- RestartHandshake -- [RECOVERING] <-- respawn() -- [BREACH]
                (epoch == next)
```

Three mechanisms keep recovery deterministic and chatter-free:

1. **Hysteresis (`warn_hysteresis = 3`):** A single borderline jitter spike puts the worker in `WARN`, and it returns to `OK` only after 3 consecutive healthy pulses.
2. **Priority-Inversion Ceiling Escalation (`PTHREAD_PRIO_INHERIT` + Ceiling `45`):** When a low-priority contender blocks a critical worker on the shared resource mutex (`shared_bus_mtx_`), the watchdog flags `PRIORITY_INVERSION` and escalates the worker to `ceiling_priority = 45` until normal cadence resumes.
3. **Epoch Fencing During Respawn:** When `BREACH` fires, the watchdog increments `expected_epoch`, marks `fault_detected_ns`, retires the stalled thread, and launches the replacement thread with `new_epoch`. Any late pre-crash pulses carrying the old epoch are discarded; recovery latency is measured cleanly from `fault_detected_ns` to arrival of the new worker's `RestartHandshake` pulse (`RESPAWN_COMPLETE`).

### Hands-On Fault Injection Experiment

Run the fault injector locally (or inside your QNX QEMU VM):

```sh
./build/fault_injector --mode all --stall-ms 14 --jsonl-out web/sample_telemetry.jsonl
```

Expected output (verified on macOS ARM64 host):

```text
[fault_injector] baseline warmup 250ms...
[fault_injector] injecting cpu_stall (14ms) on worker 0...
[fault_injector] cpu_stall recovered=PASS restarts=1 completed_recoveries=1 last_recovery_us=81
[fault_injector] injecting priority_inversion on worker 1...
[fault_injector] priority_inversion escalated=PASS inversions=1
[fault_injector] injecting missed_pulse (14ms) on worker 2...
[fault_injector] missed_pulse recovered=PASS restarts=2 completed_recoveries=2 last_recovery_us=46
[fault_injector] summary pulses=1326 dropped=0 p50_us=9 p95_us=30 p99_us=57 p999_us=149 max_us=210 restarts=2 inversions=1 last_recovery_us=46 max_recovery_us=81 verdict=PASS
```

Both `cpu_stall` (`81 us`) and `missed_pulse` (`46 us`) recovered well inside the 10 ms (`10,000 us`) real-time budget with zero dropped pulses in the SPSC ring.

---

## Module 7 — Inspecting Telemetry in the Web Console

Open `web/index.html` in any modern browser (or serve the `web/` folder locally):

```sh
python3 -m http.server 8088 --directory web
# Open http://localhost:8088/
```

Use **Play**, **Step +1**, **Jump to Fault**, or **Load .jsonl** to inspect your own `pulseguard_telemetry.jsonl` capture from QEMU or host runs:

- **Top KPIs:** `p50 / p99`, `p99.9 / max`, total pulses, ring drops (`0`), completed recoveries, and priority inversions.
- **Latency Timeline:** Plots `p50`, `p99`, and `p99.9` curves alongside vertical fault/recovery markers.
- **Worker Gauges:** Displays live priority ceiling escalation (`22 -> 45`), epoch transitions, and per-worker recovery time.
- **State Ledger:** Audits every `OK -> WARN -> BREACH -> RECOVERING -> RESTARTED -> OK` transition.

---

## Verification Checklist & Troubleshooting

| Check | Expected Result | Command |
|---|---|---|
| Clean `-Werror` build | `0` warnings, `3` binaries built | `make CXX=clang++ clean all` |
| Unit + E2E tests | `30/30` passed, `0` hot-path allocations | `make CXX=clang++ test` |
| Sub-10ms recovery | `cpu_stall` & `missed_pulse` `< 10,000 us` | `./build/fault_injector --mode all` |
| `/dev/pulseguard` (QNX) | JSONL snapshot printed by `cat /dev/pulseguard` | See [QNX_QEMU_RUNBOOK.md](QNX_QEMU_RUNBOOK.md) |

**Common Issues:**
- *`sched_fifo=false` on host Linux/macOS:* Unprivileged users on Linux may lack `CAP_SYS_NICE`. Run with `sudo` or set `RLIMIT_RTPRIO` (`ulimit -r 99`). On QNX 8.0 inside QEMU as `root`, `SCHED_FIFO` applies natively.
- *QNX `q++` not found:* Ensure `source ~/qnx800/qnxsdp-env.sh` was executed in the current shell before running `make qnx-x86_64`.
