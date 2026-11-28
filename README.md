# QNX-PulseGuard


- **Narrated 1080p Demo (YouTube)**: https://www.youtube.com/watch?v=L1PQB58JKWY

Deterministic C++20 Real-Time Microkernel IPC Telemetry & Deadline Watchdog for **QNX Neutrino RTOS 8.0** (with full POSIX/macOS/Linux host-parity compilation and execution).

- **Live Telemetry Console (GitHub Pages):** https://alphonshhhomenicmtu.github.io/qnx-pulseguard/
- **8-Slide Architecture & Codelab Deck (PDF):** https://alphonshhhomenicmtu.github.io/qnx-pulseguard/qnx-pulseguard-deck.pdf (repo copy: [docs/qnx-pulseguard-deck.pdf](docs/qnx-pulseguard-deck.pdf))
- **Hands-On Developer Codelab (`1,776` words, 7 modules):** [CODELAB.md](CODELAB.md)
- **QNX SDP 8.0 QEMU Runbook (`x86_64` / `aarch64`):** [QNX_QEMU_RUNBOOK.md](QNX_QEMU_RUNBOOK.md)

## Overview

`QNX-PulseGuard` monitors safety-critical real-time worker processes over asynchronous kernel pulses, measures microsecond-level IPC scheduling jitter with zero heap allocations on the hot path, exposes live telemetry through a native `/dev/pulseguard` QNX Resource Manager, and recovers from CPU stalls, missed heartbeats, or priority inversions in under 10 milliseconds.

### Key Components

- **Native QNX Neutrino 8.0 IPC & Resource Manager (`src/qnx_ipc_bridge.hpp`, `src/resmgr_pulseguard.cpp`)**:
  - Uses `ChannelCreate_r()`, `ConnectAttach_r()`, `MsgSendPulse_r()`, `TimerTimeout_r()`, and `MsgReceivePulse_r()` under `#ifdef __QNXNTO__`, packing `(worker_id:4, epoch:8, ts_us:20)` inside QNX's 32-bit `struct _pulse` payload.
  - Attaches `/dev/pulseguard` via `resmgr_attach()`, serving zero-allocation JSONL telemetry on `read()` (`io_read` / `read64`) and CRC32-protected binary snapshots (`TelemetryBinaryFrame`) via `devctl()` (`DCMD_PULSEGUARD_GET_BINARY`).
  - Exercises `ClockCycles()` + `SYSPAGE_ENTRY(qtime)->cycles_per_sec` for sub-microsecond hardware timestamping and `SCHED_FIFO` + `PTHREAD_PRIO_INHERIT` / `PTHREAD_PRIO_PROTECT` thread/mutex attributes.
- **Lock-Free Real-Time Core (`include/pulseguard/`)**:
  - `ring_buffer.hpp`: Wait-free SPSC ring buffer (`SpscRingBuffer`) with 64-byte cache-line padding and acquire/release atomic cursors.
  - `histogram.hpp`: Fixed-memory 513-bucket HDR-style microsecond jitter histogram (`4.1 KB`, O(1) per sample, 1 us resolution from `0..63 us`, `p50/p95/p99/p99.9/max/mean`).
  - `watchdog.hpp`: Multi-worker deadline state machine (`OK -> WARN -> BREACH -> RECOVERING -> RESTARTED -> OK`) with hysteresis damping, epoch-fenced restart handshakes, priority-inversion ceiling escalation, and sub-10ms recovery tracking.
  - `telemetry_json.hpp`: Zero-allocation fixed-buffer JSONL and CRC32 binary frame serializer.
- **CLI Tools & Dashboard**:
  - `pulseguard_daemon`: Runs real-time IPC worker threads, pulse receiver, deadline watchdog, and `/dev/pulseguard` exporter.
  - `fault_injector`: Injects synthetic `cpu_stall`, `priority_inversion`, and `missed_pulse` faults to verify automatic detection and `< 10 ms` recovery.
  - `web/index.html`: Zero-build telemetry cockpit visualizing `/dev/pulseguard` JSONL captures (`p50/p99/p99.9` curves, worker deadline gauges, fault-recovery ledger).

## Quick Start (Host POSIX / macOS / Linux)

```sh
# Build all binaries and run the 31-test C++20 unit & E2E suite (-Wall -Wextra -Werror)
make CXX=clang++ test

# Run the live daemon with a synthetic CPU stall injected at 450 ms
./build/pulseguard_daemon --duration-ms 1000 --inject-at-ms 450

# Run the full fault-injection verification suite (CPU stall + priority inversion + missed pulse)
./build/fault_injector --mode all --stall-ms 14 --jsonl-out web/sample_telemetry.jsonl
```

You can also build and test via CMake:

```sh
cmake -B build_cmake -DCMAKE_CXX_COMPILER=clang++
cmake --build build_cmake -j4
ctest --test-dir build_cmake --output-on-failure
```

## QNX SDP 8.0 Cross-Compilation & QEMU Execution

Source your QNX SDP 8.0 environment and compile natively for `x86_64` or `aarch64`:

```sh
source ~/qnx800/qnxsdp-env.sh
make qnx-x86_64     # or: make qnx-aarch64
```

See [QNX_QEMU_RUNBOOK.md](QNX_QEMU_RUNBOOK.md) for step-by-step instructions to boot a QNX 8.0 QEMU VM, deploy the binaries, read `/dev/pulseguard`, and run the fault-recovery benchmark inside QEMU. For the full developer walkthrough, see [CODELAB.md](CODELAB.md).

## Measured Host Verification Results

Verified with Apple Clang (`-std=c++20 -O2 -Wall -Wextra -Werror`, zero warnings) on macOS ARM64:

- **Unit & E2E Test Suite (`./build/test_pulseguard`)**: `31/31` tests passed (`66,300+` assertions verified), including zero-allocation hot-path verification (`0` `operator new` calls on ring/histogram/watchdog/JSON serialization).
- **Fault Injector Baseline Trace (`./build/fault_injector --mode all --stall-ms 14 --jsonl-out web/sample_telemetry.jsonl`)**:
  - `total_pulses = 1,354`, `dropped_pulses = 0`
  - `p50 = 7 us`, `p95 = 26 us`, `p99 = 47 us`, `p99.9 = 74 us`, `max = 89 us`, `mean = 9.87 us`
  - `cpu_stall` recovery latency: **`29 us` (`0.029 ms`)** vs. `10 ms` SLA
  - `missed_pulse` recovery latency: **`49 us` (`0.049 ms`)** vs. `10 ms` SLA
  - `priority_inversion` escalation: detected & escalated to ceiling priority `45` (`inversions = 1`)

## Repository Layout

```text
qnx-2026-pulseguard/
├── CMakeLists.txt
├── CODELAB.md
├── LICENSE
├── Makefile
├── QNX_QEMU_RUNBOOK.md
├── README.md
├── docs/
│   └── qnx-pulseguard-deck.pdf
├── include/pulseguard/
│   ├── clock.hpp
│   ├── engine.hpp
│   ├── histogram.hpp
│   ├── resmgr.hpp
│   ├── ring_buffer.hpp
│   ├── telemetry_json.hpp
│   └── watchdog.hpp
├── src/
│   ├── fault_injector.cpp
│   ├── pulseguard_daemon.cpp
│   ├── qnx_ipc_bridge.hpp
│   └── resmgr_pulseguard.cpp
├── tests/
│   └── test_pulseguard.cpp
└── web/
    ├── index.html
    ├── qnx-pulseguard-deck.pdf
    └── sample_telemetry.jsonl
```

## License

MIT. See [LICENSE](LICENSE).
