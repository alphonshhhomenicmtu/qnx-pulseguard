# QNX SDP 8.0 & QEMU Runbook (`x86_64` / `aarch64`)

This runbook documents how to cross-compile `QNX-PulseGuard` with **QNX SDP 8.0** (free non-commercial license via **QNX Everywhere**), boot a **QNX 8.0 QEMU VM**, deploy the binaries, read live telemetry from `/dev/pulseguard`, and verify sub-10ms fault recovery without physical hardware.

---

## 1. Prerequisites

1. Register and install **QNX Software Center** + **QNX SDP 8.0** (QNX Everywhere).
2. Install QEMU on your host (`brew install qemu` on macOS, `sudo apt install qemu-system-x86 qemu-system-arm` on Ubuntu/Debian).
3. Source the QNX SDP 8.0 environment in your terminal:

```sh
source ~/qnx800/qnxsdp-env.sh
q++ --version
```

---

## 2. Cross-Compile `QNX-PulseGuard`

Choose either `x86_64` or `aarch64`:

```sh
# Option A: x86_64 QEMU target
make clean
make qnx-x86_64

# Option B: aarch64 QEMU target
make clean
make qnx-aarch64
```

Verify the generated ELF target binaries:

```sh
file build/pulseguard_daemon build/fault_injector build/test_pulseguard
```

---

## 3. Create & Boot a QNX 8.0 QEMU Image

If you do not already have a running QNX 8.0 QEMU image, generate one with `mkqnximage` inside a temporary VM directory:

```sh
mkdir -p ~/qnx-vm && cd ~/qnx-vm
mkqnximage --type=qemu --arch=x86_64 --build
```

Boot the generated QEMU image (forwarding host port `2222` to guest SSH port `22`):

```sh
qemu-system-x86_64 \
  -smp 2 -m 1024M \
  -drive file=output/disk-qemu.vmdk,if=ide,format=vmdk \
  -netdev user,id=net0,hostfwd=tcp::2222-:22 \
  -device e1000,netdev=net0 \
  -nographic
```

Wait for the `qnxpi` / `root` login prompt (default QNX `mkqnximage` credentials: `root` / `root` or check your local image prompt).

---

## 4. Deploy & Run Inside QNX 8.0 QEMU

From your host terminal inside `qnx-2026-pulseguard/`:

```sh
scp -P 2222 build/pulseguard_daemon build/fault_injector build/test_pulseguard root@localhost:/tmp/
```

Inside the QNX 8.0 QEMU shell:

```sh
cd /tmp

# 1. Run full unit & E2E verification natively on QNX Neutrino 8.0
./test_pulseguard

# 2. Launch pulseguard_daemon in background registering /dev/pulseguard
./pulseguard_daemon --duration-ms 15000 --device-path /dev/pulseguard --jsonl-out /tmp/pg.jsonl &

# 3. Read live JSONL telemetry directly from the QNX Resource Manager node
cat /dev/pulseguard

# 4. Run fault injector to prove sub-10ms recovery under QNX SCHED_FIFO
./fault_injector --mode all --stall-ms 14 --jsonl-out /tmp/pg_faults.jsonl
```

Copy `/tmp/pg_faults.jsonl` back to your host to inspect in `web/index.html`:

```sh
scp -P 2222 root@localhost:/tmp/pg_faults.jsonl ./web/qnx_qemu_telemetry.jsonl
```

---

## 5. Binary `devctl()` Inspection (QNX Client Snippet)

Any QNX C/C++ client can query the CRC32-verified 128-byte binary snapshot from `/dev/pulseguard` via `devctl()`:

```cpp
#include "pulseguard/resmgr.hpp"
#include <devctl.h>
#include <fcntl.h>
#include <unistd.h>

int fd = open("/dev/pulseguard", O_RDONLY);
pulseguard::TelemetryBinaryFrame frame{};
if (fd >= 0) {
    devctl(fd, DCMD_PULSEGUARD_GET_BINARY, &frame, sizeof(frame), nullptr);
    close(fd);
}
```
