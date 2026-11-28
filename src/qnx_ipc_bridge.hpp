#pragma once

#include "pulseguard/clock.hpp"

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sched.h>
#include <unistd.h>

#ifdef __QNXNTO__
#include <sys/netmgr.h>
#include <sys/neutrino.h>
#ifndef ND_LOCAL_NODE
#define ND_LOCAL_NODE 0
#endif
#endif

namespace pulseguard {

enum class PulseKind : uint8_t {
    Heartbeat = 0,
    RestartHandshake = 1,
    InversionAlert = 2,
    Shutdown = 3,
};

struct PulseFrame {
    uint8_t worker_id{0};
    PulseKind kind{PulseKind::Heartbeat};
    uint16_t epoch{1};
    uint32_t send_ts_us24{0};
    int32_t priority{20};
    uint32_t seq{0};
};

// Packs worker_id (4 bits), epoch (8 bits), and a 20-bit microsecond timestamp
// into QNX's native 32-bit pulse value. Eight epoch bits keep epoch 17 distinct
// from stale epoch 1 (a 4-bit nibble aliases 17 to 1).
inline constexpr int32_t pack_pulse_value(uint8_t worker_id, uint16_t epoch,
                                          uint32_t ts_us24) noexcept {
    const uint32_t packed =
        ((static_cast<uint32_t>(worker_id) & 0x0FU) << 28U) |
        ((static_cast<uint32_t>(epoch) & 0xFFU) << 20U) |
        (ts_us24 & kPulseTsMask);
    return static_cast<int32_t>(packed);
}

inline constexpr void unpack_pulse_value(int32_t raw_value, uint8_t& worker_id,
                                         uint16_t& epoch,
                                         uint32_t& ts_us24) noexcept {
    const uint32_t u = static_cast<uint32_t>(raw_value);
    worker_id = static_cast<uint8_t>((u >> 28U) & 0x0FU);
    epoch = static_cast<uint16_t>((u >> 20U) & 0xFFU);
    ts_us24 = u & kPulseTsMask;
}

struct SchedStatus {
    bool applied{false};
    bool permission_limited{false};
    int policy{SCHED_OTHER};
    int priority{0};
    int err{0};
};

inline SchedStatus configure_realtime_thread(pthread_attr_t* attr, int prio) noexcept {
    SchedStatus st{};
    if (!attr) {
        st.err = EINVAL;
        return st;
    }
    const int min_p = sched_get_priority_min(SCHED_FIFO);
    const int max_p = sched_get_priority_max(SCHED_FIFO);
    int clamped = prio;
    if (min_p >= 0 && max_p >= min_p) {
        if (clamped < min_p) {
            clamped = min_p;
        }
        if (clamped > max_p) {
            clamped = max_p;
        }
    }
    struct sched_param sp{};
    sp.sched_priority = clamped;
    int rc = pthread_attr_setschedpolicy(attr, SCHED_FIFO);
    if (rc == 0) {
        rc = pthread_attr_setschedparam(attr, &sp);
    }
    if (rc == 0) {
        rc = pthread_attr_setinheritsched(attr, PTHREAD_EXPLICIT_SCHED);
    }
    st.policy = SCHED_FIFO;
    st.priority = clamped;
    st.err = rc;
    st.applied = (rc == 0);
    st.permission_limited = (rc == EPERM);
    return st;
}

inline SchedStatus escalate_current_thread_priority(int prio) noexcept {
    SchedStatus st{};
    const int min_p = sched_get_priority_min(SCHED_FIFO);
    const int max_p = sched_get_priority_max(SCHED_FIFO);
    int clamped = prio;
    if (min_p >= 0 && max_p >= min_p) {
        if (clamped < min_p) {
            clamped = min_p;
        }
        if (clamped > max_p) {
            clamped = max_p;
        }
    }
    struct sched_param sp{};
    sp.sched_priority = clamped;
    const int rc = pthread_setschedparam(pthread_self(), SCHED_FIFO, &sp);
    st.policy = SCHED_FIFO;
    st.priority = clamped;
    st.err = rc;
    st.applied = (rc == 0);
    st.permission_limited = (rc == EPERM);
    return st;
}

struct PiMutexStatus {
    bool initialized{false};
    bool protocol_applied{false};
    int err{0};
};

inline PiMutexStatus init_pi_mutex(pthread_mutex_t* mtx, int ceiling_prio,
                                   bool use_priority_protect = false) noexcept {
    PiMutexStatus st{};
    if (!mtx) {
        st.err = EINVAL;
        return st;
    }
    pthread_mutexattr_t attr;
    if (pthread_mutexattr_init(&attr) != 0) {
        st.err = errno;
        return st;
    }
    int proto_rc = ENOTSUP;
    if (use_priority_protect) {
#ifdef PTHREAD_PRIO_PROTECT
        proto_rc = pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_PROTECT);
        if (proto_rc == 0) {
            (void)pthread_mutexattr_setprioceiling(&attr, ceiling_prio);
        }
#else
        (void)ceiling_prio;
#endif
    } else {
#ifdef PTHREAD_PRIO_INHERIT
        proto_rc = pthread_mutexattr_setprotocol(&attr, PTHREAD_PRIO_INHERIT);
#endif
    }
    int init_rc = pthread_mutex_init(mtx, &attr);
    if (init_rc == 0) {
        st.initialized = true;
        st.protocol_applied = (proto_rc == 0);
    } else {
        st.err = init_rc;
        if (pthread_mutex_init(mtx, nullptr) == 0) {
            st.initialized = true;
            st.protocol_applied = false;
        }
    }
    pthread_mutexattr_destroy(&attr);
    return st;
}

// Dual-target IPC transport.
// On QNX Neutrino 8.0 (__QNXNTO__), uses native ChannelCreate_r(), ConnectAttach_r(),
// MsgSendPulse_r(), TimerTimeout_r(), and MsgReceivePulse_r().
// On host POSIX (macOS/Linux), uses an atomic-write non-buffered pipe with packed
// 8-byte pulse frames and poll() timeouts to preserve real kernel IPC context switches.
class PulseTransport {
public:
    enum class RecvStatus : uint8_t {
        Received = 0,
        Timeout = 1,
        SystemPulse = 2,
        Error = 3,
    };

    PulseTransport() noexcept = default;
    ~PulseTransport() noexcept { close(); }

    PulseTransport(const PulseTransport&) = delete;
    PulseTransport& operator=(const PulseTransport&) = delete;

    bool open_server() noexcept {
        close();
#ifdef __QNXNTO__
        const int ch = ChannelCreate_r(_NTO_CHF_DISCONNECT | _NTO_CHF_COID_DISCONNECT |
                                       _NTO_CHF_UNBLOCK);
        if (ch < 0) {
            last_errno_ = -ch;
            return false;
        }
        chid_ = ch;
        const int co = ConnectAttach_r(ND_LOCAL_NODE, 0, chid_, _NTO_SIDE_CHANNEL, 0);
        if (co < 0) {
            last_errno_ = -co;
            ChannelDestroy_r(chid_);
            chid_ = -1;
            return false;
        }
        coid_ = co;
        owns_channel_ = true;
        return true;
#else
        if (::pipe(pipe_fds_) != 0) {
            last_errno_ = errno;
            pipe_fds_[0] = -1;
            pipe_fds_[1] = -1;
            return false;
        }
        (void)::fcntl(pipe_fds_[0], F_SETFD, FD_CLOEXEC);
        (void)::fcntl(pipe_fds_[1], F_SETFD, FD_CLOEXEC);
        owns_channel_ = true;
        return true;
#endif
    }

    bool connect_client(int server_pid, int server_chid) noexcept {
#ifdef __QNXNTO__
        const int co = ConnectAttach_r(ND_LOCAL_NODE, static_cast<pid_t>(server_pid),
                                       server_chid, _NTO_SIDE_CHANNEL, 0);
        if (co < 0) {
            last_errno_ = -co;
            return false;
        }
        coid_ = co;
        owns_channel_ = false;
        return true;
#else
        (void)server_pid;
        (void)server_chid;
        return pipe_fds_[1] >= 0;
#endif
    }

    bool send_pulse(const PulseFrame& frame) noexcept {
        const int32_t packed = pack_pulse_value(frame.worker_id, frame.epoch, frame.send_ts_us24);
#ifdef __QNXNTO__
        if (coid_ < 0) {
            last_errno_ = EBADF;
            return false;
        }
        const int code = _PULSE_CODE_MINAVAIL + static_cast<int>(frame.kind);
        const int rc = MsgSendPulse_r(coid_, frame.priority, code, packed);
        if (rc < 0) {
            last_errno_ = -rc;
            return false;
        }
        return true;
#else
        if (pipe_fds_[1] < 0) {
            last_errno_ = EBADF;
            return false;
        }
        WirePulse wire{};
        wire.code = static_cast<int8_t>(frame.kind);
        wire.packed_value = packed;
        wire.priority = static_cast<int16_t>(frame.priority);
        wire.seq = frame.seq;
        const auto* ptr = reinterpret_cast<const uint8_t*>(&wire);
        std::size_t sent = 0;
        while (sent < sizeof(wire)) {
            const ssize_t n = ::write(pipe_fds_[1], ptr + sent, sizeof(wire) - sent);
            if (n > 0) {
                sent += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            last_errno_ = errno;
            return false;
        }
        return true;
#endif
    }

    RecvStatus receive_pulse(PulseFrame& out, int timeout_ms) noexcept {
#ifdef __QNXNTO__
        if (chid_ < 0) {
            last_errno_ = EBADF;
            return RecvStatus::Error;
        }
        if (timeout_ms >= 0) {
            const uint64_t timeout_ns = static_cast<uint64_t>(timeout_ms) * 1'000'000ULL;
            (void)TimerTimeout_r(CLOCK_MONOTONIC, _NTO_TIMEOUT_RECEIVE, nullptr,
                                 &timeout_ns, nullptr);
        }
        struct _pulse pulse{};
        const int rc = MsgReceivePulse_r(chid_, &pulse, sizeof(pulse), nullptr);
        if (rc < 0) {
            const int err = -rc;
            if (err == ETIMEDOUT) {
                return RecvStatus::Timeout;
            }
            if (err == EINTR) {
                return RecvStatus::Timeout;
            }
            last_errno_ = err;
            return RecvStatus::Error;
        }
        if (pulse.code < 0) {
            return RecvStatus::SystemPulse;
        }
        const int kind_idx = static_cast<int>(pulse.code) - _PULSE_CODE_MINAVAIL;
        out.kind = (kind_idx >= 0 && kind_idx <= 3)
                       ? static_cast<PulseKind>(kind_idx)
                       : PulseKind::Heartbeat;
        unpack_pulse_value(pulse.value.sival_int, out.worker_id, out.epoch,
                           out.send_ts_us24);
        out.priority = 0;
        out.seq = 0;
        return RecvStatus::Received;
#else
        if (pipe_fds_[0] < 0) {
            last_errno_ = EBADF;
            return RecvStatus::Error;
        }
        struct pollfd pfd{};
        pfd.fd = pipe_fds_[0];
        pfd.events = POLLIN;
        for (;;) {
            const int prc = ::poll(&pfd, 1, timeout_ms);
            if (prc == 0) {
                return RecvStatus::Timeout;
            }
            if (prc < 0) {
                if (errno == EINTR) {
                    continue;
                }
                last_errno_ = errno;
                return RecvStatus::Error;
            }
            break;
        }
        WirePulse wire{};
        auto* ptr = reinterpret_cast<uint8_t*>(&wire);
        std::size_t got = 0;
        while (got < sizeof(wire)) {
            const ssize_t n = ::read(pipe_fds_[0], ptr + got, sizeof(wire) - got);
            if (n > 0) {
                got += static_cast<std::size_t>(n);
                continue;
            }
            if (n < 0 && errno == EINTR) {
                continue;
            }
            last_errno_ = (n == 0) ? EPIPE : errno;
            return RecvStatus::Error;
        }
        out.kind = (wire.code >= 0 && wire.code <= 3)
                       ? static_cast<PulseKind>(wire.code)
                       : PulseKind::Heartbeat;
        unpack_pulse_value(wire.packed_value, out.worker_id, out.epoch, out.send_ts_us24);
        out.priority = wire.priority;
        out.seq = wire.seq;
        return RecvStatus::Received;
#endif
    }

    void close() noexcept {
#ifdef __QNXNTO__
        if (coid_ >= 0) {
            ConnectDetach_r(coid_);
            coid_ = -1;
        }
        if (owns_channel_ && chid_ >= 0) {
            ChannelDestroy_r(chid_);
            chid_ = -1;
        }
        owns_channel_ = false;
#else
        if (pipe_fds_[1] >= 0) {
            ::close(pipe_fds_[1]);
            pipe_fds_[1] = -1;
        }
        if (pipe_fds_[0] >= 0) {
            ::close(pipe_fds_[0]);
            pipe_fds_[0] = -1;
        }
        owns_channel_ = false;
#endif
    }

    int channel_id() const noexcept {
#ifdef __QNXNTO__
        return chid_;
#else
        return pipe_fds_[0];
#endif
    }

    int connection_id() const noexcept {
#ifdef __QNXNTO__
        return coid_;
#else
        return pipe_fds_[1];
#endif
    }

    int last_errno() const noexcept { return last_errno_; }

private:
#ifndef __QNXNTO__
    struct WirePulse {
        int8_t code{0};
        uint8_t reserved{0};
        int16_t priority{0};
        int32_t packed_value{0};
        uint32_t seq{0};
    };
    static_assert(sizeof(WirePulse) == 12, "WirePulse must stay compact");
    int pipe_fds_[2]{-1, -1};
#else
    int chid_{-1};
    int coid_{-1};
#endif
    bool owns_channel_{false};
    int last_errno_{0};
};

}  // namespace pulseguard
