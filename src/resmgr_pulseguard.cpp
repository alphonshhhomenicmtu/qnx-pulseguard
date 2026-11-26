#include "pulseguard/resmgr.hpp"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>

#ifdef __QNXNTO__
#include <sys/dispatch.h>
#include <sys/iofunc.h>
#include <sys/resmgr.h>
#endif

namespace pulseguard {

#ifdef __QNXNTO__
namespace {

struct PulseGuardAttr {
    iofunc_attr_t base;
    ResourceManagerHost* host;
};

int pulseguard_io_read(resmgr_context_t* ctp, io_read_t* msg, RESMGR_OCB_T* ocb) {
    int nonblock = 0;
    const int status = iofunc_read_verify(ctp, msg, ocb, &nonblock);
    if (status != EOK) {
        return status;
    }
    if ((msg->i.xtype & _IO_XTYPE_MASK) != _IO_XTYPE_NONE) {
        return ENOSYS;
    }

    auto* pg_attr = reinterpret_cast<PulseGuardAttr*>(ocb->attr);
    ResourceManagerHost* host = pg_attr->host;
    if (ocb->offset == 0) {
        const std::size_t snapped = host->latch_dispatch_snapshot();
        pg_attr->base.nbytes = static_cast<off_t>(snapped);
    }

    const std::size_t total = host->dispatch_scratch_len();
    const std::size_t off = ocb->offset > 0 ? static_cast<std::size_t>(ocb->offset) : 0;
    if (off >= total) {
        _IO_SET_READ_NBYTES(ctp, 0);
        return _RESMGR_NPARTS(0);
    }

    const std::size_t avail = total - off;
    const std::size_t nbytes = std::min(static_cast<std::size_t>(msg->i.nbytes), avail);
    if (nbytes == 0) {
        _IO_SET_READ_NBYTES(ctp, 0);
        return _RESMGR_NPARTS(0);
    }

    SETIOV(&ctp->iov[0], const_cast<char*>(host->dispatch_scratch() + off), nbytes);
    ocb->offset += static_cast<off_t>(nbytes);
    _IO_SET_READ_NBYTES(ctp, static_cast<int>(nbytes));
    return _RESMGR_NPARTS(1);
}

int pulseguard_io_devctl(resmgr_context_t* ctp, io_devctl_t* msg, RESMGR_OCB_T* ocb) {
    const int status = iofunc_devctl_default(ctp, msg, ocb);
    if (status != _RESMGR_DEFAULT) {
        return status;
    }

    auto* pg_attr = reinterpret_cast<PulseGuardAttr*>(ocb->attr);
    ResourceManagerHost* host = pg_attr->host;

    if (static_cast<unsigned int>(msg->i.dcmd) ==
        static_cast<unsigned int>(DCMD_PULSEGUARD_GET_BINARY)) {
        const TelemetryBinaryFrame frame = host->read_latest_binary();
        void* dptr = _DEVCTL_DATA(msg->i);
        std::memcpy(dptr, &frame, sizeof(frame));
        std::memset(&msg->o, 0, sizeof(msg->o));
        msg->o.ret_val = EOK;
        msg->o.nbytes = static_cast<int32_t>(sizeof(frame));
        return _RESMGR_PTR(ctp, &msg->o, sizeof(msg->o) + sizeof(frame));
    }

    return ENOTTY;
}

}  // namespace
#endif  // __QNXNTO__

bool ResourceManagerHost::start(const ResMgrConfig& cfg) noexcept {
    stop();
    cfg_ = cfg;
    stop_requested_.store(false, std::memory_order_release);

    {
        std::lock_guard<std::mutex> lock(mtx_);
        snapshot_len_ = 0;
        snapshot_buf_[0] = '\0';
        binary_snapshot_ = TelemetryBinaryFrame{};
    }

    if (cfg_.write_host_files && cfg_.host_jsonl_path && cfg_.host_jsonl_path[0] != '\0') {
        const int fd = ::open(cfg_.host_jsonl_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
        if (fd >= 0) {
            ::close(fd);
        }
    }

#ifdef __QNXNTO__
    dispatch_t* dpp = dispatch_create();
    if (!dpp) {
        return false;
    }
    dispatch_ptr_ = dpp;
    running_.store(true, std::memory_order_release);
    if (pthread_create(&dispatch_tid_, nullptr, &ResourceManagerHost::dispatch_thread_entry,
                       this) != 0) {
        running_.store(false, std::memory_order_release);
        dispatch_destroy(dpp);
        dispatch_ptr_ = nullptr;
        return false;
    }
    dispatch_thread_started_ = true;
    return true;
#else
    running_.store(true, std::memory_order_release);
    return true;
#endif
}

void ResourceManagerHost::stop() noexcept {
    stop_requested_.store(true, std::memory_order_release);
    running_.store(false, std::memory_order_release);

#ifdef __QNXNTO__
    if (dispatch_thread_started_) {
        if (dispatch_ptr_) {
            dispatch_unblock(static_cast<dispatch_t*>(dispatch_ptr_));
        }
        pthread_join(dispatch_tid_, nullptr);
        dispatch_thread_started_ = false;
    }
    if (dispatch_ptr_) {
        dispatch_destroy(static_cast<dispatch_t*>(dispatch_ptr_));
        dispatch_ptr_ = nullptr;
    }
    resmgr_id_ = -1;
#endif
}

void ResourceManagerHost::publish_telemetry(const char* jsonl_line, std::size_t jsonl_len,
                                            const TelemetryBinaryFrame& binary_frame,
                                            bool append_stream) noexcept {
    if (!jsonl_line || jsonl_len == 0) {
        return;
    }
    const std::size_t clamped = std::min(jsonl_len, kSnapshotCap - 1);
    {
        std::lock_guard<std::mutex> lock(mtx_);
        std::memcpy(snapshot_buf_, jsonl_line, clamped);
        snapshot_buf_[clamped] = '\0';
        snapshot_len_ = clamped;
        binary_snapshot_ = binary_frame;
    }
    publish_count_.fetch_add(1, std::memory_order_relaxed);

    if (cfg_.write_host_files) {
        if (cfg_.host_snapshot_path && cfg_.host_snapshot_path[0] != '\0') {
            write_atomic_file(cfg_.host_snapshot_path, jsonl_line, clamped);
        }
        if (cfg_.host_binary_path && cfg_.host_binary_path[0] != '\0') {
            write_atomic_file(cfg_.host_binary_path, &binary_frame, sizeof(binary_frame));
        }
        if (append_stream && cfg_.host_jsonl_path && cfg_.host_jsonl_path[0] != '\0') {
            append_file(cfg_.host_jsonl_path, jsonl_line, clamped);
        }
    }
}

void ResourceManagerHost::publish_event(const char* event_jsonl, std::size_t len) noexcept {
    if (!event_jsonl || len == 0) {
        return;
    }
    if (cfg_.write_host_files && cfg_.host_jsonl_path && cfg_.host_jsonl_path[0] != '\0') {
        append_file(cfg_.host_jsonl_path, event_jsonl, len);
    }
}

std::size_t ResourceManagerHost::read_latest_jsonl(char* out, std::size_t cap) const noexcept {
    if (!out || cap == 0) {
        return 0;
    }
    std::lock_guard<std::mutex> lock(mtx_);
    const std::size_t n = std::min(snapshot_len_, cap - 1);
    std::memcpy(out, snapshot_buf_, n);
    out[n] = '\0';
    return n;
}

TelemetryBinaryFrame ResourceManagerHost::read_latest_binary() const noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    return binary_snapshot_;
}

std::size_t ResourceManagerHost::latch_dispatch_snapshot() noexcept {
    std::lock_guard<std::mutex> lock(mtx_);
    const std::size_t n = std::min(snapshot_len_, kSnapshotCap - 1);
    std::memcpy(dispatch_scratch_, snapshot_buf_, n);
    dispatch_scratch_[n] = '\0';
    dispatch_scratch_len_ = n;
    return n;
}

void* ResourceManagerHost::dispatch_thread_entry(void* arg) noexcept {
    auto* self = static_cast<ResourceManagerHost*>(arg);
    self->run_qnx_dispatch_loop();
    return nullptr;
}

void ResourceManagerHost::run_qnx_dispatch_loop() noexcept {
#ifdef __QNXNTO__
    auto* dpp = static_cast<dispatch_t*>(dispatch_ptr_);
    if (!dpp) {
        return;
    }
    resmgr_connect_funcs_t connect_funcs{};
    resmgr_io_funcs_t io_funcs{};
    iofunc_func_init(_RESMGR_CONNECT_NFUNCS, &connect_funcs,
                     _RESMGR_IO_NFUNCS, &io_funcs);
    io_funcs.read = pulseguard_io_read;
    io_funcs.read64 = pulseguard_io_read;
    io_funcs.devctl = pulseguard_io_devctl;

    PulseGuardAttr pg_attr{};
    iofunc_attr_init(&pg_attr.base, S_IFNAM | 0444, nullptr, nullptr);
    pg_attr.host = this;

    resmgr_attr_t resmgr_attr{};
    resmgr_attr.nparts_max = 1;
    resmgr_attr.msg_max_size = 2048;

    const char* mount = (cfg_.device_path && cfg_.device_path[0] != '\0')
                            ? cfg_.device_path
                            : "/dev/pulseguard";
    resmgr_id_ = resmgr_attach(dpp, &resmgr_attr, mount, _FTYPE_ANY, 0,
                               &connect_funcs, &io_funcs, &pg_attr.base);
    if (resmgr_id_ == -1) {
        return;
    }

    dispatch_context_t* ctp = dispatch_context_alloc(dpp);
    if (!ctp) {
        resmgr_detach(dpp, resmgr_id_, _RESMGR_DETACH_ALL);
        resmgr_id_ = -1;
        return;
    }

    while (!stop_requested_.load(std::memory_order_acquire)) {
        dispatch_context_t* next = dispatch_block(ctp);
        if (!next) {
            break;
        }
        ctp = next;
        dispatch_handler(ctp);
    }

    dispatch_context_free(ctp);
    if (resmgr_id_ != -1) {
        resmgr_detach(dpp, resmgr_id_, _RESMGR_DETACH_ALL);
        resmgr_id_ = -1;
    }
#endif
}

void ResourceManagerHost::write_atomic_file(const char* path, const void* data,
                                            std::size_t len) noexcept {
    char tmp_path[512]{};
    const int pn = std::snprintf(tmp_path, sizeof(tmp_path), "%s.tmp", path);
    if (pn <= 0 || static_cast<std::size_t>(pn) >= sizeof(tmp_path)) {
        return;
    }
    const int fd = ::open(tmp_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        return;
    }
    const auto* ptr = static_cast<const uint8_t*>(data);
    std::size_t off = 0;
    while (off < len) {
        const ssize_t n = ::write(fd, ptr + off, len - off);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        ::close(fd);
        ::unlink(tmp_path);
        return;
    }
    ::close(fd);
    (void)::rename(tmp_path, path);
}

void ResourceManagerHost::append_file(const char* path, const void* data,
                                      std::size_t len) noexcept {
    const int fd = ::open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
    if (fd < 0) {
        return;
    }
    const auto* ptr = static_cast<const uint8_t*>(data);
    std::size_t off = 0;
    while (off < len) {
        const ssize_t n = ::write(fd, ptr + off, len - off);
        if (n > 0) {
            off += static_cast<std::size_t>(n);
            continue;
        }
        if (n < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
    ::close(fd);
}

}  // namespace pulseguard
