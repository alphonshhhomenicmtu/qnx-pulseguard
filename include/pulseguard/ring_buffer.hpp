#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <type_traits>

namespace pulseguard {

// Wait-free single-producer single-consumer (SPSC) lock-free ring buffer.
// Performs zero dynamic allocations on the hot path and pads producer/consumer
// cursors onto distinct 64-byte cache lines to avoid false sharing.
template <typename T, std::size_t Capacity>
class SpscRingBuffer {
    static_assert(Capacity >= 2, "Capacity must be at least 2");
    static_assert((Capacity & (Capacity - 1)) == 0, "Capacity must be a power of 2");
    static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable");

public:
    static constexpr std::size_t kMask = Capacity - 1;

    SpscRingBuffer() noexcept = default;
    SpscRingBuffer(const SpscRingBuffer&) = delete;
    SpscRingBuffer& operator=(const SpscRingBuffer&) = delete;

    bool try_push(const T& item) noexcept {
        const std::size_t h = head_.load(std::memory_order_relaxed);
        if ((h - tail_cache_) >= Capacity) {
            tail_cache_ = tail_.load(std::memory_order_acquire);
            if ((h - tail_cache_) >= Capacity) {
                dropped_.fetch_add(1, std::memory_order_relaxed);
                return false;
            }
        }
        buffer_[h & kMask] = item;
        head_.store(h + 1, std::memory_order_release);
        return true;
    }

    bool try_pop(T& out) noexcept {
        const std::size_t t = tail_.load(std::memory_order_relaxed);
        if (t == head_cache_) {
            head_cache_ = head_.load(std::memory_order_acquire);
            if (t == head_cache_) {
                return false;
            }
        }
        out = buffer_[t & kMask];
        tail_.store(t + 1, std::memory_order_release);
        return true;
    }

    template <typename Fn>
    std::size_t drain(Fn&& fn, std::size_t max_items = Capacity) noexcept {
        std::size_t drained = 0;
        T item{};
        while (drained < max_items && try_pop(item)) {
            fn(item);
            ++drained;
        }
        return drained;
    }

    std::size_t size_approx() const noexcept {
        const std::size_t h = head_.load(std::memory_order_acquire);
        const std::size_t t = tail_.load(std::memory_order_acquire);
        return h - t;
    }

    bool empty() const noexcept {
        return head_.load(std::memory_order_acquire) ==
               tail_.load(std::memory_order_acquire);
    }

    constexpr std::size_t capacity() const noexcept { return Capacity; }

    uint64_t dropped() const noexcept {
        return dropped_.load(std::memory_order_relaxed);
    }

    // Only safe when both producer and consumer threads are quiescent.
    void reset() noexcept {
        head_.store(0, std::memory_order_relaxed);
        tail_.store(0, std::memory_order_relaxed);
        tail_cache_ = 0;
        head_cache_ = 0;
        dropped_.store(0, std::memory_order_relaxed);
    }

private:
    alignas(64) std::atomic<std::size_t> head_{0};
    std::size_t tail_cache_{0};

    alignas(64) std::atomic<std::size_t> tail_{0};
    std::size_t head_cache_{0};

    alignas(64) std::atomic<uint64_t> dropped_{0};
    alignas(64) std::array<T, Capacity> buffer_{};
};

}  // namespace pulseguard
