#pragma once

#include <algorithm>
#include <array>
#include <bit>
#include <cmath>
#include <cstddef>
#include <cstdint>

namespace pulseguard {

// Fixed-memory HDR-style microsecond latency histogram.
// Covers 0 us .. 1,048,575 us (~1.05s) across 512 logarithmic-linear buckets
// (exact 1 us resolution from 0..63 us; <= 3.125% bucket width above 63 us)
// plus 1 overflow bucket. Zero heap allocation, O(1) per sample.
class JitterHistogram {
public:
    static constexpr std::size_t kSubBits = 5;
    static constexpr std::size_t kSubBuckets = 1ULL << kSubBits;  // 32
    static constexpr std::size_t kOctaves = 16;
    static constexpr std::size_t kLinearBuckets = kSubBuckets * kOctaves;  // 512
    static constexpr std::size_t kTotalBuckets = kLinearBuckets + 1;       // 513
    static constexpr uint64_t kMaxLinearUs = (1ULL << 20) - 1ULL;          // 1,048,575 us

    JitterHistogram() noexcept { reset(); }

    void reset() noexcept {
        buckets_.fill(0);
        count_ = 0;
        sum_us_ = 0;
        min_us_ = UINT64_MAX;
        max_us_ = 0;
    }

    static std::size_t bucket_index(uint64_t latency_us) noexcept {
        if (latency_us < kSubBuckets) {
            return static_cast<std::size_t>(latency_us);
        }
        if (latency_us > kMaxLinearUs) {
            return kLinearBuckets;
        }
        const unsigned msb = static_cast<unsigned>(std::bit_width(latency_us) - 1U);
        const unsigned oct = msb - (static_cast<unsigned>(kSubBits) - 1U);
        const unsigned shift = oct - 1U;
        const std::size_t sub = static_cast<std::size_t>((latency_us >> shift) - kSubBuckets);
        return static_cast<std::size_t>(oct) * kSubBuckets + sub;
    }

    void record(uint64_t latency_us) noexcept {
        const std::size_t idx = bucket_index(latency_us);
        ++buckets_[idx];
        ++count_;
        sum_us_ += latency_us;
        if (latency_us < min_us_) {
            min_us_ = latency_us;
        }
        if (latency_us > max_us_) {
            max_us_ = latency_us;
        }
    }

    void merge(const JitterHistogram& other) noexcept {
        if (other.count_ == 0) {
            return;
        }
        for (std::size_t i = 0; i < kTotalBuckets; ++i) {
            buckets_[i] += other.buckets_[i];
        }
        count_ += other.count_;
        sum_us_ += other.sum_us_;
        if (other.min_us_ < min_us_) {
            min_us_ = other.min_us_;
        }
        if (other.max_us_ > max_us_) {
            max_us_ = other.max_us_;
        }
    }

    uint64_t percentile(double pct) const noexcept {
        if (count_ == 0) {
            return 0;
        }
        if (pct <= 0.0) {
            return min_us_;
        }
        if (pct >= 100.0) {
            return max_us_;
        }
        const double raw_rank = std::ceil((pct / 100.0) * static_cast<double>(count_));
        const uint64_t target = std::clamp<uint64_t>(
            static_cast<uint64_t>(raw_rank), 1ULL, count_);

        uint64_t cumulative = 0;
        for (std::size_t i = 0; i < kTotalBuckets; ++i) {
            cumulative += buckets_[i];
            if (cumulative >= target) {
                const uint64_t rep = bucket_representative(i);
                return std::clamp(rep, min_us_, max_us_);
            }
        }
        return max_us_;
    }

    uint64_t p50_us() const noexcept { return percentile(50.0); }
    uint64_t p95_us() const noexcept { return percentile(95.0); }
    uint64_t p99_us() const noexcept { return percentile(99.0); }
    uint64_t p999_us() const noexcept { return percentile(99.9); }

    uint64_t min_us() const noexcept { return count_ == 0 ? 0 : min_us_; }
    uint64_t max_us() const noexcept { return count_ == 0 ? 0 : max_us_; }

    double mean_us() const noexcept {
        if (count_ == 0) {
            return 0.0;
        }
        return static_cast<double>(sum_us_) / static_cast<double>(count_);
    }

    uint64_t count() const noexcept { return count_; }
    const std::array<uint64_t, kTotalBuckets>& buckets() const noexcept { return buckets_; }

private:
    uint64_t bucket_representative(std::size_t idx) const noexcept {
        if (idx < kSubBuckets) {
            return static_cast<uint64_t>(idx);
        }
        if (idx >= kLinearBuckets) {
            return max_us_;
        }
        const std::size_t oct = idx / kSubBuckets;
        const std::size_t sub = idx % kSubBuckets;
        const unsigned shift = static_cast<unsigned>(oct - 1U);
        const uint64_t low = (static_cast<uint64_t>(kSubBuckets) + sub) << shift;
        const uint64_t span = 1ULL << shift;
        return low + ((span - 1ULL) >> 1U);
    }

    std::array<uint64_t, kTotalBuckets> buckets_{};
    uint64_t count_{0};
    uint64_t sum_us_{0};
    uint64_t min_us_{UINT64_MAX};
    uint64_t max_us_{0};
};

}  // namespace pulseguard
