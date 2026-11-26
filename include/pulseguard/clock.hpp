#pragma once

#include <cstdint>
#include <ctime>

#ifdef __QNXNTO__
#include <sys/neutrino.h>
#include <sys/syspage.h>
#elif defined(__APPLE__)
#include <mach/mach_time.h>
#endif

namespace pulseguard {

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

inline uint64_t cycles_per_sec() noexcept {
#ifdef __QNXNTO__
    static const uint64_t cached = SYSPAGE_ENTRY(qtime)->cycles_per_sec;
    return cached > 0 ? cached : 1'000'000'000ULL;
#elif defined(__APPLE__)
    static const mach_timebase_info_data_t tb = [] {
        mach_timebase_info_data_t info{};
        mach_timebase_info(&info);
        if (info.denom == 0) {
            info.numer = 1;
            info.denom = 1;
        }
        return info;
    }();
    return (1'000'000'000ULL * static_cast<uint64_t>(tb.denom)) /
           static_cast<uint64_t>(tb.numer);
#else
    return 1'000'000'000ULL;
#endif
}

inline uint64_t cycles_to_ns(uint64_t delta_cycles) noexcept {
#ifdef __QNXNTO__
    const uint64_t cps = cycles_per_sec();
#ifdef __SIZEOF_INT128__
    return static_cast<uint64_t>((static_cast<__uint128_t>(delta_cycles) * 1'000'000'000ULL) / cps);
#else
    const uint64_t sec = delta_cycles / cps;
    const uint64_t rem = delta_cycles % cps;
    return sec * 1'000'000'000ULL + (rem * 1'000'000'000ULL) / cps;
#endif
#elif defined(__APPLE__)
    static const mach_timebase_info_data_t tb = [] {
        mach_timebase_info_data_t info{};
        mach_timebase_info(&info);
        if (info.denom == 0) {
            info.numer = 1;
            info.denom = 1;
        }
        return info;
    }();
#ifdef __SIZEOF_INT128__
    return static_cast<uint64_t>(
        (static_cast<__uint128_t>(delta_cycles) * tb.numer) / tb.denom);
#else
    return (delta_cycles / tb.denom) * tb.numer +
           ((delta_cycles % tb.denom) * tb.numer) / tb.denom;
#endif
#else
    return delta_cycles;
#endif
}

inline uint64_t now_ns() noexcept {
#ifdef __QNXNTO__
    return cycles_to_ns(ClockCycles());
#elif defined(__APPLE__)
    return cycles_to_ns(mach_continuous_time());
#else
    struct timespec ts{};
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return static_cast<uint64_t>(ts.tv_sec) * 1'000'000'000ULL +
           static_cast<uint64_t>(ts.tv_nsec);
#endif
}

inline uint64_t now_us() noexcept {
    return now_ns() / 1'000ULL;
}

inline constexpr uint32_t pack_ts_us24(uint64_t ts_us) noexcept {
    return static_cast<uint32_t>(ts_us & 0x00FFFFFFULL);
}

inline constexpr uint64_t delta_ts_us24(uint32_t recv_us24, uint32_t send_us24) noexcept {
    return static_cast<uint64_t>((recv_us24 - send_us24) & 0x00FFFFFFU);
}

inline constexpr const char* target_platform_tag() noexcept {
#ifdef __QNXNTO__
    return "QNX8";
#elif defined(__APPLE__)
    return "POSIX-MACOS";
#else
    return "POSIX-LINUX";
#endif
}

}  // namespace pulseguard
