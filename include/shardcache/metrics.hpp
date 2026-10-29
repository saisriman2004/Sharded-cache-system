#ifndef SHARDCACHE_METRICS_HPP
#define SHARDCACHE_METRICS_HPP

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <string>

namespace shardcache {

// Counter split across cache lines so threads incrementing it concurrently
// do not bounce one line between cores; reads sum the stripes.
class StripedCounter {
public:
    void add(uint64_t n = 1) {
        stripes_[stripe()].value.fetch_add(n, std::memory_order_relaxed);
    }

    uint64_t value() const {
        uint64_t total = 0;
        for (const auto& s : stripes_) total += s.value.load(std::memory_order_relaxed);
        return total;
    }

private:
    static constexpr std::size_t kStripes = 16;
    struct alignas(64) Stripe {
        std::atomic<uint64_t> value{0};
    };

    static std::size_t stripe() {
        static std::atomic<std::size_t> next{0};
        thread_local std::size_t mine = next.fetch_add(1, std::memory_order_relaxed) % kStripes;
        return mine;
    }

    std::array<Stripe, kStripes> stripes_{};
};

// Latency histogram with fixed exponential buckets (10us .. ~2.6s), in the
// cumulative form Prometheus expects.
class LatencyHistogram {
public:
    static constexpr std::size_t kBuckets = 19;

    // Upper bound of bucket i in seconds: 10us * 2^i.
    static double bound(std::size_t i) { return 10e-6 * static_cast<double>(1u << i); }

    void observe(std::chrono::nanoseconds d) {
        const uint64_t us = static_cast<uint64_t>(d.count() / 1000);
        std::size_t i = 0;
        while (i < kBuckets && us > static_cast<uint64_t>(10u << i)) ++i;
        buckets_[i].add();
        sum_ns_.add(static_cast<uint64_t>(d.count()));
    }

    uint64_t bucket(std::size_t i) const { return buckets_[i].value(); }  // i == kBuckets is +Inf
    uint64_t count() const {
        uint64_t total = 0;
        for (const auto& b : buckets_) total += b.value();
        return total;
    }
    double sum_seconds() const { return static_cast<double>(sum_ns_.value()) / 1e9; }

private:
    std::array<StripedCounter, kBuckets + 1> buckets_{};
    StripedCounter sum_ns_;
};

} // namespace shardcache

#endif // SHARDCACHE_METRICS_HPP
