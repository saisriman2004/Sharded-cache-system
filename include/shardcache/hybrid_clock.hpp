#ifndef SHARDCACHE_HYBRID_CLOCK_HPP
#define SHARDCACHE_HYBRID_CLOCK_HPP

#include <atomic>
#include <chrono>
#include <cstdint>

namespace shardcache {

// Hybrid logical clock (Kulkarni et al.): timestamps track wall-clock time
// but never go backwards and always move past any timestamp observed from a
// peer, so a write is ordered after every write its coordinator has seen.
// Layout: high 48 bits = Unix milliseconds, low 16 bits = logical counter.
class HybridClock {
public:
    static constexpr int kLogicalBits = 16;

    // Timestamp for a new local event (a write this node coordinates).
    uint64_t now() {
        const uint64_t physical = physical_now() << kLogicalBits;
        uint64_t last = last_.load(std::memory_order_relaxed);
        uint64_t next;
        do {
            next = physical > last ? physical : last + 1;
        } while (!last_.compare_exchange_weak(last, next, std::memory_order_relaxed));
        return next;
    }

    // Merges a timestamp received from another node.
    void observe(uint64_t remote) {
        uint64_t last = last_.load(std::memory_order_relaxed);
        while (remote > last && !last_.compare_exchange_weak(last, remote, std::memory_order_relaxed)) {
        }
    }

    static uint64_t physical_ms(uint64_t timestamp) { return timestamp >> kLogicalBits; }

private:
    static uint64_t physical_now() {
        return static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count());
    }

    std::atomic<uint64_t> last_{0};
};

} // namespace shardcache

#endif // SHARDCACHE_HYBRID_CLOCK_HPP
