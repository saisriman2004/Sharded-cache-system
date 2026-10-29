#ifndef SHARDCACHE_CACHE_ENTRY_HPP
#define SHARDCACHE_CACHE_ENTRY_HPP

#include <string>
#include <optional>
#include <chrono>
#include <cstdint>

namespace shardcache {

struct CacheEntry {
    std::string value;
    std::optional<std::chrono::steady_clock::time_point> expires_at{std::nullopt};
    // Hybrid-logical-clock version assigned by the write's coordinator; 0 for
    // unversioned local writes.
    uint64_t version{0};
    // A tombstone records a delete so an older replicated write arriving
    // late cannot resurrect the key. Tombstones are invisible to reads.
    bool tombstone{false};

    [[nodiscard]] bool is_expired(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const {
        if (!expires_at.has_value()) {
            return false;
        }
        return now >= expires_at.value();
    }
};

// A value together with the metadata replicas need to order writes.
struct VersionedValue {
    std::string value;
    uint64_t version{0};
    bool tombstone{false};
    // Remaining time to live; nullopt means no expiry.
    std::optional<std::chrono::milliseconds> ttl{std::nullopt};
};

// Last-writer-wins order: higher version wins; on a tie deletes win, then
// the larger value, so every replica resolves the same way.
inline bool supersedes(uint64_t version, bool tombstone, const std::string& value, const CacheEntry& current) {
    if (version != current.version) return version > current.version;
    if (tombstone != current.tombstone) return tombstone;
    return value > current.value;
}

} // namespace shardcache

#endif // SHARDCACHE_CACHE_ENTRY_HPP
