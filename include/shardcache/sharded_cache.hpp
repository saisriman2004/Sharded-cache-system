#ifndef SHARDCACHE_SHARDED_CACHE_HPP
#define SHARDCACHE_SHARDED_CACHE_HPP

#include "shardcache/cache.hpp"
#include "shardcache/single_flight.hpp"
#include "shardcache/ttl_manager.hpp"
#include <vector>
#include <memory>
#include <string>
#include <cstdint>
#include <functional>

namespace shardcache {

class ShardedCache {
public:
    explicit ShardedCache(
        std::size_t total_capacity = 10000,
        std::size_t shard_count = 16,
        std::optional<std::chrono::milliseconds> active_ttl_interval = std::nullopt,
        std::chrono::milliseconds tombstone_ttl = std::chrono::seconds(60)
    );
    ~ShardedCache();

    ShardedCache(const ShardedCache&) = delete;
    ShardedCache& operator=(const ShardedCache&) = delete;

    void set(
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt
    );

    std::optional<std::string> get(const std::string& key);

    bool remove(const std::string& key);

    bool apply(const std::string& key, const VersionedValue& v, bool* was_live = nullptr);
    std::optional<VersionedValue> get_versioned(const std::string& key) const;

    // Visits a snapshot of every shard, one shard at a time, so no lock is
    // held while `fn` runs.
    void for_each_shard_snapshot(
        const std::function<void(std::vector<std::pair<std::string, VersionedValue>>&)>& fn) const;

    bool contains(const std::string& key);

    bool expire(const std::string& key, std::chrono::milliseconds ttl);
    std::optional<std::chrono::seconds> ttl(const std::string& key);
    int64_t pttl(const std::string& key);

    std::string get_or_load(
        const std::string& key,
        std::function<std::string()> loader,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt
    );

    void purge_expired();

    std::size_t size() const;
    std::size_t capacity() const;
    std::size_t shard_count() const { return shards_.size(); }

    void clear();

    struct Totals {
        uint64_t hits{0}, misses{0}, sets{0}, deletes{0}, evictions{0}, expirations{0};
    };
    Totals totals() const;

    uint64_t total_hits() const;
    uint64_t total_misses() const;
    double overall_hit_rate() const;

private:
    std::size_t get_shard_index(const std::string& key) const;

    std::vector<std::unique_ptr<Cache>> shards_;
    SingleFlight single_flight_;
    std::unique_ptr<TTLManager> ttl_manager_;
};

} // namespace shardcache

#endif // SHARDCACHE_SHARDED_CACHE_HPP
