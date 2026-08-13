#include "shardcache/sharded_cache.hpp"
#include "shardcache/consistent_hash.hpp"

namespace shardcache {

ShardedCache::ShardedCache(std::size_t total_capacity, std::size_t shard_count) {
    if (shard_count == 0) shard_count = 1;
    std::size_t capacity_per_shard = total_capacity / shard_count;
    if (capacity_per_shard == 0) capacity_per_shard = 1;

    shards_.reserve(shard_count);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Cache>(capacity_per_shard));
    }
}

std::size_t ShardedCache::get_shard_index(const std::string& key) const {
    return ConsistentHashRing::hash_fn(key) % shards_.size();
}

void ShardedCache::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    std::size_t idx = get_shard_index(key);
    shards_[idx]->set(key, value, ttl);
}

std::optional<std::string> ShardedCache::get(const std::string& key) {
    std::size_t idx = get_shard_index(key);
    return shards_[idx]->get(key);
}

bool ShardedCache::remove(const std::string& key) {
    std::size_t idx = get_shard_index(key);
    return shards_[idx]->remove(key);
}

bool ShardedCache::contains(const std::string& key) {
    std::size_t idx = get_shard_index(key);
    return shards_[idx]->contains(key);
}

std::size_t ShardedCache::size() const {
    std::size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->size();
    }
    return total;
}

std::size_t ShardedCache::capacity() const {
    std::size_t total = 0;
    for (const auto& shard : shards_) {
        total += shard->capacity();
    }
    return total;
}

void ShardedCache::clear() {
    for (auto& shard : shards_) {
        shard->clear();
    }
}

uint64_t ShardedCache::total_hits() const {
    uint64_t hits = 0;
    for (const auto& shard : shards_) {
        hits += shard->metrics().hits.load();
    }
    return hits;
}

uint64_t ShardedCache::total_misses() const {
    uint64_t misses = 0;
    for (const auto& shard : shards_) {
        misses += shard->metrics().misses.load();
    }
    return misses;
}

double ShardedCache::overall_hit_rate() const {
    uint64_t h = total_hits();
    uint64_t m = total_misses();
    uint64_t total = h + m;
    return total == 0 ? 0.0 : (static_cast<double>(h) / total) * 100.0;
}

} // namespace shardcache
