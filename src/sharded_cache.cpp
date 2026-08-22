#include "shardcache/sharded_cache.hpp"
#include "shardcache/consistent_hash.hpp"

namespace shardcache {

ShardedCache::ShardedCache(
    std::size_t total_capacity,
    std::size_t shard_count,
    std::optional<std::chrono::milliseconds> active_ttl_interval
) {
    if (shard_count == 0) shard_count = 1;
    std::size_t capacity_per_shard = total_capacity / shard_count;
    if (capacity_per_shard == 0) capacity_per_shard = 1;

    shards_.reserve(shard_count);
    for (std::size_t i = 0; i < shard_count; ++i) {
        shards_.push_back(std::make_unique<Cache>(capacity_per_shard));
    }

    if (active_ttl_interval.has_value()) {
        ttl_manager_ = std::make_unique<TTLManager>([this]() {
            this->purge_expired();
        }, active_ttl_interval.value());
        ttl_manager_->start();
    }
}

ShardedCache::~ShardedCache() {
    if (ttl_manager_) {
        ttl_manager_->stop();
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

bool ShardedCache::expire(const std::string& key, std::chrono::seconds ttl) {
    std::size_t idx = get_shard_index(key);
    return shards_[idx]->expire(key, ttl);
}

std::optional<std::chrono::seconds> ShardedCache::ttl(const std::string& key) {
    std::size_t idx = get_shard_index(key);
    return shards_[idx]->ttl(key);
}

std::string ShardedCache::get_or_load(
    const std::string& key,
    std::function<std::string()> loader,
    std::optional<std::chrono::seconds> ttl
) {
    auto val = get(key);
    if (val.has_value()) {
        return val.value();
    }

    return single_flight_.do_call(key, [this, &key, &loader, &ttl]() {
        auto cached = get(key);
        if (cached.has_value()) {
            return cached.value();
        }
        std::string loaded = loader();
        set(key, loaded, ttl);
        return loaded;
    });
}

void ShardedCache::purge_expired() {
    for (auto& shard : shards_) {
        shard->purge_expired();
    }
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
