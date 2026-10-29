#include "shardcache/cache.hpp"

namespace shardcache {

Cache::Cache(std::size_t capacity)
    : capacity_(capacity) {}

void Cache::erase_unsafe(std::list<Node>::iterator it) {
    entries_.erase(it->key);
    lru_list_.erase(it);
}

void Cache::evict_lru_unsafe() {
    if (lru_list_.empty()) return;
    erase_unsafe(std::prev(lru_list_.end()));
    metrics_.evictions.fetch_add(1, std::memory_order_relaxed);
}

void Cache::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::milliseconds> ttl
) {
    std::optional<std::chrono::steady_clock::time_point> expires_at = std::nullopt;
    if (ttl.has_value()) {
        expires_at = std::chrono::steady_clock::now() + ttl.value();
    }

    std::unique_lock lock(mutex_);
    metrics_.sets.fetch_add(1, std::memory_order_relaxed);

    auto it = entries_.find(key);
    if (it != entries_.end()) {
        it->second->entry.value = value;
        it->second->entry.expires_at = expires_at;
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        return;
    }

    if (capacity_ > 0 && entries_.size() >= capacity_) {
        evict_lru_unsafe();
    }

    lru_list_.emplace_front(key, CacheEntry{value, expires_at});
    entries_.emplace(key, lru_list_.begin());
}

std::optional<std::string> Cache::get(const std::string& key) {
    const auto now = std::chrono::steady_clock::now();
    const int64_t now_ticks = now.time_since_epoch().count();
    std::optional<std::string> value;
    {
        std::shared_lock lock(mutex_);

        auto it = entries_.find(key);
        if (it == entries_.end() || it->second->entry.is_expired(now)) {
            // Expired entries are reclaimed by writers or the active TTL sweep,
            // which keeps the read path free of exclusive locking.
            metrics_.misses.fetch_add(1, std::memory_order_relaxed);
            return std::nullopt;
        }

        metrics_.hits.fetch_add(1, std::memory_order_relaxed);
        value = it->second->entry.value;

        const int64_t last = it->second->last_promoted.load(std::memory_order_relaxed);
        const auto interval = std::chrono::duration_cast<std::chrono::steady_clock::duration>(kPromoteInterval).count();
        if (last != 0 && now_ticks - last < interval) {
            return value;
        }
    }

    // Slow path: promote under the exclusive lock. The entry may have been
    // removed or replaced since the shared lock was released, so look it up again.
    std::unique_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it != entries_.end()) {
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        it->second->last_promoted.store(now_ticks, std::memory_order_relaxed);
    }
    return value;
}

bool Cache::remove(const std::string& key) {
    std::unique_lock lock(mutex_);

    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }

    bool was_live = !it->second->entry.is_expired();
    erase_unsafe(it->second);
    metrics_.deletes.fetch_add(1, std::memory_order_relaxed);
    return was_live;
}

bool Cache::contains(const std::string& key) {
    std::shared_lock lock(mutex_);

    auto it = entries_.find(key);
    return it != entries_.end() && !it->second->entry.is_expired();
}

std::size_t Cache::size() const {
    std::shared_lock lock(mutex_);
    return entries_.size();
}

std::size_t Cache::capacity() const {
    std::shared_lock lock(mutex_);
    return capacity_;
}

void Cache::clear() {
    std::unique_lock lock(mutex_);
    entries_.clear();
    lru_list_.clear();
}

bool Cache::expire(const std::string& key, std::chrono::milliseconds ttl) {
    std::unique_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }
    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.is_expired(now)) {
        erase_unsafe(it->second);
        metrics_.expirations.fetch_add(1, std::memory_order_relaxed);
        return false;
    }
    it->second->entry.expires_at = now + ttl;
    return true;
}

std::optional<std::chrono::seconds> Cache::ttl(const std::string& key) const {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return std::nullopt;
    }
    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.is_expired(now)) {
        return std::nullopt;
    }
    if (!it->second->entry.expires_at.has_value()) {
        return std::nullopt;
    }
    auto diff = std::chrono::duration_cast<std::chrono::seconds>(it->second->entry.expires_at.value() - now);
    return diff.count() >= 0 ? diff : std::chrono::seconds(0);
}

int64_t Cache::pttl(const std::string& key) const {
    std::shared_lock lock(mutex_);
    auto it = entries_.find(key);
    auto now = std::chrono::steady_clock::now();
    if (it == entries_.end() || it->second->entry.is_expired(now)) {
        return -2;
    }
    if (!it->second->entry.expires_at.has_value()) {
        return -1;
    }
    return std::chrono::duration_cast<std::chrono::milliseconds>(it->second->entry.expires_at.value() - now).count();
}

void Cache::purge_expired() {
    std::unique_lock lock(mutex_);
    auto now = std::chrono::steady_clock::now();
    for (auto it = lru_list_.begin(); it != lru_list_.end(); ) {
        if (it->entry.is_expired(now)) {
            entries_.erase(it->key);
            it = lru_list_.erase(it);
            metrics_.expirations.fetch_add(1, std::memory_order_relaxed);
        } else {
            ++it;
        }
    }
}

} // namespace shardcache
