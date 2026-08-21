#include "shardcache/cache.hpp"

namespace shardcache {

Cache::Cache(std::size_t capacity)
    : capacity_(capacity) {}

void Cache::move_to_front_unsafe(std::list<Node>::iterator it) {
    lru_list_.splice(lru_list_.begin(), lru_list_, it);
}

void Cache::evict_lru_unsafe() {
    if (lru_list_.empty()) return;
    auto last_it = std::prev(lru_list_.end());
    entries_.erase(last_it->key);
    lru_list_.pop_back();
    metrics_.evictions.fetch_add(1, std::memory_order_relaxed);
}

void Cache::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    std::unique_lock lock(mutex_);

    metrics_.sets.fetch_add(1, std::memory_order_relaxed);

    auto now = std::chrono::steady_clock::now();
    std::optional<std::chrono::steady_clock::time_point> expires_at = std::nullopt;
    if (ttl.has_value()) {
        expires_at = now + ttl.value();
    }

    auto it = entries_.find(key);
    if (it != entries_.end()) {
        it->second->entry.value = value;
        it->second->entry.expires_at = expires_at;
        move_to_front_unsafe(it->second);
        return;
    }

    lru_list_.push_front(Node{key, CacheEntry{value, expires_at}});
    entries_[key] = lru_list_.begin();

    if (capacity_ > 0 && entries_.size() > capacity_) {
        evict_lru_unsafe();
    }
}

std::optional<std::string> Cache::get(const std::string& key) {
    std::unique_lock lock(mutex_);

    auto it = entries_.find(key);
    if (it == entries_.end()) {
        metrics_.misses.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.is_expired(now)) {
        lru_list_.erase(it->second);
        entries_.erase(it);
        metrics_.expirations.fetch_add(1, std::memory_order_relaxed);
        metrics_.misses.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }

    move_to_front_unsafe(it->second);
    metrics_.hits.fetch_add(1, std::memory_order_relaxed);
    return it->second->entry.value;
}

bool Cache::remove(const std::string& key) {
    std::unique_lock lock(mutex_);

    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }

    lru_list_.erase(it->second);
    entries_.erase(it);
    metrics_.deletes.fetch_add(1, std::memory_order_relaxed);
    return true;
}

bool Cache::contains(const std::string& key) {
    std::unique_lock lock(mutex_);

    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }

    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.is_expired(now)) {
        lru_list_.erase(it->second);
        entries_.erase(it);
        metrics_.expirations.fetch_add(1, std::memory_order_relaxed);
        return false;
    }

    return true;
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

bool Cache::expire(const std::string& key, std::chrono::seconds ttl) {
    std::unique_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }
    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.is_expired(now)) {
        lru_list_.erase(it->second);
        entries_.erase(it);
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
