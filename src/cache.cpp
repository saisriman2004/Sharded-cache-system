#include "shardcache/cache.hpp"

namespace shardcache {

Cache::Cache(std::size_t capacity, std::chrono::milliseconds tombstone_ttl)
    : capacity_(capacity), tombstone_ttl_(tombstone_ttl) {}

namespace {
// Live means visible to reads: present, not expired, not a tombstone.
bool is_live(const CacheEntry& e, std::chrono::steady_clock::time_point now) {
    return !e.tombstone && !e.is_expired(now);
}
} // namespace

void Cache::erase_unsafe(std::list<Node>::iterator it) {
    if (it->entry.tombstone) --tombstones_;
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
        auto& entry = it->second->entry;
        if (entry.tombstone) --tombstones_;
        entry = CacheEntry{value, expires_at};
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
        if (it == entries_.end() || !is_live(it->second->entry, now)) {
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

    bool was_live = is_live(it->second->entry, std::chrono::steady_clock::now());
    erase_unsafe(it->second);
    metrics_.deletes.fetch_add(1, std::memory_order_relaxed);
    return was_live;
}

bool Cache::contains(const std::string& key) {
    std::shared_lock lock(mutex_);

    auto it = entries_.find(key);
    return it != entries_.end() && is_live(it->second->entry, std::chrono::steady_clock::now());
}

bool Cache::apply(const std::string& key, const VersionedValue& v, bool* was_live) {
    const auto now = std::chrono::steady_clock::now();
    std::optional<std::chrono::steady_clock::time_point> expires_at = std::nullopt;
    if (v.tombstone) {
        expires_at = now + tombstone_ttl_;
    } else if (v.ttl.has_value()) {
        expires_at = now + v.ttl.value();
    }

    std::unique_lock lock(mutex_);
    auto it = entries_.find(key);
    if (was_live) *was_live = it != entries_.end() && is_live(it->second->entry, now);
    if (it != entries_.end()) {
        auto& entry = it->second->entry;
        // An expired entry carries no information worth protecting.
        if (!entry.is_expired(now) && !supersedes(v.version, v.tombstone, v.value, entry)) {
            return false;
        }
        if (entry.tombstone) --tombstones_;
        if (v.tombstone) ++tombstones_;
        entry = CacheEntry{v.value, expires_at, v.version, v.tombstone};
        lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
        metrics_.sets.fetch_add(1, std::memory_order_relaxed);
        return true;
    }

    // A tombstone for an absent key is still stored: it may be ordering a
    // delete ahead of an older write that has not arrived yet.
    if (capacity_ > 0 && entries_.size() >= capacity_) {
        evict_lru_unsafe();
    }
    lru_list_.emplace_front(key, CacheEntry{v.value, expires_at, v.version, v.tombstone});
    entries_.emplace(key, lru_list_.begin());
    if (v.tombstone) ++tombstones_;
    metrics_.sets.fetch_add(1, std::memory_order_relaxed);
    return true;
}

std::optional<VersionedValue> Cache::get_versioned(const std::string& key) const {
    const auto now = std::chrono::steady_clock::now();
    std::shared_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end() || it->second->entry.is_expired(now)) {
        metrics_.misses.fetch_add(1, std::memory_order_relaxed);
        return std::nullopt;
    }
    const auto& e = it->second->entry;
    (e.tombstone ? metrics_.misses : metrics_.hits).fetch_add(1, std::memory_order_relaxed);
    VersionedValue out{e.value, e.version, e.tombstone, std::nullopt};
    if (!e.tombstone && e.expires_at.has_value()) {
        out.ttl = std::chrono::duration_cast<std::chrono::milliseconds>(e.expires_at.value() - now);
    }
    return out;
}

void Cache::snapshot(std::vector<std::pair<std::string, VersionedValue>>& out) const {
    const auto now = std::chrono::steady_clock::now();
    std::shared_lock lock(mutex_);
    out.reserve(out.size() + entries_.size());
    for (const auto& node : lru_list_) {
        const auto& e = node.entry;
        if (e.is_expired(now)) continue;
        VersionedValue v{e.value, e.version, e.tombstone, std::nullopt};
        if (!e.tombstone && e.expires_at.has_value()) {
            v.ttl = std::chrono::duration_cast<std::chrono::milliseconds>(e.expires_at.value() - now);
        }
        out.emplace_back(node.key, std::move(v));
    }
}

std::size_t Cache::size() const {
    std::shared_lock lock(mutex_);
    return entries_.size() - tombstones_;
}

std::size_t Cache::capacity() const {
    std::shared_lock lock(mutex_);
    return capacity_;
}

void Cache::clear() {
    std::unique_lock lock(mutex_);
    entries_.clear();
    lru_list_.clear();
    tombstones_ = 0;
}

bool Cache::expire(const std::string& key, std::chrono::milliseconds ttl) {
    std::unique_lock lock(mutex_);
    auto it = entries_.find(key);
    if (it == entries_.end()) {
        return false;
    }
    auto now = std::chrono::steady_clock::now();
    if (it->second->entry.tombstone) {
        return false;
    }
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
    if (!is_live(it->second->entry, now)) {
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
    if (it == entries_.end() || !is_live(it->second->entry, now)) {
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
            if (it->entry.tombstone) --tombstones_;
            entries_.erase(it->key);
            it = lru_list_.erase(it);
            metrics_.expirations.fetch_add(1, std::memory_order_relaxed);
        } else {
            ++it;
        }
    }
}

} // namespace shardcache
