#ifndef SHARDCACHE_CACHE_HPP
#define SHARDCACHE_CACHE_HPP

#include "shardcache/cache_entry.hpp"

#include <string>
#include <optional>
#include <unordered_map>
#include <list>
#include <shared_mutex>
#include <mutex>
#include <atomic>
#include <chrono>

namespace shardcache {

struct CacheMetrics {
    std::atomic<uint64_t> hits{0};
    std::atomic<uint64_t> misses{0};
    std::atomic<uint64_t> sets{0};
    std::atomic<uint64_t> deletes{0};
    std::atomic<uint64_t> evictions{0};
    std::atomic<uint64_t> expirations{0};

    [[nodiscard]] double hit_rate() const {
        uint64_t h = hits.load();
        uint64_t m = misses.load();
        uint64_t total = h + m;
        return total == 0 ? 0.0 : (static_cast<double>(h) / total) * 100.0;
    }
};

class Cache {
public:
    explicit Cache(std::size_t capacity = 10000);
    ~Cache() = default;

    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;
    Cache(Cache&&) = delete;
    Cache& operator=(Cache&&) = delete;

    void set(
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::seconds> ttl = std::nullopt
    );

    std::optional<std::string> get(const std::string& key);

    bool remove(const std::string& key);

    bool contains(const std::string& key);

    std::size_t size() const;

    std::size_t capacity() const;

    void clear();

    bool expire(const std::string& key, std::chrono::seconds ttl);
    std::optional<std::chrono::seconds> ttl(const std::string& key) const;

    const CacheMetrics& metrics() const { return metrics_; }

    void purge_expired();

private:
    // LRU with throttled promotion (as in memcached): a read moves an entry to
    // the front at most once per kPromoteInterval, so repeated reads of hot
    // keys are served entirely under the shared lock.
    static constexpr std::chrono::milliseconds kPromoteInterval{100};

    struct Node {
        Node(std::string k, CacheEntry e) : key(std::move(k)), entry(std::move(e)) {}

        std::string key;
        CacheEntry entry;
        // steady_clock ticks of the last promotion; 0 means never promoted.
        std::atomic<int64_t> last_promoted{0};
    };

    void evict_lru_unsafe();
    void erase_unsafe(std::list<Node>::iterator it);

    std::size_t capacity_;
    mutable std::shared_mutex mutex_;

    std::list<Node> lru_list_;
    std::unordered_map<std::string, std::list<Node>::iterator> entries_;

    CacheMetrics metrics_;
};

} // namespace shardcache

#endif // SHARDCACHE_CACHE_HPP
