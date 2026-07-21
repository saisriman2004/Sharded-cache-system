#ifndef SHARDCACHE_CACHE_ENTRY_HPP
#define SHARDCACHE_CACHE_ENTRY_HPP

#include <string>
#include <optional>
#include <chrono>

namespace shardcache {

struct CacheEntry {
    std::string value;
    std::optional<std::chrono::steady_clock::time_point> expires_at{std::nullopt};

    [[nodiscard]] bool is_expired(std::chrono::steady_clock::time_point now = std::chrono::steady_clock::now()) const {
        if (!expires_at.has_value()) {
            return false;
        }
        return now >= expires_at.value();
    }
};

} // namespace shardcache

#endif // SHARDCACHE_CACHE_ENTRY_HPP
