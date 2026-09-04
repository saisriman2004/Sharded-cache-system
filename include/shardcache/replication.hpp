#ifndef SHARDCACHE_REPLICATION_HPP
#define SHARDCACHE_REPLICATION_HPP

#include "shardcache/consistent_hash.hpp"
#include <string>
#include <vector>
#include <optional>
#include <chrono>

namespace shardcache {

enum class ReplicationMode {
    Sync,
    Async
};

class ReplicationManager {
public:
    explicit ReplicationManager(ReplicationMode mode = ReplicationMode::Sync);
    explicit ReplicationManager(
        const std::string& local_node_id,
        ReplicationMode mode = ReplicationMode::Sync
    );
    ~ReplicationManager() = default;

    bool replicate_set(
        const std::vector<CacheNode>& replicas,
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::seconds> ttl = std::nullopt
    );

    bool replicate_delete(
        const std::vector<CacheNode>& replicas,
        const std::string& key
    );

    bool replicate_expire(
        const std::vector<CacheNode>& replicas,
        const std::string& key,
        std::chrono::seconds ttl
    );

    ReplicationMode mode() const { return mode_; }

private:
    std::string local_node_id_;
    ReplicationMode mode_;
};

} // namespace shardcache

#endif // SHARDCACHE_REPLICATION_HPP
