#ifndef SHARDCACHE_CLUSTER_HPP
#define SHARDCACHE_CLUSTER_HPP

#include "shardcache/consistent_hash.hpp"
#include "shardcache/sharded_cache.hpp"
#include "shardcache/replication.hpp"
#include "shardcache/health_checker.hpp"
#include <memory>
#include <string>

#include <unordered_map>

namespace shardcache {

class Cluster {
public:
    /// Construct a cluster node with explicit identity.
    /// @param local_node  The identity of this node (id, advertised host, advertised port).
    /// @param replication_factor  Number of total copies (including primary).
    /// @param cache_capacity  Maximum entries in the local cache.
    explicit Cluster(
        const CacheNode& local_node,
        std::size_t replication_factor = 2,
        std::size_t cache_capacity = 100000
    );

    /// Convenience constructor using just an ID (local address defaults to 127.0.0.1:7001).
    /// Kept for backward compatibility with existing tests.
    explicit Cluster(
        const std::string& local_node_id,
        std::size_t replication_factor = 2,
        std::size_t cache_capacity = 100000
    );

    ~Cluster() = default;

    void add_node(const CacheNode& node);
    void remove_node(const std::string& node_id);

    bool set(
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::seconds> ttl = std::nullopt
    );

    std::optional<std::string> get(const std::string& key);

    bool remove(const std::string& key);

    bool expire(const std::string& key, std::chrono::seconds ttl);
    std::optional<std::chrono::seconds> ttl(const std::string& key);

    std::string get_or_load(
        const std::string& key,
        std::function<std::string()> loader,
        std::optional<std::chrono::seconds> ttl = std::nullopt
    );

    std::size_t node_count() const { return ring_.node_count(); }
    std::optional<CacheNode> locate(const std::string& key) const { return ring_.locate(key); }
    ShardedCache& local_cache() { return local_cache_; }

    const CacheNode& local_node() const { return local_node_; }

private:
    void init_common();

    CacheNode local_node_;
    std::size_t replication_factor_;
    ConsistentHashRing ring_;
    ShardedCache local_cache_;
    ReplicationManager replication_mgr_;
    std::unique_ptr<HealthChecker> health_checker_;
    std::unordered_map<std::string, CacheNode> nodes_;
};

} // namespace shardcache

#endif // SHARDCACHE_CLUSTER_HPP
