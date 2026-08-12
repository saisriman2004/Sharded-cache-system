#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"

namespace shardcache {

Cluster::Cluster(const std::string& local_node_id, std::size_t replication_factor, std::size_t cache_capacity)
    : local_node_id_(local_node_id),
      replication_factor_(replication_factor),
      ring_(100),
      local_cache_(cache_capacity, 16),
      replication_mgr_(ReplicationMode::Sync) {

    health_checker_ = std::make_unique<HealthChecker>([this](const std::string& node_id, bool healthy) {
        if (!healthy) {
            Logger::instance().warning("Node " + node_id + " failed health check! Removing from hash ring.");
            ring_.remove_node(node_id);
        }
    });

    ring_.add_node({local_node_id_, "127.0.0.1", 7001});
}

void Cluster::add_node(const CacheNode& node) {
    ring_.add_node(node);
    health_checker_->add_node(node);
}

void Cluster::remove_node(const std::string& node_id) {
    ring_.remove_node(node_id);
    health_checker_->remove_node(node_id);
}

void Cluster::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    auto primary = ring_.locate(key);
    if (!primary.has_value()) {
        local_cache_.set(key, value, ttl);
        return;
    }

    if (primary->id == local_node_id_) {
        local_cache_.set(key, value, ttl);
        auto replicas = ring_.locate_replicas(key, replication_factor_);
        replication_mgr_.replicate_set(replicas, key, value, ttl);
    } else {
        local_cache_.set(key, value, ttl);
    }
}

std::optional<std::string> Cluster::get(const std::string& key) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_id_) {
        return local_cache_.get(key);
    }
    auto val = local_cache_.get(key);
    if (val.has_value()) return val;
    return std::nullopt;
}

bool Cluster::remove(const std::string& key) {
    auto replicas = ring_.locate_replicas(key, replication_factor_);
    replication_mgr_.replicate_delete(replicas, key);
    return local_cache_.remove(key);
}

} // namespace shardcache
