#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"
#include "network/client.hpp"

namespace shardcache {

// Common initialization shared by both constructors.
void Cluster::init_common() {
    health_checker_ = std::make_unique<HealthChecker>([this](const std::string& node_id, bool healthy) {
        if (!healthy) {
            Logger::instance().warning("Node " + node_id + " failed health check! Removing from hash ring.");
            this->remove_node(node_id);
        }
    });

    ring_.add_node(local_node_);
    nodes_[local_node_.id] = local_node_;
}

Cluster::Cluster(const CacheNode& local_node, std::size_t replication_factor, std::size_t cache_capacity)
    : local_node_(local_node),
      replication_factor_(replication_factor),
      ring_(100),
      local_cache_(cache_capacity, 16),
      replication_mgr_(local_node.id, ReplicationMode::Sync) {
    init_common();
}

Cluster::Cluster(const std::string& local_node_id, std::size_t replication_factor, std::size_t cache_capacity)
    : local_node_{local_node_id, "127.0.0.1", 7001},
      replication_factor_(replication_factor),
      ring_(100),
      local_cache_(cache_capacity, 16),
      replication_mgr_(local_node_id, ReplicationMode::Sync) {
    init_common();
}

void Cluster::add_node(const CacheNode& node) {
    nodes_[node.id] = node;
    ring_.add_node(node);
    if (node.id != local_node_.id) {
        health_checker_->add_node(node);
    }
}

void Cluster::remove_node(const std::string& node_id) {
    ring_.remove_node(node_id);
    health_checker_->remove_node(node_id);
    nodes_.erase(node_id);
}

bool Cluster::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_.id) {
        local_cache_.set(key, value, ttl);
        auto replicas = ring_.locate_replicas(key, replication_factor_);
        replication_mgr_.replicate_set(replicas, key, value, ttl);
        return true;
    }

    Logger::instance().debug("Forwarding SET key '" + key + "' to primary node " + primary->id + " (" + primary->host + ":" + std::to_string(primary->port) + ")");
    network::Client client(primary->host, primary->port);
    if (client.connect()) {
        std::optional<uint64_t> ttl_sec = std::nullopt;
        if (ttl.has_value()) {
            ttl_sec = static_cast<uint64_t>(ttl.value().count());
        }
        auto res = client.set(key, value, ttl_sec);
        return res.is_ok();
    }

    Logger::instance().warning("Primary node " + primary->id + " unreachable for SET key '" + key + "'");
    return false;
}

std::optional<std::string> Cluster::get(const std::string& key) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_.id) {
        return local_cache_.get(key);
    }

    Logger::instance().debug("Forwarding GET key '" + key + "' to primary node " + primary->id + " (" + primary->host + ":" + std::to_string(primary->port) + ")");
    network::Client client(primary->host, primary->port);
    if (client.connect()) {
        auto res = client.get(key);
        if (res.is_ok()) {
            return res.value;
        }
        if (res.is_not_found()) {
            return std::nullopt;
        }
        Logger::instance().warning("Error retrieving key '" + key + "' from primary node " + primary->id + ": " + res.message);
        return std::nullopt;
    }

    Logger::instance().warning("Primary node " + primary->id + " unreachable for GET key '" + key + "'");
    return std::nullopt;
}

bool Cluster::remove(const std::string& key) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_.id) {
        auto replicas = ring_.locate_replicas(key, replication_factor_);
        replication_mgr_.replicate_delete(replicas, key);
        return local_cache_.remove(key);
    }

    network::Client client(primary->host, primary->port);
    if (client.connect()) {
        auto res = client.remove(key);
        return res.is_ok();
    }

    Logger::instance().warning("Primary node " + primary->id + " unreachable for DELETE key '" + key + "'");
    return false;
}

bool Cluster::expire(const std::string& key, std::chrono::seconds ttl) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_.id) {
        return local_cache_.expire(key, ttl);
    }

    network::Client client(primary->host, primary->port);
    if (client.connect()) {
        auto res = client.expire(key, static_cast<uint64_t>(ttl.count()));
        return res.is_ok();
    }

    Logger::instance().warning("Primary node " + primary->id + " unreachable for EXPIRE key '" + key + "'");
    return false;
}

std::optional<std::chrono::seconds> Cluster::ttl(const std::string& key) {
    auto primary = ring_.locate(key);
    if (!primary.has_value() || primary->id == local_node_.id) {
        return local_cache_.ttl(key);
    }

    network::Client client(primary->host, primary->port);
    if (client.connect()) {
        auto res = client.ttl(key);
        if (res.is_ok() && res.value.has_value()) {
            return std::chrono::seconds(res.value.value());
        }
        return std::nullopt;
    }

    Logger::instance().warning("Primary node " + primary->id + " unreachable for TTL key '" + key + "'");
    return std::nullopt;
}

std::string Cluster::get_or_load(
    const std::string& key,
    std::function<std::string()> loader,
    std::optional<std::chrono::seconds> ttl
) {
    return local_cache_.get_or_load(key, loader, ttl);
}

} // namespace shardcache
