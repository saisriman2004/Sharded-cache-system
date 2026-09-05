#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"
#include "network/client.hpp"

namespace shardcache {

// Common initialization shared by both constructors.
void Cluster::init_common() {
    health_checker_ = std::make_unique<HealthChecker>([this](const std::string& node_id, bool healthy) {
        std::unique_lock lock(membership_mutex_);
        node_health_[node_id] = healthy;
        if (!healthy) {
            Logger::instance().warning("Node " + node_id + " marked unhealthy in cluster routing.");
        } else {
            Logger::instance().info("Node " + node_id + " marked healthy in cluster routing.");
        }
    });

    ring_.add_node(local_node_);
    nodes_[local_node_.id] = local_node_;
    node_health_[local_node_.id] = true;
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

Cluster::~Cluster() {
    stop();
}

void Cluster::start() {
    if (health_checker_) {
        health_checker_->start();
    }
}

void Cluster::stop() {
    if (health_checker_) {
        health_checker_->stop();
    }
}

void Cluster::add_node(const CacheNode& node) {
    {
        std::unique_lock lock(membership_mutex_);
        nodes_[node.id] = node;
        ring_.add_node(node);
        node_health_[node.id] = true;
    }
    if (node.id != local_node_.id) {
        health_checker_->add_node(node);
    }
}

void Cluster::remove_node(const std::string& node_id) {
    {
        std::unique_lock lock(membership_mutex_);
        ring_.remove_node(node_id);
        nodes_.erase(node_id);
        node_health_.erase(node_id);
    }
    health_checker_->remove_node(node_id);
}

std::size_t Cluster::node_count() const {
    std::shared_lock lock(membership_mutex_);
    return ring_.node_count();
}

std::optional<CacheNode> Cluster::locate(const std::string& key) const {
    std::shared_lock lock(membership_mutex_);
    auto primary = ring_.locate(key);
    if (!primary.has_value()) return std::nullopt;
    auto it = node_health_.find(primary->id);
    if (it != node_health_.end() && !it->second) {
        auto replicas = ring_.locate_replicas(key, replication_factor_);
        for (const auto& rep : replicas) {
            auto rep_it = node_health_.find(rep.id);
            if (rep_it == node_health_.end() || rep_it->second) {
                return rep;
            }
        }
    }
    return primary;
}

std::vector<CacheNode> Cluster::locate_replicas(const std::string& key, std::size_t count) const {
    std::shared_lock lock(membership_mutex_);
    return ring_.locate_replicas(key, count);
}

bool Cluster::is_node_healthy(const std::string& node_id) const {
    std::shared_lock lock(membership_mutex_);
    auto it = node_health_.find(node_id);
    if (it != node_health_.end()) {
        return it->second;
    }
    return true;
}

void Cluster::set_node_health(const std::string& node_id, bool healthy) {
    std::unique_lock lock(membership_mutex_);
    node_health_[node_id] = healthy;
    if (!healthy) {
        Logger::instance().warning("Node " + node_id + " marked unhealthy in cluster routing.");
    } else {
        Logger::instance().info("Node " + node_id + " marked healthy in cluster routing.");
    }
}

bool Cluster::set(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    std::optional<CacheNode> primary;
    std::vector<CacheNode> replicas;
    {
        std::shared_lock lock(membership_mutex_);
        primary = ring_.locate(key);
        if (!primary.has_value() || primary->id == local_node_.id) {
            replicas = ring_.locate_replicas(key, replication_factor_);
        }
    }

    if (!primary.has_value() || primary->id == local_node_.id) {
        local_cache_.set(key, value, ttl);
        bool repl_ok = replication_mgr_.replicate_set(replicas, key, value, ttl);
        if (replication_mgr_.mode() == ReplicationMode::Sync && !repl_ok) {
            local_cache_.remove(key);
            Logger::instance().warning("Sync replication failed for key '" + key + "', write rolled back");
            return false;
        }
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
    auto target = locate(key);
    if (!target.has_value()) {
        Logger::instance().warning("No healthy node available to serve GET for key '" + key + "'");
        return std::nullopt;
    }

    if (target->id == local_node_.id) {
        return local_cache_.get(key);
    }

    Logger::instance().debug("Forwarding GET key '" + key + "' to target node " + target->id + " (" + target->host + ":" + std::to_string(target->port) + ")");
    network::Client client(target->host, target->port);
    if (client.connect()) {
        auto res = client.get(key);
        if (res.is_ok()) {
            return res.value;
        }
        if (res.is_not_found()) {
            return std::nullopt;
        }
        Logger::instance().warning("Error retrieving key '" + key + "' from node " + target->id + ": " + res.message);
        return std::nullopt;
    }

    Logger::instance().warning("Target node " + target->id + " unreachable for GET key '" + key + "'");
    return std::nullopt;
}

bool Cluster::exists(const std::string& key) {
    return get(key).has_value();
}

bool Cluster::remove(const std::string& key) {
    std::optional<CacheNode> primary;
    std::vector<CacheNode> replicas;
    {
        std::shared_lock lock(membership_mutex_);
        primary = ring_.locate(key);
        if (!primary.has_value() || primary->id == local_node_.id) {
            replicas = ring_.locate_replicas(key, replication_factor_);
        }
    }

    if (!primary.has_value() || primary->id == local_node_.id) {
        bool repl_ok = replication_mgr_.replicate_delete(replicas, key);
        bool removed = local_cache_.remove(key);
        if (replication_mgr_.mode() == ReplicationMode::Sync && !repl_ok) {
            Logger::instance().warning("Sync replication failed for DELETE key '" + key + "'");
            return false;
        }
        return removed;
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
    std::optional<CacheNode> primary;
    std::vector<CacheNode> replicas;
    {
        std::shared_lock lock(membership_mutex_);
        primary = ring_.locate(key);
        if (!primary.has_value() || primary->id == local_node_.id) {
            replicas = ring_.locate_replicas(key, replication_factor_);
        }
    }

    if (!primary.has_value() || primary->id == local_node_.id) {
        bool repl_ok = replication_mgr_.replicate_expire(replicas, key, ttl);
        bool ok = local_cache_.expire(key, ttl);
        if (replication_mgr_.mode() == ReplicationMode::Sync && !repl_ok) {
            Logger::instance().warning("Sync replication failed for EXPIRE key '" + key + "'");
            return false;
        }
        return ok;
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
    std::optional<CacheNode> primary;
    {
        std::shared_lock lock(membership_mutex_);
        primary = ring_.locate(key);
    }

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
