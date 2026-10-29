#include "shardcache/replication.hpp"
#include "shardcache/logger.hpp"
#include "network/peer_pool.hpp"

namespace shardcache {

ReplicationManager::ReplicationManager(ReplicationMode mode)
    : ReplicationManager("", mode) {}

ReplicationManager::ReplicationManager(
    const std::string& local_node_id,
    ReplicationMode mode,
    std::shared_ptr<network::PeerPool> peer_pool
)
    : local_node_id_(local_node_id),
      mode_(mode),
      peer_pool_(peer_pool ? std::move(peer_pool) : std::make_shared<network::PeerPool>()) {}

bool ReplicationManager::replicate_set(
    const std::vector<CacheNode>& replicas,
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::milliseconds> ttl
) {
    if (replicas.empty()) return true;

    bool all_ok = true;
    for (const auto& node : replicas) {
        if (!local_node_id_.empty() && node.id == local_node_id_) continue;

        Logger::instance().debug("Replicating SET for key '" + key + "' to node " + node.id + " (" + node.host + ":" + std::to_string(node.port) + ")");
        auto client = peer_pool_->acquire(node.host, node.port);
        if (client) {
            std::optional<uint64_t> ttl_ms = std::nullopt;
            if (ttl.has_value()) {
                ttl_ms = static_cast<uint64_t>(ttl.value().count());
            }
            auto res = (*client)->replica_set(key, value, ttl_ms);
            if (!res.is_ok()) {
                Logger::instance().warning("Failed replication SET to replica node " + node.id + ": " + res.message);
                all_ok = false;
            }
        } else {
            Logger::instance().warning("Could not connect to replica node " + node.id + " for replication");
            all_ok = false;
        }
    }
    return all_ok;
}

bool ReplicationManager::replicate_delete(
    const std::vector<CacheNode>& replicas,
    const std::string& key
) {
    if (replicas.empty()) return true;

    bool all_ok = true;
    for (const auto& node : replicas) {
        if (!local_node_id_.empty() && node.id == local_node_id_) continue;

        Logger::instance().debug("Replicating DELETE for key '" + key + "' to node " + node.id);
        auto client = peer_pool_->acquire(node.host, node.port);
        if (client) {
            auto res = (*client)->replica_delete(key);
            if (!res.is_ok()) {
                all_ok = false;
            }
        } else {
            all_ok = false;
        }
    }
    return all_ok;
}

bool ReplicationManager::replicate_expire(
    const std::vector<CacheNode>& replicas,
    const std::string& key,
    std::chrono::milliseconds ttl
) {
    if (replicas.empty()) return true;

    bool all_ok = true;
    for (const auto& node : replicas) {
        if (!local_node_id_.empty() && node.id == local_node_id_) continue;

        Logger::instance().debug("Replicating EXPIRE for key '" + key + "' to node " + node.id);
        auto client = peer_pool_->acquire(node.host, node.port);
        if (client) {
            auto res = (*client)->replica_expire(key, static_cast<uint64_t>(ttl.count()));
            if (!res.is_ok()) {
                all_ok = false;
            }
        } else {
            all_ok = false;
        }
    }
    return all_ok;
}

} // namespace shardcache
