#include "shardcache/replication.hpp"
#include "shardcache/logger.hpp"
#include "network/client.hpp"

namespace shardcache {

ReplicationManager::ReplicationManager(ReplicationMode mode)
    : local_node_id_(""), mode_(mode) {}

ReplicationManager::ReplicationManager(const std::string& local_node_id, ReplicationMode mode)
    : local_node_id_(local_node_id), mode_(mode) {}

bool ReplicationManager::replicate_set(
    const std::vector<CacheNode>& replicas,
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    if (replicas.empty()) return true;

    bool all_ok = true;
    for (const auto& node : replicas) {
        if (!local_node_id_.empty() && node.id == local_node_id_) continue;

        Logger::instance().debug("Replicating SET for key '" + key + "' to node " + node.id + " (" + node.host + ":" + std::to_string(node.port) + ")");
        network::Client client(node.host, node.port);
        if (client.connect()) {
            std::optional<uint64_t> ttl_sec = std::nullopt;
            if (ttl.has_value()) {
                 ttl_sec = static_cast<uint64_t>(ttl.value().count());
            }
            auto res = client.replica_set(key, value, ttl_sec);
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
        network::Client client(node.host, node.port);
        if (client.connect()) {
            auto res = client.replica_delete(key);
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
    std::chrono::seconds ttl
) {
    if (replicas.empty()) return true;

    bool all_ok = true;
    for (const auto& node : replicas) {
        if (!local_node_id_.empty() && node.id == local_node_id_) continue;

        Logger::instance().debug("Replicating EXPIRE for key '" + key + "' to node " + node.id);
        network::Client client(node.host, node.port);
        if (client.connect()) {
            auto res = client.replica_expire(key, static_cast<uint64_t>(ttl.count()));
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
