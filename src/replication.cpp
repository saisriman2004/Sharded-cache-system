#include "shardcache/replication.hpp"
#include "shardcache/logger.hpp"

namespace shardcache {

ReplicationManager::ReplicationManager(ReplicationMode mode)
    : mode_(mode) {}

bool ReplicationManager::replicate_set(
    const std::vector<CacheNode>& replicas,
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl
) {
    if (replicas.empty()) return true;

    for (const auto& node : replicas) {
        Logger::instance().debug("Replicating SET for key '" + key + "' to node " + node.id);
    }
    return true;
}

bool ReplicationManager::replicate_delete(
    const std::vector<CacheNode>& replicas,
    const std::string& key
) {
    if (replicas.empty()) return true;

    for (const auto& node : replicas) {
        Logger::instance().debug("Replicating DELETE for key '" + key + "' to node " + node.id);
    }
    return true;
}

} // namespace shardcache
