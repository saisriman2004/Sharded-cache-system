#ifndef SHARDCACHE_ROUTING_TABLE_HPP
#define SHARDCACHE_ROUTING_TABLE_HPP

#include "shardcache/consistent_hash.hpp"

#include <cstdint>
#include <string>
#include <vector>

namespace shardcache {

// Immutable snapshot of the hash ring with every vnode's preference list
// (its N distinct successor nodes) precomputed. Routing a key is one binary
// search with no locks or allocations; membership changes build a new table.
class RoutingTable {
public:
    RoutingTable(const ConsistentHashRing& ring, std::size_t replication_factor, const std::string& local_id);

    // Index of the ring point that owns `key`.
    std::size_t point_for(const std::string& key) const;

    // Node indices of the preference list for a ring point, in ring order.
    const std::vector<uint16_t>& replicas(std::size_t point) const { return replicas_[point]; }
    bool local_is_replica(std::size_t point) const { return local_member_[point] != 0; }

    const CacheNode& node(uint16_t index) const { return nodes_[index]; }
    std::size_t node_count() const { return nodes_.size(); }
    bool empty() const { return points_.empty(); }

    std::vector<CacheNode> preference_list(std::size_t point) const;
    std::vector<CacheNode> preference_list(const std::string& key) const;

private:
    std::vector<CacheNode> nodes_;
    std::vector<uint32_t> points_;                // sorted vnode hashes
    std::vector<std::vector<uint16_t>> replicas_; // per point
    std::vector<uint8_t> local_member_;           // per point
};

} // namespace shardcache

#endif // SHARDCACHE_ROUTING_TABLE_HPP
