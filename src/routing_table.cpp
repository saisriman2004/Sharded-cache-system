#include "shardcache/routing_table.hpp"

#include <algorithm>
#include <unordered_map>

namespace shardcache {

RoutingTable::RoutingTable(const ConsistentHashRing& ring, std::size_t replication_factor, const std::string& local_id) {
    std::unordered_map<std::string, uint16_t> index_of;
    std::vector<uint16_t> owner;
    for (const auto& [hash, node] : ring.points()) {
        auto [it, inserted] = index_of.emplace(node.id, static_cast<uint16_t>(nodes_.size()));
        if (inserted) nodes_.push_back(node);
        points_.push_back(hash);
        owner.push_back(it->second);
    }

    const std::size_t n = std::min(replication_factor, nodes_.size());
    const std::size_t points = points_.size();
    replicas_.resize(points);
    local_member_.resize(points, 0);
    for (std::size_t p = 0; p < points; ++p) {
        auto& list = replicas_[p];
        for (std::size_t step = 0; step < points && list.size() < n; ++step) {
            uint16_t candidate = owner[(p + step) % points];
            if (std::find(list.begin(), list.end(), candidate) == list.end()) {
                list.push_back(candidate);
            }
        }
        for (uint16_t idx : list) {
            if (nodes_[idx].id == local_id) local_member_[p] = 1;
        }
    }
}

std::size_t RoutingTable::point_for(const std::string& key) const {
    auto it = std::lower_bound(points_.begin(), points_.end(), ConsistentHashRing::hash_fn(key));
    return it == points_.end() ? 0 : static_cast<std::size_t>(it - points_.begin());
}

std::vector<CacheNode> RoutingTable::preference_list(std::size_t point) const {
    std::vector<CacheNode> out;
    out.reserve(replicas_[point].size());
    for (uint16_t idx : replicas_[point]) out.push_back(nodes_[idx]);
    return out;
}

std::vector<CacheNode> RoutingTable::preference_list(const std::string& key) const {
    if (empty()) return {};
    return preference_list(point_for(key));
}

} // namespace shardcache
