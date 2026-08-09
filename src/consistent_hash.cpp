#include "shardcache/consistent_hash.hpp"
#include <algorithm>
#include <set>

namespace shardcache {

uint32_t ConsistentHashRing::hash_fn(const std::string& key) {
    uint32_t hash = 2166136261u;
    for (char c : key) {
        hash ^= static_cast<uint8_t>(c);
        hash *= 16777619u;
    }
    return hash;
}

ConsistentHashRing::ConsistentHashRing(std::size_t virtual_nodes_per_physical)
    : num_vnodes_(virtual_nodes_per_physical) {}

void ConsistentHashRing::add_node(const CacheNode& node) {
    physical_nodes_[node.id] = node;
    for (std::size_t i = 0; i < num_vnodes_; ++i) {
        std::string vnode_id = node.id + "#" + std::to_string(i);
        uint32_t hash_val = hash_fn(vnode_id);
        ring_[hash_val] = node;
    }
}

void ConsistentHashRing::remove_node(const std::string& node_id) {
    auto it = physical_nodes_.find(node_id);
    if (it == physical_nodes_.end()) return;

    for (std::size_t i = 0; i < num_vnodes_; ++i) {
        std::string vnode_id = node_id + "#" + std::to_string(i);
        uint32_t hash_val = hash_fn(vnode_id);
        ring_.erase(hash_val);
    }
    physical_nodes_.erase(it);
}

std::optional<CacheNode> ConsistentHashRing::locate(const std::string& key) const {
    if (ring_.empty()) {
        return std::nullopt;
    }

    uint32_t hash_val = hash_fn(key);
    auto it = ring_.lower_bound(hash_val);
    if (it == ring_.end()) {
        it = ring_.begin();
    }
    return it->second;
}

std::vector<CacheNode> ConsistentHashRing::locate_replicas(const std::string& key, std::size_t count) const {
    std::vector<CacheNode> replicas;
    if (ring_.empty() || count == 0) {
        return replicas;
    }

    uint32_t hash_val = hash_fn(key);
    auto it = ring_.lower_bound(hash_val);
    std::set<std::string> seen_nodes;

    std::size_t steps = 0;
    while (seen_nodes.size() < count && steps < ring_.size()) {
        if (it == ring_.end()) {
            it = ring_.begin();
        }
        if (seen_nodes.find(it->second.id) == seen_nodes.end()) {
            seen_nodes.insert(it->second.id);
            replicas.push_back(it->second);
        }
        ++it;
        ++steps;
    }

    return replicas;
}

} // namespace shardcache
