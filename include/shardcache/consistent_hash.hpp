#ifndef SHARDCACHE_CONSISTENT_HASH_HPP
#define SHARDCACHE_CONSISTENT_HASH_HPP

#include <string>
#include <map>
#include <vector>
#include <optional>
#include <cstdint>

namespace shardcache {

struct CacheNode {
    std::string id;
    std::string host;
    uint16_t port{0};

    bool operator==(const CacheNode& other) const {
        return id == other.id && host == other.host && port == other.port;
    }
};

class ConsistentHashRing {
public:
    explicit ConsistentHashRing(std::size_t virtual_nodes_per_physical = 100);
    ~ConsistentHashRing() = default;

    void add_node(const CacheNode& node);
    void remove_node(const std::string& node_id);

    [[nodiscard]] std::optional<CacheNode> locate(const std::string& key) const;
    [[nodiscard]] std::vector<CacheNode> locate_replicas(const std::string& key, std::size_t count) const;

    [[nodiscard]] std::size_t node_count() const { return physical_nodes_.size(); }
    [[nodiscard]] std::size_t ring_size() const { return ring_.size(); }

    static uint32_t hash_fn(const std::string& key);

private:
    std::size_t num_vnodes_;
    std::map<std::string, CacheNode> physical_nodes_;
    std::map<uint32_t, CacheNode> ring_;
};

} // namespace shardcache

#endif // SHARDCACHE_CONSISTENT_HASH_HPP
