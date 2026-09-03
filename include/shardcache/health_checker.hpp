#ifndef SHARDCACHE_HEALTH_CHECKER_HPP
#define SHARDCACHE_HEALTH_CHECKER_HPP

#include "shardcache/consistent_hash.hpp"
#include <thread>
#include <atomic>
#include <chrono>
#include <functional>
#include <vector>
#include <mutex>

#include <unordered_map>

namespace shardcache {

enum class NodeHealth {
    Healthy,
    Suspect,
    Unhealthy
};

struct NodeHealthState {
    NodeHealth status{NodeHealth::Healthy};
    int failures{0};
    int successes{0};
};

class HealthChecker {
public:
    using HealthCallback = std::function<void(const std::string& node_id, bool is_healthy)>;

    explicit HealthChecker(HealthCallback cb, std::chrono::milliseconds check_interval = std::chrono::milliseconds(1000));
    ~HealthChecker();

    void add_node(const CacheNode& node);
    void remove_node(const std::string& node_id);

    void start();
    void stop();

    NodeHealth get_node_health(const std::string& node_id) const;

private:
    void run_check();

    HealthCallback callback_;
    std::chrono::milliseconds interval_;
    std::atomic<bool> running_{false};
    std::thread worker_;
    mutable std::mutex nodes_mutex_;
    std::vector<CacheNode> nodes_;
    std::unordered_map<std::string, NodeHealthState> node_states_;
};

} // namespace shardcache

#endif // SHARDCACHE_HEALTH_CHECKER_HPP
