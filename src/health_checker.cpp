#include "shardcache/health_checker.hpp"
#include "shardcache/logger.hpp"
#include <algorithm>

namespace shardcache {

HealthChecker::HealthChecker(HealthCallback cb, std::chrono::milliseconds check_interval)
    : callback_(std::move(cb)), interval_(check_interval) {}

HealthChecker::~HealthChecker() {
    stop();
}

void HealthChecker::add_node(const CacheNode& node) {
    std::lock_guard lock(nodes_mutex_);
    nodes_.push_back(node);
}

void HealthChecker::remove_node(const std::string& node_id) {
    std::lock_guard lock(nodes_mutex_);
    nodes_.erase(
        std::remove_if(nodes_.begin(), nodes_.end(), [&node_id](const CacheNode& n) { return n.id == node_id; }),
        nodes_.end()
    );
}

void HealthChecker::start() {
    if (running_.exchange(true)) return;
    worker_ = std::thread([this]() {
        while (running_.load()) {
            std::this_thread::sleep_for(interval_);
            if (!running_.load()) break;
            run_check();
        }
    });
}

void HealthChecker::stop() {
    if (!running_.exchange(false)) return;
    if (worker_.joinable()) {
        worker_.join();
    }
}

void HealthChecker::run_check() {
    std::vector<CacheNode> target_nodes;
    {
        std::lock_guard lock(nodes_mutex_);
        target_nodes = nodes_;
    }

    for (const auto& node : target_nodes) {
        bool healthy = true;
        if (callback_) {
            callback_(node.id, healthy);
        }
    }
}

} // namespace shardcache
