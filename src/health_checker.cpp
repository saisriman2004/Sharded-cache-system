#include "shardcache/health_checker.hpp"
#include "shardcache/logger.hpp"
#include "network/client.hpp"

namespace shardcache {

HealthChecker::HealthChecker(HealthCallback cb, std::chrono::milliseconds check_interval)
    : callback_(std::move(cb)), interval_(check_interval) {}

HealthChecker::~HealthChecker() {
    stop();
}

void HealthChecker::add_node(const CacheNode& node) {
    std::lock_guard lock(nodes_mutex_);
    nodes_.push_back(node);
    failure_counts_[node.id] = 0;
}

void HealthChecker::remove_node(const std::string& node_id) {
    std::lock_guard lock(nodes_mutex_);
    nodes_.erase(
        std::remove_if(nodes_.begin(), nodes_.end(), [&node_id](const CacheNode& n) { return n.id == node_id; }),
        nodes_.end()
    );
    failure_counts_.erase(node_id);
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
        bool healthy = false;
        network::Client client(node.host, node.port);
        if (client.connect() && client.ping()) {
            healthy = true;
        }

        std::lock_guard lock(nodes_mutex_);
        if (healthy) {
            failure_counts_[node.id] = 0;
            if (callback_) {
                callback_(node.id, true);
            }
        } else {
            failure_counts_[node.id]++;
            if (failure_counts_[node.id] >= 3) {
                Logger::instance().warning("Node " + node.id + " failed 3 consecutive TCP PING checks!");
                if (callback_) {
                    callback_(node.id, false);
                }
            }
        }
    }
}

} // namespace shardcache
