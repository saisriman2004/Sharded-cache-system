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
    node_states_[node.id] = NodeHealthState{NodeHealth::Healthy, 0, 0};
}

void HealthChecker::remove_node(const std::string& node_id) {
    std::lock_guard lock(nodes_mutex_);
    nodes_.erase(
        std::remove_if(nodes_.begin(), nodes_.end(), [&node_id](const CacheNode& n) { return n.id == node_id; }),
        nodes_.end()
    );
    node_states_.erase(node_id);
}

NodeHealth HealthChecker::get_node_health(const std::string& node_id) const {
    std::lock_guard lock(nodes_mutex_);
    auto it = node_states_.find(node_id);
    if (it != node_states_.end()) {
        return it->second.status;
    }
    return NodeHealth::Unhealthy;
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

    std::vector<std::pair<std::string, bool>> events;

    for (const auto& node : target_nodes) {
        bool healthy = false;
        network::Client client(node.host, node.port);
        if (client.connect() && client.ping()) {
            healthy = true;
        }

        {
            std::lock_guard lock(nodes_mutex_);
            auto& state = node_states_[node.id];
            if (healthy) {
                state.failures = 0;
                state.successes++;
                if (state.status != NodeHealth::Healthy && state.successes >= 2) {
                    state.status = NodeHealth::Healthy;
                    Logger::instance().info("Node " + node.id + " recovered and passed health checks!");
                    events.emplace_back(node.id, true);
                }
            } else {
                state.successes = 0;
                state.failures++;
                if (state.status == NodeHealth::Healthy && state.failures >= 1) {
                    state.status = NodeHealth::Suspect;
                }
                if (state.status != NodeHealth::Unhealthy && state.failures >= 3) {
                    state.status = NodeHealth::Unhealthy;
                    Logger::instance().warning("Node " + node.id + " failed 3 consecutive TCP PING checks!");
                    events.emplace_back(node.id, false);
                }
            }
        }
    }

    if (callback_) {
        for (const auto& [node_id, is_healthy] : events) {
            callback_(node_id, is_healthy);
        }
    }
}

} // namespace shardcache
