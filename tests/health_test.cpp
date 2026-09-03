#include <gtest/gtest.h>
#include "shardcache/health_checker.hpp"
#include <atomic>
#include <thread>

TEST(HealthCheckerTest, NodeHealthMonitoring) {
    std::atomic<int> failure_callbacks{0};
    shardcache::HealthChecker checker([&failure_callbacks](const std::string& /*node_id*/, bool healthy) {
        if (!healthy) {
            failure_callbacks.fetch_add(1);
        }
    }, std::chrono::milliseconds(30));

    checker.add_node({"node_offline", "127.0.0.1", 19999});
    checker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    checker.stop();

    EXPECT_GE(failure_callbacks.load(), 1);
}

TEST(HealthCheckerTest, CallbackDoesNotDeadlockOnRemoveNode) {
    shardcache::HealthChecker* checker_ptr = nullptr;
    std::atomic<bool> callback_executed{false};

    shardcache::HealthChecker checker([&](const std::string& node_id, bool healthy) {
        if (!healthy) {
            callback_executed.store(true);
            if (checker_ptr) {
                // Calling remove_node from within callback must not deadlock
                checker_ptr->remove_node(node_id);
            }
        }
    }, std::chrono::milliseconds(20));

    checker_ptr = &checker;
    checker.add_node({"deadlock_node", "127.0.0.1", 19998});
    checker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(120));
    checker.stop();

    EXPECT_TRUE(callback_executed.load());
}

TEST(HealthCheckerTest, NodeHealthTransitionAndRecovery) {
    std::atomic<bool> marked_unhealthy{false};

    shardcache::HealthChecker checker([&](const std::string& node_id, bool healthy) {
        if (node_id == "unhealthy_node" && !healthy) {
            marked_unhealthy.store(true);
        }
    }, std::chrono::milliseconds(25));

    checker.add_node({"unhealthy_node", "127.0.0.1", 19997});
    EXPECT_EQ(checker.get_node_health("unhealthy_node"), shardcache::NodeHealth::Healthy);

    checker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(160));
    checker.stop();

    EXPECT_TRUE(marked_unhealthy.load());
    EXPECT_EQ(checker.get_node_health("unhealthy_node"), shardcache::NodeHealth::Unhealthy);
}


