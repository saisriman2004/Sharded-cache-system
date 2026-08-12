#include <gtest/gtest.h>
#include "shardcache/health_checker.hpp"
#include <atomic>

TEST(HealthCheckerTest, NodeHealthMonitoring) {
    std::atomic<int> checks{0};
    shardcache::HealthChecker checker([&checks](const std::string& node_id, bool healthy) {
        checks.fetch_add(1);
    }, std::chrono::milliseconds(50));

    checker.add_node({"nodeA", "127.0.0.1", 7001});
    checker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(180));
    checker.stop();

    EXPECT_GE(checks.load(), 2);
}
