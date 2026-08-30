#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"

TEST(ClusterTest, NodeRoutingAndStorage) {
    shardcache::Cluster cluster("nodeA", 2, 1000);
    cluster.add_node({"nodeB", "127.0.0.1", 7002});
    cluster.add_node({"nodeC", "127.0.0.1", 7003});

    EXPECT_EQ(cluster.node_count(), 3);

    // Find keys belonging to local node (nodeA) and remote nodes
    std::string local_key;
    std::string remote_key;
    for (int i = 0; i < 1000; ++i) {
        std::string k = "key:" + std::to_string(i);
        auto primary = cluster.locate(k);
        if (primary.has_value()) {
            if (primary->id == "nodeA" && local_key.empty()) {
                local_key = k;
            } else if (primary->id != "nodeA" && remote_key.empty()) {
                remote_key = k;
            }
        }
        if (!local_key.empty() && !remote_key.empty()) break;
    }
    ASSERT_FALSE(local_key.empty());
    ASSERT_FALSE(remote_key.empty());

    // Local key succeeds
    EXPECT_TRUE(cluster.set(local_key, "Alice"));
    auto res = cluster.get(local_key);
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), "Alice");

    // Remote key without active remote server fails safely without fallback
    EXPECT_FALSE(cluster.set(remote_key, "Bob"));
    EXPECT_FALSE(cluster.get(remote_key).has_value());
}
