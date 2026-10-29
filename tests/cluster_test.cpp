#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"

TEST(ClusterTest, NodeRoutingAndStorage) {
    shardcache::Cluster cluster("nodeA", 1, 1000);
    cluster.add_node({"nodeB", "127.0.0.1", 7002});
    cluster.add_node({"nodeC", "127.0.0.1", 7003});

    EXPECT_EQ(cluster.node_count(), 3ul);

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

TEST(ClusterTest, WriteFailsWithoutQuorum) {
    // N = 2, W = 2: with the only peer offline no write can be acknowledged.
    shardcache::Cluster cluster("nodeA", 2, 1000);
    cluster.add_node({"nodeB", "127.0.0.1", 19995});

    auto result = cluster.put("any_key", "secret_value");
    EXPECT_FALSE(result.ok);
    EXPECT_EQ(result.acks, 1u);  // only the local replica

    // R = 2 cannot be met either: the read reports unavailability, not absence.
    EXPECT_EQ(cluster.read("any_key").status, shardcache::ReadStatus::Unavailable);

    // The missed write is queued for hinted handoff.
    EXPECT_EQ(cluster.pending_hints(), 1u);
}

TEST(ClusterTest, SingleNodeServesLocally) {
    shardcache::Cluster cluster("solo", 3, 1000);
    EXPECT_TRUE(cluster.can_serve_locally("k", true));
    EXPECT_TRUE(cluster.can_serve_locally("k", false));
    EXPECT_TRUE(cluster.set("k", "v"));
    EXPECT_EQ(cluster.get("k").value_or(""), "v");
    EXPECT_TRUE(cluster.remove("k"));
    EXPECT_FALSE(cluster.remove("k"));
    EXPECT_FALSE(cluster.exists("k"));
}
