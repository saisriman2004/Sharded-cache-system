#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"

TEST(ClusterTest, NodeRoutingAndStorage) {
    shardcache::Cluster cluster("nodeA", 2, 1000);
    cluster.add_node({"nodeB", "127.0.0.1", 7002});
    cluster.add_node({"nodeC", "127.0.0.1", 7003});

    EXPECT_EQ(cluster.node_count(), 3);

    cluster.set("user:101", "Alice");
    auto res = cluster.get("user:101");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), "Alice");
}
