#include <gtest/gtest.h>
#include "shardcache/consistent_hash.hpp"
#include <map>
#include <cmath>

TEST(ConsistentHashTest, NodeAdditionAndLocate) {
    shardcache::ConsistentHashRing ring(100);

    shardcache::CacheNode nodeA{"nodeA", "127.0.0.1", 7001};
    shardcache::CacheNode nodeB{"nodeB", "127.0.0.1", 7002};

    ring.add_node(nodeA);
    ring.add_node(nodeB);

    EXPECT_EQ(ring.node_count(), 2);
    EXPECT_EQ(ring.ring_size(), 200);

    auto target = ring.locate("user:100");
    ASSERT_TRUE(target.has_value());
    EXPECT_TRUE(target.value() == nodeA || target.value() == nodeB);
}

TEST(ConsistentHashTest, DistributionUniformity) {
    shardcache::ConsistentHashRing ring(100);

    shardcache::CacheNode nodeA{"nodeA", "127.0.0.1", 7001};
    shardcache::CacheNode nodeB{"nodeB", "127.0.0.1", 7002};
    shardcache::CacheNode nodeC{"nodeC", "127.0.0.1", 7003};
    shardcache::CacheNode nodeD{"nodeD", "127.0.0.1", 7004};

    ring.add_node(nodeA);
    ring.add_node(nodeB);
    ring.add_node(nodeC);
    ring.add_node(nodeD);

    constexpr int total_keys = 10000;
    std::map<std::string, int> counts;

    for (int i = 0; i < total_keys; ++i) {
        std::string key = "key_" + std::to_string(i);
        auto node = ring.locate(key);
        ASSERT_TRUE(node.has_value());
        counts[node.value().id]++;
    }

    for (const auto& [node_id, count] : counts) {
        double percentage = (static_cast<double>(count) / total_keys) * 100.0;
        EXPECT_GE(percentage, 15.0);
        EXPECT_LE(percentage, 35.0);
    }
}

TEST(ConsistentHashTest, ReplicasLookup) {
    shardcache::ConsistentHashRing ring(100);

    ring.add_node({"nodeA", "127.0.0.1", 7001});
    ring.add_node({"nodeB", "127.0.0.1", 7002});
    ring.add_node({"nodeC", "127.0.0.1", 7003});

    auto replicas = ring.locate_replicas("session:xyz", 2);
    EXPECT_EQ(replicas.size(), 2);
    EXPECT_NE(replicas[0].id, replicas[1].id);
}
