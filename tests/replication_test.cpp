#include <gtest/gtest.h>
#include "shardcache/replication.hpp"

TEST(ReplicationTest, ReplicateSetAndDelete) {
    shardcache::ReplicationManager mgr("nodeA", shardcache::ReplicationMode::Sync);
    
    // Empty replica list succeeds immediately
    EXPECT_TRUE(mgr.replicate_set({}, "k1", "v1"));
    EXPECT_TRUE(mgr.replicate_delete({}, "k1"));

    // Unreachable replica nodes correctly fail replication
    std::vector<shardcache::CacheNode> replicas = {
        {"nodeB", "127.0.0.1", 7002},
        {"nodeC", "127.0.0.1", 7003}
    };

    EXPECT_FALSE(mgr.replicate_set(replicas, "k1", "v1"));
    EXPECT_FALSE(mgr.replicate_delete(replicas, "k1"));
}
