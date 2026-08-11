#include <gtest/gtest.h>
#include "shardcache/replication.hpp"

TEST(ReplicationTest, ReplicateSetAndDelete) {
    shardcache::ReplicationManager mgr(shardcache::ReplicationMode::Sync);
    std::vector<shardcache::CacheNode> replicas = {
        {"nodeB", "127.0.0.1", 7002},
        {"nodeC", "127.0.0.1", 7003}
    };

    EXPECT_TRUE(mgr.replicate_set(replicas, "k1", "v1"));
    EXPECT_TRUE(mgr.replicate_delete(replicas, "k1"));
}
