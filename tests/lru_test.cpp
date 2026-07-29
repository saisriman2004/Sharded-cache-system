#include <gtest/gtest.h>
#include "shardcache/lru.hpp"

TEST(LRUTrackerTest, EvictionOrder) {
    shardcache::LRUTracker tracker(3);
    tracker.touch("a");
    tracker.touch("b");
    tracker.touch("c");

    tracker.touch("a");

    auto evicted = tracker.evict_lru();
    ASSERT_TRUE(evicted.has_value());
    EXPECT_EQ(evicted.value(), "b");
}
