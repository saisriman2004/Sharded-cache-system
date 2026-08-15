#include <gtest/gtest.h>
#include "shardcache/sharded_cache.hpp"
#include <thread>
#include <vector>

TEST(ShardedCacheTest, BasicOperations) {
    shardcache::ShardedCache cache(1000, 16);

    cache.set("user:1", "Alice");
    cache.set("user:2", "Bob");

    EXPECT_EQ(cache.get("user:1").value(), "Alice");
    EXPECT_EQ(cache.get("user:2").value(), "Bob");
    EXPECT_FALSE(cache.get("user:3").has_value());

    EXPECT_EQ(cache.size(), 2);
    EXPECT_EQ(cache.shard_count(), 16);
}

TEST(ShardedCacheTest, HighConcurrencyMultiThreaded) {
    shardcache::ShardedCache cache(10000, 16);
    constexpr int num_threads = 16;
    constexpr int ops_per_thread = 1000;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string k = "sharded_key_" + std::to_string(t * ops_per_thread + i);
                std::string v = "value_" + std::to_string(i);
                cache.set(k, v);
                (void)cache.get(k);
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_GT(cache.size(), 0);
    EXPECT_EQ(cache.total_hits(), num_threads * ops_per_thread);
}
