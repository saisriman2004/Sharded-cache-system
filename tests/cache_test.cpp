#include <gtest/gtest.h>
#include "shardcache/cache.hpp"

#include <thread>
#include <vector>
#include <chrono>

TEST(CacheTest, BasicSetAndGet) {
    shardcache::Cache cache(10);
    cache.set("key1", "val1");
    auto res = cache.get("key1");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), "val1");
}

TEST(CacheTest, OverwriteExistingKey) {
    shardcache::Cache cache(10);
    cache.set("key1", "val1");
    cache.set("key1", "val2");
    auto res = cache.get("key1");
    ASSERT_TRUE(res.has_value());
    EXPECT_EQ(res.value(), "val2");
    EXPECT_EQ(cache.size(), 1);
}

TEST(CacheTest, MissingKey) {
    shardcache::Cache cache(10);
    auto res = cache.get("nonexistent");
    EXPECT_FALSE(res.has_value());
}

TEST(CacheTest, RemoveKey) {
    shardcache::Cache cache(10);
    cache.set("key1", "val1");
    EXPECT_TRUE(cache.remove("key1"));
    EXPECT_FALSE(cache.contains("key1"));
    EXPECT_FALSE(cache.remove("key1"));
    EXPECT_EQ(cache.size(), 0);
}

TEST(CacheTest, ClearCache) {
    shardcache::Cache cache(10);
    cache.set("k1", "v1");
    cache.set("k2", "v2");
    EXPECT_EQ(cache.size(), 2);
    cache.clear();
    EXPECT_EQ(cache.size(), 0);
    EXPECT_FALSE(cache.contains("k1"));
}

TEST(CacheTest, EmptyAndLargeValues) {
    shardcache::Cache cache(10);
    std::string empty_val = "";
    std::string large_val(1024 * 1024, 'X');

    cache.set("empty", empty_val);
    cache.set("large", large_val);

    EXPECT_EQ(cache.get("empty").value(), empty_val);
    EXPECT_EQ(cache.get("large").value(), large_val);
}

TEST(CacheTest, LRUEviction) {
    shardcache::Cache cache(3);
    cache.set("k1", "v1");
    cache.set("k2", "v2");
    cache.set("k3", "v3");

    (void)cache.get("k1");

    cache.set("k4", "v4");

    EXPECT_TRUE(cache.contains("k1"));
    EXPECT_FALSE(cache.contains("k2"));
    EXPECT_TRUE(cache.contains("k3"));
    EXPECT_TRUE(cache.contains("k4"));
    EXPECT_EQ(cache.size(), 3);
}

TEST(CacheTest, TTLExpiration) {
    shardcache::Cache cache(10);
    cache.set("temp", "data", std::chrono::seconds(1));

    EXPECT_TRUE(cache.contains("temp"));
    EXPECT_EQ(cache.get("temp").value(), "data");

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));

    EXPECT_FALSE(cache.contains("temp"));
    EXPECT_FALSE(cache.get("temp").has_value());
}

TEST(CacheTest, ConcurrentAccess) {
    shardcache::Cache cache(1000);
    constexpr int num_threads = 8;
    constexpr int ops_per_thread = 500;

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    for (int t = 0; t < num_threads; ++t) {
        threads.emplace_back([&cache, t]() {
            for (int i = 0; i < ops_per_thread; ++i) {
                std::string k = "key_" + std::to_string((t * ops_per_thread + i) % 100);
                std::string v = "val_" + std::to_string(i);
                cache.set(k, v);
                (void)cache.get(k);
                if (i % 10 == 0) {
                    cache.contains(k);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_GT(cache.size(), 0);
    EXPECT_LE(cache.size(), 1000);
}

TEST(CacheTest, MetricsTracking) {
    shardcache::Cache cache(10);
    cache.set("k1", "v1");
    (void)cache.get("k1");
    (void)cache.get("missing");

    const auto& metrics = cache.metrics();
    EXPECT_EQ(metrics.sets.load(), 1);
    EXPECT_EQ(metrics.hits.load(), 1);
    EXPECT_EQ(metrics.misses.load(), 1);
    EXPECT_DOUBLE_EQ(metrics.hit_rate(), 50.0);
}
