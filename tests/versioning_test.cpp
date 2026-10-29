#include <gtest/gtest.h>
#include "shardcache/cache.hpp"
#include "shardcache/hybrid_clock.hpp"
#include "shardcache/sharded_cache.hpp"

#include <set>
#include <thread>
#include <vector>

using shardcache::VersionedValue;

TEST(HybridClockTest, StrictlyMonotonicAcrossThreads) {
    shardcache::HybridClock clock;
    constexpr int kThreads = 8;
    constexpr int kPerThread = 20000;
    std::vector<std::vector<uint64_t>> seen(kThreads);
    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&, t]() {
            uint64_t prev = 0;
            for (int i = 0; i < kPerThread; ++i) {
                uint64_t ts = clock.now();
                EXPECT_GT(ts, prev);
                prev = ts;
                seen[t].push_back(ts);
            }
        });
    }
    for (auto& th : threads) th.join();

    std::set<uint64_t> unique;
    for (auto& v : seen) unique.insert(v.begin(), v.end());
    EXPECT_EQ(unique.size(), static_cast<std::size_t>(kThreads * kPerThread));
}

TEST(HybridClockTest, ObserveMovesPastRemoteTimestamps) {
    shardcache::HybridClock clock;
    uint64_t local = clock.now();
    uint64_t far_future = local + (uint64_t{10'000} << shardcache::HybridClock::kLogicalBits);
    clock.observe(far_future);
    EXPECT_GT(clock.now(), far_future);
}

TEST(VersioningTest, StaleWritesAreIgnored) {
    shardcache::Cache cache(100);
    EXPECT_TRUE(cache.apply("k", VersionedValue{"v2", 20}));
    EXPECT_FALSE(cache.apply("k", VersionedValue{"v1", 10}));
    EXPECT_EQ(cache.get("k").value_or(""), "v2");

    EXPECT_TRUE(cache.apply("k", VersionedValue{"v3", 30}));
    EXPECT_EQ(cache.get("k").value_or(""), "v3");
    EXPECT_EQ(cache.get_versioned("k")->version, 30u);
}

TEST(VersioningTest, ReorderedReplicationConverges) {
    // Two replicas receive the same writes in opposite orders.
    shardcache::Cache a(100), b(100);
    std::vector<VersionedValue> writes = {{"x", 5}, {"y", 9}, {"z", 7}};
    for (const auto& w : writes) a.apply("k", w);
    for (auto it = writes.rbegin(); it != writes.rend(); ++it) b.apply("k", *it);
    EXPECT_EQ(a.get("k"), b.get("k"));
    EXPECT_EQ(a.get("k").value_or(""), "y");
}

TEST(VersioningTest, TombstonePreventsResurrection) {
    shardcache::Cache cache(100);
    cache.apply("k", VersionedValue{"v1", 10});
    cache.apply("k", VersionedValue{"", 20, true});
    EXPECT_FALSE(cache.get("k").has_value());
    EXPECT_FALSE(cache.contains("k"));
    EXPECT_EQ(cache.pttl("k"), -2);
    EXPECT_EQ(cache.size(), 0u);

    // The write the delete superseded arrives late: it must not come back.
    EXPECT_FALSE(cache.apply("k", VersionedValue{"v1", 10}));
    EXPECT_FALSE(cache.get("k").has_value());

    // A newer write does recreate the key.
    EXPECT_TRUE(cache.apply("k", VersionedValue{"v3", 30}));
    EXPECT_EQ(cache.get("k").value_or(""), "v3");
    EXPECT_EQ(cache.size(), 1u);
}

TEST(VersioningTest, DeleteBeforeWriteIsOrdered) {
    shardcache::Cache cache(100);
    EXPECT_TRUE(cache.apply("k", VersionedValue{"", 20, true}));
    EXPECT_FALSE(cache.apply("k", VersionedValue{"old", 10}));
    EXPECT_FALSE(cache.get("k").has_value());
    auto stored = cache.get_versioned("k");
    ASSERT_TRUE(stored.has_value());
    EXPECT_TRUE(stored->tombstone);
}

TEST(VersioningTest, TiesResolveDeterministically) {
    shardcache::Cache a(100), b(100);
    a.apply("k", VersionedValue{"apple", 5});
    a.apply("k", VersionedValue{"banana", 5});
    b.apply("k", VersionedValue{"banana", 5});
    b.apply("k", VersionedValue{"apple", 5});
    EXPECT_EQ(a.get("k"), b.get("k"));

    a.apply("k", VersionedValue{"", 5, true});
    EXPECT_FALSE(a.get("k").has_value()); // deletes win ties
}

TEST(VersioningTest, TombstonesExpire) {
    shardcache::Cache cache(100, std::chrono::milliseconds(50));
    cache.apply("k", VersionedValue{"", 20, true});
    std::this_thread::sleep_for(std::chrono::milliseconds(80));
    cache.purge_expired();
    EXPECT_FALSE(cache.get_versioned("k").has_value());
    // Once the tombstone is gone an old write can land again; this window is
    // why tombstone_ttl must exceed the longest replication delay.
    EXPECT_TRUE(cache.apply("k", VersionedValue{"old", 10}));
}

TEST(VersioningTest, VersionedTtlIsPreserved) {
    shardcache::ShardedCache cache(1000, 4);
    cache.apply("k", VersionedValue{"v", 1, false, std::chrono::milliseconds(5000)});
    auto ttl = cache.pttl("k");
    EXPECT_GT(ttl, 4000);
    EXPECT_LE(ttl, 5000);
    auto v = cache.get_versioned("k");
    ASSERT_TRUE(v.has_value());
    ASSERT_TRUE(v->ttl.has_value());
    EXPECT_GT(v->ttl->count(), 4000);
}
