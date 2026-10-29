#include <gtest/gtest.h>
#include "shardcache/snapshot.hpp"

#include <cstdio>
#include <fstream>
#include <thread>

using shardcache::VersionedValue;

class SnapshotTest : public ::testing::Test {
protected:
    void TearDown() override { std::remove(path_.c_str()); }
    std::string path_ = ::testing::TempDir() + "shardcache_snapshot_test.bin";
};

TEST_F(SnapshotTest, RoundTripPreservesValuesVersionsTtlsAndTombstones) {
    shardcache::ShardedCache source(1000, 4);
    source.apply("plain", VersionedValue{"hello world", 10});
    source.apply("binary", VersionedValue{std::string("a\0b\r\nc", 6), 11});
    source.apply("ttl", VersionedValue{"temp", 12, false, std::chrono::milliseconds(60000)});
    source.apply("deleted", VersionedValue{"", 13, true});

    auto saved = shardcache::save_snapshot(source, path_);
    ASSERT_TRUE(saved.has_value());
    EXPECT_EQ(saved->entries, 4u);

    shardcache::ShardedCache restored(1000, 4);
    auto loaded = shardcache::load_snapshot(restored, path_);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->entries, 4u);

    EXPECT_EQ(restored.get("plain").value_or(""), "hello world");
    EXPECT_EQ(restored.get("binary").value_or(""), std::string("a\0b\r\nc", 6));
    EXPECT_EQ(restored.get_versioned("plain")->version, 10u);
    auto ttl = restored.pttl("ttl");
    EXPECT_GT(ttl, 55000);
    EXPECT_LE(ttl, 60000);
    EXPECT_FALSE(restored.get("deleted").has_value());
    EXPECT_TRUE(restored.get_versioned("deleted")->tombstone);
}

TEST_F(SnapshotTest, RestoreNeverOverwritesNewerData) {
    shardcache::ShardedCache old_state(100, 2);
    old_state.apply("k", VersionedValue{"stale", 5});
    ASSERT_TRUE(shardcache::save_snapshot(old_state, path_));

    shardcache::ShardedCache live(100, 2);
    live.apply("k", VersionedValue{"fresh", 9});
    ASSERT_TRUE(shardcache::load_snapshot(live, path_));
    EXPECT_EQ(live.get("k").value_or(""), "fresh");
}

TEST_F(SnapshotTest, EntriesThatExpiredWhileDownAreSkipped) {
    shardcache::ShardedCache source(100, 2);
    source.apply("short", VersionedValue{"v", 1, false, std::chrono::milliseconds(30)});
    source.apply("long", VersionedValue{"v", 2});
    ASSERT_TRUE(shardcache::save_snapshot(source, path_));
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    shardcache::ShardedCache restored(100, 2);
    auto loaded = shardcache::load_snapshot(restored, path_);
    ASSERT_TRUE(loaded.has_value());
    EXPECT_EQ(loaded->entries, 1u);
    EXPECT_FALSE(restored.get("short").has_value());
    EXPECT_TRUE(restored.get("long").has_value());
}

TEST_F(SnapshotTest, CorruptOrTruncatedFilesAreRejected) {
    shardcache::ShardedCache source(100, 2);
    for (int i = 0; i < 50; ++i) source.apply("k" + std::to_string(i), VersionedValue{"v", 1});
    ASSERT_TRUE(shardcache::save_snapshot(source, path_));

    std::string bytes;
    {
        std::ifstream in(path_, std::ios::binary);
        bytes.assign(std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>());
    }
    auto write = [&](const std::string& content) {
        std::ofstream out(path_, std::ios::binary | std::ios::trunc);
        out << content;
    };

    std::string error;
    shardcache::ShardedCache target(100, 2);

    std::string flipped = bytes;
    flipped[bytes.size() / 2] ^= 0x40;
    write(flipped);
    EXPECT_FALSE(shardcache::load_snapshot(target, path_, &error).has_value());
    EXPECT_NE(error.find("checksum"), std::string::npos);

    write(bytes.substr(0, bytes.size() - 20));
    EXPECT_FALSE(shardcache::load_snapshot(target, path_, &error).has_value());

    write("not a snapshot at all");
    EXPECT_FALSE(shardcache::load_snapshot(target, path_, &error).has_value());

    EXPECT_EQ(target.size(), 0u) << "a rejected file must not be partially applied";
    std::remove(path_.c_str());
    EXPECT_FALSE(shardcache::load_snapshot(target, path_, &error).has_value());
}
