#include <gtest/gtest.h>
#include "shardcache/ttl_manager.hpp"
#include <atomic>

TEST(TTLManagerTest, BackgroundPurgeTrigger) {
    std::atomic<int> purge_calls{0};
    shardcache::TTLManager manager([&purge_calls]() {
        purge_calls.fetch_add(1);
    }, std::chrono::milliseconds(50));

    manager.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(180));
    manager.stop();

    EXPECT_GE(purge_calls.load(), 2);
}
