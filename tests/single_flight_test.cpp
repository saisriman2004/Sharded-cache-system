#include <gtest/gtest.h>
#include "shardcache/single_flight.hpp"
#include <atomic>
#include <thread>
#include <vector>

TEST(SingleFlightTest, CoalesceDuplicateRequests) {
    shardcache::SingleFlight sf;
    std::atomic<int> fetch_count{0};

    auto expensive_fetch = [&fetch_count]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        fetch_count.fetch_add(1);
        return "fetched_data";
    };

    constexpr int num_threads = 20;
    std::vector<std::thread> threads;
    std::vector<std::string> results(num_threads);

    for (int i = 0; i < num_threads; ++i) {
        threads.emplace_back([&sf, &expensive_fetch, &results, i]() {
            results[i] = sf.do_call("product:123", expensive_fetch);
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(fetch_count.load(), 1);
    for (int i = 0; i < num_threads; ++i) {
        EXPECT_EQ(results[i], "fetched_data");
    }
}
