#include "shardcache/cache.hpp"
#include "shardcache/sharded_cache.hpp"

#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <algorithm>
#include <numeric>
#include <iomanip>
#include <string>

using namespace shardcache;

void run_benchmark_workload(
    const std::string& name,
    std::size_t num_threads,
    std::size_t ops_per_thread,
    int set_percentage,
    bool use_sharded
) {
    Cache single_cache(100000);
    ShardedCache sharded_cache(100000, 16);

    // Pre-populate data to simulate realistic cache hit rates
    for (int i = 0; i < 10000; ++i) {
        std::string k = "key_" + std::to_string(i);
        std::string v = "val_" + std::to_string(i);
        if (use_sharded) {
            sharded_cache.set(k, v);
        } else {
            single_cache.set(k, v);
        }
    }

    std::vector<std::thread> threads;
    threads.reserve(num_threads);

    std::vector<double> latencies;
    std::mutex lat_mutex;

    auto start_time = std::chrono::high_resolution_clock::now();

    for (std::size_t t = 0; t < num_threads; ++t) {
        threads.emplace_back([&, t]() {
            std::vector<double> local_latencies;
            local_latencies.reserve(ops_per_thread);

            for (std::size_t i = 0; i < ops_per_thread; ++i) {
                std::string k = "key_" + std::to_string((t * ops_per_thread + i) % 10000);
                std::string v = "val_" + std::to_string(i);

                bool is_set = ((i % 100) < static_cast<std::size_t>(set_percentage));

                auto t0 = std::chrono::high_resolution_clock::now();

                if (use_sharded) {
                    if (is_set) {
                        sharded_cache.set(k, v);
                    } else {
                        (void)sharded_cache.get(k);
                    }
                } else {
                    if (is_set) {
                        single_cache.set(k, v);
                    } else {
                        (void)single_cache.get(k);
                    }
                }

                auto t1 = std::chrono::high_resolution_clock::now();
                double micro = std::chrono::duration<double, std::micro>(t1 - t0).count();
                local_latencies.push_back(micro);
            }

            std::unique_lock lk(lat_mutex);
            latencies.insert(latencies.end(), local_latencies.begin(), local_latencies.end());
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    auto end_time = std::chrono::high_resolution_clock::now();
    double total_sec = std::chrono::duration<double>(end_time - start_time).count();

    std::sort(latencies.begin(), latencies.end());

    std::size_t total_ops = num_threads * ops_per_thread;
    double ops_per_sec = total_ops / total_sec;

    double p50 = latencies[static_cast<std::size_t>(total_ops * 0.50)];
    double p95 = latencies[static_cast<std::size_t>(total_ops * 0.95)];
    double p99 = latencies[static_cast<std::size_t>(total_ops * 0.99)];

    std::cout << "| " << std::setw(25) << std::left << name
              << " | " << std::setw(8) << num_threads
              << " | " << std::setw(12) << static_cast<uint64_t>(ops_per_sec)
              << " | " << std::setw(8) << std::fixed << std::setprecision(1) << p50 << " µs"
              << " | " << std::setw(8) << std::fixed << std::setprecision(1) << p95 << " µs"
              << " | " << std::setw(8) << std::fixed << std::setprecision(1) << p99 << " µs"
              << " |" << std::endl;
}

int main() {
    std::cout << "=========================================================================================" << std::endl;
    std::cout << "                         SHARDCACHE WORKLOAD BENCHMARK SUITE                             " << std::endl;
    std::cout << "=========================================================================================" << std::endl;
    std::cout << "| Target Implementation     | Threads  | Ops/sec      | p50      | p95      | p99      |" << std::endl;
    std::cout << "|---------------------------|----------|--------------|----------|----------|----------|" << std::endl;

    std::cout << "--- Typical Cache Workload (80% GET / 20% SET) ---" << std::endl;
    for (std::size_t threads : {1, 4, 8, 16}) {
        run_benchmark_workload("Single Lock Cache", threads, 30000, 20, false);
        run_benchmark_workload("16-Shard Cache", threads, 30000, 20, true);
    }

    std::cout << "--- Read-Heavy Workload (95% GET / 5% SET) ---" << std::endl;
    for (std::size_t threads : {4, 16}) {
        run_benchmark_workload("Single Lock Cache", threads, 30000, 5, false);
        run_benchmark_workload("16-Shard Cache", threads, 30000, 5, true);
    }

    std::cout << "=========================================================================================" << std::endl;
    return 0;
}
