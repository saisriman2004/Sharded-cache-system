#ifndef SHARDCACHE_TTL_MANAGER_HPP
#define SHARDCACHE_TTL_MANAGER_HPP

#include <thread>
#include <condition_variable>
#include <mutex>
#include <atomic>
#include <chrono>
#include <functional>

namespace shardcache {

class TTLManager {
public:
    using PurgeCallback = std::function<void()>;

    explicit TTLManager(PurgeCallback cb, std::chrono::milliseconds interval = std::chrono::milliseconds(500));
    ~TTLManager();

    void start();
    void stop();

private:
    PurgeCallback callback_;
    std::chrono::milliseconds interval_;
    std::atomic<bool> running_{false};
    std::mutex wake_mutex_;
    std::condition_variable wake_cv_;  // lets stop() interrupt the sleep
    std::thread worker_thread_;
};

} // namespace shardcache

#endif // SHARDCACHE_TTL_MANAGER_HPP
