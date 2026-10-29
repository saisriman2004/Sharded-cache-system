#include "shardcache/ttl_manager.hpp"

namespace shardcache {

TTLManager::TTLManager(PurgeCallback cb, std::chrono::milliseconds interval)
    : callback_(std::move(cb)), interval_(interval) {}

TTLManager::~TTLManager() {
    stop();
}

void TTLManager::start() {
    if (running_.exchange(true)) return;
    worker_thread_ = std::thread([this]() {
        while (running_.load()) {
            {
                std::unique_lock lock(wake_mutex_);
                wake_cv_.wait_for(lock, interval_, [this]() { return !running_.load(); });
            }
            if (!running_.load()) break;
            if (callback_) {
                callback_();
            }
        }
    });
}

void TTLManager::stop() {
    if (!running_.exchange(false)) return;
    {
        std::lock_guard lock(wake_mutex_);
    }
    wake_cv_.notify_all();
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

} // namespace shardcache
