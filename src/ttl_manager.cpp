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
            std::this_thread::sleep_for(interval_);
            if (!running_.load()) break;
            if (callback_) {
                callback_();
            }
        }
    });
}

void TTLManager::stop() {
    if (!running_.exchange(false)) return;
    if (worker_thread_.joinable()) {
        worker_thread_.join();
    }
}

} // namespace shardcache
