#include "shardcache/lru.hpp"

namespace shardcache {

LRUTracker::LRUTracker(std::size_t capacity)
    : capacity_(capacity) {}

void LRUTracker::touch(const std::string& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        list_.splice(list_.begin(), list_, it->second);
    } else {
        list_.push_front(key);
        map_[key] = list_.begin();
    }
}

std::optional<std::string> LRUTracker::evict_lru() {
    if (list_.empty()) return std::nullopt;
    std::string tail_key = list_.back();
    map_.erase(tail_key);
    list_.pop_back();
    return tail_key;
}

void LRUTracker::remove(const std::string& key) {
    auto it = map_.find(key);
    if (it != map_.end()) {
        list_.erase(it->second);
        map_.erase(it);
    }
}

void LRUTracker::clear() {
    list_.clear();
    map_.clear();
}

} // namespace shardcache
