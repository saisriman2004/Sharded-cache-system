#ifndef SHARDCACHE_LRU_HPP
#define SHARDCACHE_LRU_HPP

#include <list>
#include <unordered_map>
#include <string>
#include <optional>

namespace shardcache {

class LRUTracker {
public:
    explicit LRUTracker(std::size_t capacity = 10000);
    ~LRUTracker() = default;

    void touch(const std::string& key);
    std::optional<std::string> evict_lru();
    void remove(const std::string& key);
    void clear();

    std::size_t capacity() const { return capacity_; }
    std::size_t size() const { return map_.size(); }

private:
    std::size_t capacity_;
    std::list<std::string> list_;
    std::unordered_map<std::string, std::list<std::string>::iterator> map_;
};

} // namespace shardcache

#endif // SHARDCACHE_LRU_HPP
