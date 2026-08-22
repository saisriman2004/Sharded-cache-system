#ifndef SHARDCACHE_SINGLE_FLIGHT_HPP
#define SHARDCACHE_SINGLE_FLIGHT_HPP

#include <string>
#include <functional>
#include <unordered_map>
#include <mutex>
#include <condition_variable>
#include <memory>

#include <exception>

namespace shardcache {

class SingleFlight {
public:
    SingleFlight() = default;
    ~SingleFlight() = default;

    using FetchFn = std::function<std::string()>;

    std::string do_call(const std::string& key, FetchFn fn);

private:
    struct Call {
        std::mutex mutex;
        std::condition_variable cv;
        bool done{false};
        std::string val;
        std::exception_ptr exc{nullptr};
    };

    std::mutex map_mutex_;
    std::unordered_map<std::string, std::shared_ptr<Call>> calls_;
};

} // namespace shardcache

#endif // SHARDCACHE_SINGLE_FLIGHT_HPP
