#ifndef SHARDCACHE_IO_CONTEXT_POOL_HPP
#define SHARDCACHE_IO_CONTEXT_POOL_HPP

#include <boost/asio.hpp>

#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace shardcache::network {

// One io_context per thread (typically one per core). Each accepted
// connection is pinned to a single context, so its handlers never migrate
// between threads and never contend on a shared scheduler queue.
class IoContextPool {
public:
    explicit IoContextPool(std::size_t size);
    ~IoContextPool();

    IoContextPool(const IoContextPool&) = delete;
    IoContextPool& operator=(const IoContextPool&) = delete;

    // Round-robin choice of context for the next connection.
    boost::asio::io_context& next();

    void start();
    void stop();
    void join();

    std::size_t size() const { return contexts_.size(); }

private:
    using WorkGuard = boost::asio::executor_work_guard<boost::asio::io_context::executor_type>;

    std::vector<std::unique_ptr<boost::asio::io_context>> contexts_;
    std::vector<WorkGuard> guards_;
    std::vector<std::thread> threads_;
    std::atomic<std::size_t> next_{0};
};

} // namespace shardcache::network

#endif // SHARDCACHE_IO_CONTEXT_POOL_HPP
