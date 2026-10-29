#include "io_context_pool.hpp"

namespace shardcache::network {

IoContextPool::IoContextPool(std::size_t size) {
    if (size == 0) size = 1;
    contexts_.reserve(size);
    for (std::size_t i = 0; i < size; ++i) {
        contexts_.push_back(std::make_unique<boost::asio::io_context>(1));
        guards_.push_back(boost::asio::make_work_guard(*contexts_.back()));
    }
}

IoContextPool::~IoContextPool() {
    stop();
    join();
}

boost::asio::io_context& IoContextPool::next() {
    return *contexts_[next_.fetch_add(1, std::memory_order_relaxed) % contexts_.size()];
}

void IoContextPool::start() {
    for (auto& ctx : contexts_) {
        threads_.emplace_back([c = ctx.get()]() { c->run(); });
    }
}

void IoContextPool::stop() {
    guards_.clear();
    for (auto& ctx : contexts_) {
        ctx->stop();
    }
}

void IoContextPool::join() {
    for (auto& t : threads_) {
        if (t.joinable()) t.join();
    }
    threads_.clear();
}

} // namespace shardcache::network
