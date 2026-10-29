#include "peer_pool.hpp"

namespace shardcache::network {

namespace {
std::string make_address(const std::string& host, uint16_t port) {
    return host + ":" + std::to_string(port);
}
} // namespace

PeerPool::PeerPool(
    std::size_t max_idle_per_peer,
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds io_timeout
)
    : max_idle_per_peer_(max_idle_per_peer),
      connect_timeout_(connect_timeout),
      io_timeout_(io_timeout) {}

PeerPool::Lease::~Lease() {
    if (pool_ && client_ && client_->is_connected()) {
        pool_->release(address_, std::move(client_));
    }
}

std::optional<PeerPool::Lease> PeerPool::acquire(const std::string& host, uint16_t port) {
    std::string address = make_address(host, port);
    {
        std::lock_guard lock(mutex_);
        auto it = idle_.find(address);
        if (it != idle_.end() && !it->second.empty()) {
            auto client = std::move(it->second.back());
            it->second.pop_back();
            return Lease(this, std::move(address), std::move(client));
        }
    }

    auto client = std::make_unique<Client>(host, port, connect_timeout_, io_timeout_);
    if (!client->connect()) {
        return std::nullopt;
    }
    return Lease(this, std::move(address), std::move(client));
}

void PeerPool::drop(const std::string& host, uint16_t port) {
    std::vector<std::unique_ptr<Client>> closing;
    {
        std::lock_guard lock(mutex_);
        auto it = idle_.find(make_address(host, port));
        if (it != idle_.end()) {
            closing.swap(it->second);
        }
    }
    // Clients disconnect in their destructors, outside the lock.
}

void PeerPool::release(const std::string& address, std::unique_ptr<Client> client) {
    std::lock_guard lock(mutex_);
    auto& idle = idle_[address];
    if (idle.size() < max_idle_per_peer_) {
        idle.push_back(std::move(client));
    }
}

} // namespace shardcache::network
