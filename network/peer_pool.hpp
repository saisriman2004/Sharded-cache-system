#ifndef SHARDCACHE_PEER_POOL_HPP
#define SHARDCACHE_PEER_POOL_HPP

#include "network/client.hpp"

#include <chrono>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace shardcache::network {

// Pool of reusable connections to peer nodes. Opening a TCP connection per
// forwarded or replicated request costs a round trip plus a socket per call;
// the pool keeps idle connections per peer and hands them out exclusively.
class PeerPool {
public:
    explicit PeerPool(
        std::size_t max_idle_per_peer = 64,
        std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(500),
        std::chrono::milliseconds io_timeout = std::chrono::milliseconds(1000)
    );

    PeerPool(const PeerPool&) = delete;
    PeerPool& operator=(const PeerPool&) = delete;

    // Exclusive use of one connection; returned to the pool on destruction
    // if it is still healthy.
    class Lease {
    public:
        Lease(PeerPool* pool, std::string address, std::unique_ptr<Client> client)
            : pool_(pool), address_(std::move(address)), client_(std::move(client)) {}
        Lease(Lease&&) noexcept = default;
        Lease& operator=(Lease&&) noexcept = default;
        ~Lease();

        Client* operator->() { return client_.get(); }
        Client& operator*() { return *client_; }

    private:
        PeerPool* pool_;
        std::string address_;
        std::unique_ptr<Client> client_;
    };

    // Returns a connected client, or nullopt if the peer is unreachable.
    std::optional<Lease> acquire(const std::string& host, uint16_t port);

    // Closes all idle connections to a peer (e.g. after it is marked down).
    void drop(const std::string& host, uint16_t port);

private:
    void release(const std::string& address, std::unique_ptr<Client> client);

    std::size_t max_idle_per_peer_;
    std::chrono::milliseconds connect_timeout_;
    std::chrono::milliseconds io_timeout_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<std::unique_ptr<Client>>> idle_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_PEER_POOL_HPP
