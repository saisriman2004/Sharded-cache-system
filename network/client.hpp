#ifndef SHARDCACHE_CLIENT_HPP
#define SHARDCACHE_CLIENT_HPP

#include <string>
#include <optional>
#include <vector>
#include <cstdint>
#include <boost/asio.hpp>

#include "network/request_result.hpp"
#include "protocol/resp.hpp"

namespace shardcache::network {

// Blocking RESP client with connect and per-request I/O timeouts. Used for
// inter-node RPCs, health checks and tests.
class Client {
public:
    Client(
        const std::string& host,
        uint16_t port,
        std::chrono::milliseconds connect_timeout = std::chrono::milliseconds(2000),
        std::chrono::milliseconds io_timeout = std::chrono::milliseconds(3000)
    );
    ~Client();

    bool connect();
    void disconnect();
    bool is_connected() const { return connected_; }

    // Sends an arbitrary command and returns the decoded reply.
    RequestResult<protocol::RespValue> command(const std::vector<std::string>& args);

    RequestResult<void> set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl_ms = std::nullopt);
    RequestResult<std::string> get(const std::string& key);
    RequestResult<void> remove(const std::string& key);
    RequestResult<void> expire(const std::string& key, uint64_t ttl_ms);
    // Remaining TTL in milliseconds; -1 when the key has no expiry, NotFound when absent.
    RequestResult<int64_t> pttl(const std::string& key);
    bool ping();

    // Internal replication methods — these send REPL_* commands so that
    // the receiving node writes directly to its local cache without
    // routing through its own cluster hash ring.
    RequestResult<void> replica_set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl_ms = std::nullopt);
    RequestResult<void> replica_delete(const std::string& key);
    RequestResult<void> replica_expire(const std::string& key, uint64_t ttl_ms);

private:
    RequestResult<void> expect_ok(const std::vector<std::string>& args);

    std::string host_;
    uint16_t port_;
    std::chrono::milliseconds connect_timeout_;
    std::chrono::milliseconds io_timeout_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_;
    std::string rx_;
    bool connected_{false};
};

} // namespace shardcache::network

#endif // SHARDCACHE_CLIENT_HPP
