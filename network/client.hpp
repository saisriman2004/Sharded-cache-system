#ifndef SHARDCACHE_CLIENT_HPP
#define SHARDCACHE_CLIENT_HPP

#include <string>
#include <optional>
#include <cstdint>
#include <boost/asio.hpp>

#include "network/request_result.hpp"

namespace shardcache::network {

class Client {
public:
    Client(const std::string& host, uint16_t port);
    ~Client();

    bool connect();
    void disconnect();

    RequestResult<void> set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl = std::nullopt);
    RequestResult<std::string> get(const std::string& key);
    RequestResult<void> remove(const std::string& key);
    RequestResult<void> expire(const std::string& key, uint64_t ttl_seconds);
    RequestResult<int64_t> ttl(const std::string& key);
    bool ping();

    // Internal replication methods — these send REPL_* commands so that
    // the receiving node writes directly to its local cache without
    // routing through its own cluster hash ring.
    RequestResult<void> replica_set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl = std::nullopt);
    RequestResult<void> replica_delete(const std::string& key);
    RequestResult<void> replica_expire(const std::string& key, uint64_t ttl_seconds);

private:
    RequestResult<std::string> send_raw(const std::string& command);

    std::string host_;
    uint16_t port_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_;
    bool connected_{false};
};

} // namespace shardcache::network

#endif // SHARDCACHE_CLIENT_HPP
