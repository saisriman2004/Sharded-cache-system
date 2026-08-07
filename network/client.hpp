#ifndef SHARDCACHE_CLIENT_HPP
#define SHARDCACHE_CLIENT_HPP

#include <string>
#include <optional>
#include <cstdint>
#include <boost/asio.hpp>

namespace shardcache::network {

class Client {
public:
    Client(const std::string& host, uint16_t port);
    ~Client();

    bool connect();
    void disconnect();

    bool set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl = std::nullopt);
    std::optional<std::string> get(const std::string& key);
    bool remove(const std::string& key);
    bool ping();

private:
    std::string send_raw(const std::string& command);

    std::string host_;
    uint16_t port_;
    boost::asio::io_context io_context_;
    boost::asio::ip::tcp::socket socket_;
    bool connected_{false};
};

} // namespace shardcache::network

#endif // SHARDCACHE_CLIENT_HPP
