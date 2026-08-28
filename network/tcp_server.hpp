#ifndef SHARDCACHE_TCP_SERVER_HPP
#define SHARDCACHE_TCP_SERVER_HPP

#include "session.hpp"
#include "shardcache/cluster.hpp"

#include <boost/asio.hpp>
#include <memory>
#include <string>

namespace shardcache::network {

class TCPServer {
public:
    TCPServer(boost::asio::io_context& io_context, uint16_t port, Cluster& cluster);
    ~TCPServer() = default;

    void start_accept();

private:
    boost::asio::ip::tcp::acceptor acceptor_;
    Cluster& cluster_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_TCP_SERVER_HPP
