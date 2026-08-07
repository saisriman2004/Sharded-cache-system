#ifndef SHARDCACHE_TCP_SERVER_HPP
#define SHARDCACHE_TCP_SERVER_HPP

#include "session.hpp"
#include "shardcache/sharded_cache.hpp"

#include <boost/asio.hpp>
#include <memory>
#include <string>

namespace shardcache::network {

class TCPServer {
public:
    TCPServer(boost::asio::io_context& io_context, uint16_t port, ShardedCache& cache);
    ~TCPServer() = default;

    void start_accept();

private:
    boost::asio::io_context& io_context_;
    boost::asio::ip::tcp::acceptor acceptor_;
    ShardedCache& cache_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_TCP_SERVER_HPP
