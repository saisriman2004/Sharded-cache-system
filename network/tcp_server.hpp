#ifndef SHARDCACHE_TCP_SERVER_HPP
#define SHARDCACHE_TCP_SERVER_HPP

#include "session.hpp"
#include "command_handler.hpp"
#include "io_context_pool.hpp"
#include "shardcache/cluster.hpp"

#include <boost/asio.hpp>
#include <memory>
#include <string>

namespace shardcache::network {

class TCPServer {
public:
    // Connections run on `io_context` unless `connection_pool` is given, in
    // which case each one is pinned to the pool's next context.
    TCPServer(
        boost::asio::io_context& io_context,
        uint16_t port,
        Cluster& cluster,
        IoContextPool* connection_pool = nullptr,
        std::size_t rpc_threads = 64
    );
    ~TCPServer() = default;

    void start_accept();
    CommandHandler& handler() { return handler_; }

private:
    boost::asio::ip::tcp::acceptor acceptor_;
    IoContextPool* connection_pool_;
    CommandHandler handler_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_TCP_SERVER_HPP
