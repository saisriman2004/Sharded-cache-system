#include "tcp_server.hpp"
#include "shardcache/logger.hpp"

namespace shardcache::network {

TCPServer::TCPServer(boost::asio::io_context& io_context, uint16_t port, ShardedCache& cache)
    : io_context_(io_context),
      acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      cache_(cache) {
    Logger::instance().info("TCPServer listening on port " + std::to_string(port));
    start_accept();
}

void TCPServer::start_accept() {
    acceptor_.async_accept(
        [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
            if (!ec) {
                std::make_shared<ClientSession>(std::move(socket), cache_)->start();
            } else {
                Logger::instance().error("Accept error: " + ec.message());
            }
            start_accept();
        }
    );
}

} // namespace shardcache::network
