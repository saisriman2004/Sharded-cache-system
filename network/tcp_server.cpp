#include "tcp_server.hpp"
#include "shardcache/logger.hpp"

namespace shardcache::network {

TCPServer::TCPServer(
    boost::asio::io_context& io_context,
    uint16_t port,
    Cluster& cluster,
    IoContextPool* connection_pool,
    std::size_t rpc_threads
)
    : acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)),
      connection_pool_(connection_pool),
      handler_(std::make_shared<CommandHandler>(cluster, rpc_threads)) {
    Logger::instance().info("TCPServer listening on port " + std::to_string(port));
    start_accept();
}

void TCPServer::start_accept() {
    auto& target = connection_pool_ ? connection_pool_->next() : acceptor_.get_executor().context();
    acceptor_.async_accept(
        static_cast<boost::asio::io_context&>(target),
        [this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
            if (!ec) {
                std::make_shared<ClientSession>(std::move(socket), handler_)->start();
            } else {
                Logger::instance().error("Accept error: " + ec.message());
            }
            start_accept();
        }
    );
}

} // namespace shardcache::network
