#ifndef SHARDCACHE_METRICS_SERVER_HPP
#define SHARDCACHE_METRICS_SERVER_HPP

#include "command_handler.hpp"

#include <boost/asio.hpp>
#include <functional>
#include <string>

namespace shardcache::network {

// Renders every node metric in the Prometheus text exposition format.
std::string render_metrics(CommandHandler& handler);

// Minimal HTTP/1.1 server for operators: GET /metrics (Prometheus) and
// GET /health (liveness/readiness probe). One request per connection.
class MetricsServer {
public:
    MetricsServer(boost::asio::io_context& io_context, uint16_t port, CommandHandler& handler);

private:
    void accept();

    boost::asio::ip::tcp::acceptor acceptor_;
    CommandHandler& handler_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_METRICS_SERVER_HPP
