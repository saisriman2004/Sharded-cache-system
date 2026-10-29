#include "metrics_server.hpp"
#include "shardcache/logger.hpp"

#include <memory>
#include <sstream>

namespace shardcache::network {

namespace {

using protocol::CommandType;

const char* command_label(CommandType t) {
    switch (t) {
        case CommandType::Get: return "get";
        case CommandType::Set: return "set";
        case CommandType::Delete: return "del";
        case CommandType::Exists: return "exists";
        case CommandType::Expire: return "expire";
        case CommandType::Ttl: return "ttl";
        case CommandType::Pttl: return "pttl";
        case CommandType::Stats: return "info";
        case CommandType::Ping: return "ping";
        case CommandType::Echo: return "echo";
        case CommandType::DbSize: return "dbsize";
        case CommandType::FlushAll: return "flushall";
        case CommandType::CommandDocs: return "command";
        case CommandType::Config: return "config";
        case CommandType::Select: return "select";
        case CommandType::Client: return "client";
        case CommandType::Quit: return "quit";
        case CommandType::ReplSet: return "repl_set";
        case CommandType::ReplDelete: return "repl_del";
        case CommandType::ReplGet: return "repl_get";
        case CommandType::ClusterGossip: return "cluster_gossip";
        case CommandType::ClusterNodes: return "cluster_nodes";
        case CommandType::Unknown: return "unknown";
    }
    return "unknown";
}

void metric(std::ostringstream& out, const char* name, const char* type, const char* help) {
    out << "# HELP " << name << ' ' << help << "\n# TYPE " << name << ' ' << type << '\n';
}

} // namespace

std::string render_metrics(CommandHandler& handler) {
    auto& cluster = handler.cluster();
    const auto cache = cluster.local_cache().totals();
    const auto stats = cluster.stats();
    std::ostringstream out;

    metric(out, "shardcache_commands_total", "counter", "Commands executed, by command.");
    for (std::size_t i = 0; i < CommandHandler::kCommandTypes; ++i) {
        auto t = static_cast<CommandType>(i);
        if (uint64_t n = handler.metrics(t).calls.value(); n > 0) {
            out << "shardcache_commands_total{cmd=\"" << command_label(t) << "\"} " << n << '\n';
        }
    }
    metric(out, "shardcache_command_errors_total", "counter", "Commands answered with an error reply.");
    for (std::size_t i = 0; i < CommandHandler::kCommandTypes; ++i) {
        auto t = static_cast<CommandType>(i);
        if (uint64_t n = handler.metrics(t).errors.value(); n > 0) {
            out << "shardcache_command_errors_total{cmd=\"" << command_label(t) << "\"} " << n << '\n';
        }
    }
    metric(out, "shardcache_command_duration_seconds", "histogram", "Command execution time on this node.");
    for (std::size_t i = 0; i < CommandHandler::kCommandTypes; ++i) {
        auto t = static_cast<CommandType>(i);
        const auto& h = handler.metrics(t).latency;
        if (h.count() == 0) continue;
        const char* label = command_label(t);
        uint64_t cumulative = 0;
        for (std::size_t b = 0; b < LatencyHistogram::kBuckets; ++b) {
            cumulative += h.bucket(b);
            out << "shardcache_command_duration_seconds_bucket{cmd=\"" << label << "\",le=\"" << LatencyHistogram::bound(b)
                << "\"} " << cumulative << '\n';
        }
        cumulative += h.bucket(LatencyHistogram::kBuckets);
        out << "shardcache_command_duration_seconds_bucket{cmd=\"" << label << "\",le=\"+Inf\"} " << cumulative << '\n';
        out << "shardcache_command_duration_seconds_sum{cmd=\"" << label << "\"} " << h.sum_seconds() << '\n';
        out << "shardcache_command_duration_seconds_count{cmd=\"" << label << "\"} " << cumulative << '\n';
    }

    auto counter = [&](const char* name, const char* help, uint64_t v) {
        metric(out, name, "counter", help);
        out << name << ' ' << v << '\n';
    };
    auto gauge = [&](const char* name, const char* help, double v) {
        metric(out, name, "gauge", help);
        out << name << ' ' << v << '\n';
    };

    counter("shardcache_cache_hits_total", "Local cache lookups that found a live key.", cache.hits);
    counter("shardcache_cache_misses_total", "Local cache lookups that found nothing.", cache.misses);
    counter("shardcache_cache_evictions_total", "Entries evicted by the LRU policy.", cache.evictions);
    counter("shardcache_cache_expirations_total", "Entries removed because their TTL elapsed.", cache.expirations);
    gauge("shardcache_cache_keys", "Live keys stored on this node.", static_cast<double>(cluster.local_cache().size()));
    gauge("shardcache_cache_capacity", "Maximum entries this node stores.", static_cast<double>(cluster.local_cache().capacity()));

    counter("shardcache_read_quorum_failures_total", "Reads that could not reach R replicas.", stats.read_quorum_failures);
    counter("shardcache_write_quorum_failures_total", "Writes that could not reach W replicas.", stats.write_quorum_failures);
    counter("shardcache_read_repairs_total", "Stale replicas repaired by reads.", stats.read_repairs);
    counter("shardcache_hints_delivered_total", "Missed writes delivered by hinted handoff.", stats.hints_delivered);
    counter("shardcache_hints_dropped_total", "Hints discarded because a queue was full.", stats.hints_dropped);
    counter("shardcache_rebalance_keys_moved_total", "Keys streamed to new owners after membership changes.", stats.keys_moved);
    gauge("shardcache_hints_pending", "Missed writes queued for unreachable replicas.", static_cast<double>(stats.hints_pending));
    gauge("shardcache_cluster_nodes", "Nodes in this node's ring.", static_cast<double>(stats.ring_nodes));
    gauge("shardcache_cluster_healthy_nodes", "Ring nodes currently passing health checks.", static_cast<double>(stats.healthy_nodes));

    counter("shardcache_connections_total", "Client connections accepted.", handler.connections_total());
    gauge("shardcache_connections_active", "Client connections currently open.", static_cast<double>(handler.connections_active()));
    return out.str();
}

namespace {

class HttpConnection : public std::enable_shared_from_this<HttpConnection> {
public:
    HttpConnection(boost::asio::ip::tcp::socket socket, CommandHandler& handler)
        : socket_(std::move(socket)), handler_(handler) {}

    void start() {
        auto self = shared_from_this();
        boost::asio::async_read_until(socket_, request_, "\r\n\r\n",
            [this, self](boost::system::error_code ec, std::size_t) {
                if (ec) return;
                std::istream is(&request_);
                std::string method, path;
                is >> method >> path;
                respond(method, path);
            });
    }

private:
    void respond(const std::string& method, const std::string& path) {
        std::string status = "200 OK";
        std::string type = "text/plain; version=0.0.4; charset=utf-8";
        std::string body;
        if (method != "GET") {
            status = "405 Method Not Allowed";
            body = "method not allowed\n";
        } else if (path == "/metrics") {
            body = render_metrics(handler_);
        } else if (path == "/health" || path == "/healthz") {
            type = "text/plain; charset=utf-8";
            body = "ok\n";
        } else {
            status = "404 Not Found";
            body = "not found\n";
        }
        response_ = "HTTP/1.1 " + status + "\r\nContent-Type: " + type + "\r\nContent-Length: " +
                    std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n" + body;
        auto self = shared_from_this();
        boost::asio::async_write(socket_, boost::asio::buffer(response_),
            [this, self](boost::system::error_code, std::size_t) {
                boost::system::error_code ignored;
                socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ignored);
            });
    }

    boost::asio::ip::tcp::socket socket_;
    CommandHandler& handler_;
    boost::asio::streambuf request_{8192};  // caps the request size
    std::string response_;
};

} // namespace

MetricsServer::MetricsServer(boost::asio::io_context& io_context, uint16_t port, CommandHandler& handler)
    : acceptor_(io_context, boost::asio::ip::tcp::endpoint(boost::asio::ip::tcp::v4(), port)), handler_(handler) {
    Logger::instance().info("Metrics endpoint on http://0.0.0.0:" + std::to_string(port) + "/metrics");
    accept();
}

void MetricsServer::accept() {
    acceptor_.async_accept([this](boost::system::error_code ec, boost::asio::ip::tcp::socket socket) {
        if (!ec) {
            std::make_shared<HttpConnection>(std::move(socket), handler_)->start();
        }
        accept();
    });
}

} // namespace shardcache::network
