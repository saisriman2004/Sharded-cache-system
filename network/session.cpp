#include "session.hpp"
#include "shardcache/logger.hpp"
#include <iostream>

namespace shardcache::network {

ClientSession::ClientSession(boost::asio::ip::tcp::socket socket, Cluster& cluster)
    : socket_(std::move(socket)), cluster_(cluster) {}

void ClientSession::start() {
    do_read();
}

void ClientSession::do_read() {
    auto self(shared_from_this());
    boost::asio::async_read_until(
        socket_,
        buffer_,
        "\r\n",
        [this, self](boost::system::error_code ec, std::size_t /*bytes_transferred*/) {
            if (!ec) {
                std::string line;
                std::istream is(&buffer_);
                std::getline(is, line);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                process_command_line(line);
            }
        }
    );
}

void ClientSession::process_command_line(const std::string& line) {
    if (line.empty()) {
        do_read();
        return;
    }

    using namespace protocol;
    Command cmd = Parser::parse(line);
    std::string resp;

    switch (cmd.type) {
        case CommandType::Ping:
            resp = Response::pong();
            break;

        // --- External commands routed through Cluster ---
        case CommandType::Set:
            cluster_.set(cmd.key, cmd.value, cmd.ttl_seconds.has_value() ? std::optional<std::chrono::seconds>(std::chrono::seconds(cmd.ttl_seconds.value())) : std::nullopt);
            resp = Response::ok();
            break;
        case CommandType::Get: {
            auto val = cluster_.get(cmd.key);
            if (val.has_value()) {
                resp = Response::value(val.value());
            } else {
                resp = Response::not_found();
            }
            break;
        }
        case CommandType::Delete: {
            bool removed = cluster_.remove(cmd.key);
            resp = removed ? Response::ok() : Response::not_found();
            break;
        }
        case CommandType::Exists: {
            bool exists = cluster_.local_cache().contains(cmd.key);
            resp = Response::integer(exists ? 1 : 0);
            break;
        }
        case CommandType::Expire: {
            if (!cmd.ttl_seconds.has_value()) {
                resp = Response::error("EXPIRE requires key and TTL seconds");
            } else {
                bool ok = cluster_.expire(cmd.key, std::chrono::seconds(cmd.ttl_seconds.value()));
                resp = ok ? Response::ok() : Response::not_found();
            }
            break;
        }
        case CommandType::Ttl: {
            auto remaining = cluster_.ttl(cmd.key);
            if (remaining.has_value()) {
                resp = Response::integer(remaining.value().count());
            } else {
                resp = Response::integer(-1);
            }
            break;
        }
        case CommandType::Stats: {
            auto& cache = cluster_.local_cache();
            std::string stats_body = "entries:" + std::to_string(cache.size()) + "\r\n" +
                                     "capacity:" + std::to_string(cache.capacity()) + "\r\n" +
                                     "hits:" + std::to_string(cache.total_hits()) + "\r\n" +
                                     "misses:" + std::to_string(cache.total_misses()) + "\r\n" +
                                     "hit_rate:" + std::to_string(cache.overall_hit_rate()) + "%\r\n";
            resp = Response::stats(stats_body);
            break;
        }

        // --- Internal replication commands: write DIRECTLY to local cache ---
        // These must never be routed through Cluster again.
        case CommandType::ReplSet: {
            auto ttl_opt = cmd.ttl_seconds.has_value()
                ? std::optional<std::chrono::seconds>(std::chrono::seconds(cmd.ttl_seconds.value()))
                : std::nullopt;
            cluster_.local_cache().set(cmd.key, cmd.value, ttl_opt);
            resp = Response::ok();
            break;
        }
        case CommandType::ReplDelete: {
            cluster_.local_cache().remove(cmd.key);
            resp = Response::ok();
            break;
        }
        case CommandType::ReplExpire: {
            if (cmd.ttl_seconds.has_value()) {
                cluster_.local_cache().expire(cmd.key, std::chrono::seconds(cmd.ttl_seconds.value()));
            }
            resp = Response::ok();
            break;
        }

        default:
            resp = Response::error("Unknown command");
            break;
    }

    do_write(std::make_shared<std::string>(std::move(resp)));
}

void ClientSession::do_write(std::shared_ptr<std::string> response) {
    auto self(shared_from_this());
    boost::asio::async_write(
        socket_,
        boost::asio::buffer(*response),
        [this, self, response](boost::system::error_code ec, std::size_t /*bytes_transferred*/) {
            if (!ec) {
                do_read();
            } else {
                Logger::instance().error("TCP write error: " + ec.message());
            }
        }
    );
}

} // namespace shardcache::network
