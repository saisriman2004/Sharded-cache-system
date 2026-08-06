#include "session.hpp"
#include "shardcache/logger.hpp"
#include <iostream>

namespace shardcache::network {

ClientSession::ClientSession(boost::asio::ip::tcp::socket socket, ShardedCache& cache)
    : socket_(std::move(socket)), cache_(cache) {}

void ClientSession::start() {
    do_read();
}

void ClientSession::do_read() {
    auto self(shared_from_this());
    boost::asio::async_read_until(
        socket_,
        buffer_,
        "\r\n",
        [this, self](boost::system::error_code ec, std::size_t bytes_transferred) {
            if (!ec) {
                std::string line;
                std::istream is(&buffer_);
                std::getline(is, line);
                if (!line.empty() && line.back() == '\r') {
                    line.pop_back();
                }

                process_command_line(line);
                do_read();
            }
        }
    );
}

void ClientSession::process_command_line(const std::string& line) {
    if (line.empty()) return;

    using namespace protocol;
    Command cmd = Parser::parse(line);
    std::string resp;

    switch (cmd.type) {
        case CommandType::Ping:
            resp = Response::pong();
            break;
        case CommandType::Set:
            cache_.set(cmd.key, cmd.value, cmd.ttl_seconds.has_value() ? std::optional<std::chrono::seconds>(std::chrono::seconds(cmd.ttl_seconds.value())) : std::nullopt);
            resp = Response::ok();
            break;
        case CommandType::Get: {
            auto val = cache_.get(cmd.key);
            if (val.has_value()) {
                resp = Response::value(val.value());
            } else {
                resp = Response::not_found();
            }
            break;
        }
        case CommandType::Delete: {
            bool removed = cache_.remove(cmd.key);
            resp = removed ? Response::ok() : Response::not_found();
            break;
        }
        case CommandType::Exists: {
            bool exists = cache_.contains(cmd.key);
            resp = Response::integer(exists ? 1 : 0);
            break;
        }
        case CommandType::Stats: {
            std::string stats_body = "entries:" + std::to_string(cache_.size()) + "\r\n" +
                                     "capacity:" + std::to_string(cache_.capacity()) + "\r\n" +
                                     "hits:" + std::to_string(cache_.total_hits()) + "\r\n" +
                                     "misses:" + std::to_string(cache_.total_misses()) + "\r\n" +
                                     "hit_rate:" + std::to_string(cache_.overall_hit_rate()) + "%\r\n";
            resp = Response::stats(stats_body);
            break;
        }
        default:
            resp = Response::error("Unknown command");
            break;
    }

    do_write(resp);
}

void ClientSession::do_write(const std::string& response) {
    auto self(shared_from_this());
    boost::asio::async_write(
        socket_,
        boost::asio::buffer(response),
        [this, self](boost::system::error_code ec, std::size_t /*bytes_transferred*/) {
            if (ec) {
                Logger::instance().error("TCP write error: " + ec.message());
            }
        }
    );
}

} // namespace shardcache::network
