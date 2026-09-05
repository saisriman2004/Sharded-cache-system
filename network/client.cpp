#include "client.hpp"
#include <iostream>

namespace shardcache::network {

Client::Client(
    const std::string& host,
    uint16_t port,
    std::chrono::milliseconds connect_timeout,
    std::chrono::milliseconds io_timeout
)
    : host_(host),
      port_(port),
      connect_timeout_(connect_timeout),
      io_timeout_(io_timeout),
      socket_(io_context_) {}

Client::~Client() {
    disconnect();
}

bool Client::connect() {
    try {
        io_context_.restart();
        boost::asio::ip::tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host_, std::to_string(port_));

        boost::asio::steady_timer timer(io_context_);
        timer.expires_after(connect_timeout_);

        boost::system::error_code connect_ec = boost::asio::error::would_block;
        bool timed_out = false;

        boost::asio::async_connect(
            socket_,
            endpoints,
            [&connect_ec, &timer](const boost::system::error_code& ec, const boost::asio::ip::tcp::endpoint&) {
                connect_ec = ec;
                timer.cancel();
            }
        );

        timer.async_wait([&](const boost::system::error_code& ec) {
            if (!ec) {
                timed_out = true;
                boost::system::error_code close_ec;
                socket_.close(close_ec);
            }
        });

        io_context_.run();

        if (timed_out || connect_ec) {
            connected_ = false;
            return false;
        }

        connected_ = true;
        return true;
    } catch (...) {
        connected_ = false;
        return false;
    }
}

void Client::disconnect() {
    if (connected_) {
        boost::system::error_code ec;
        socket_.close(ec);
        connected_ = false;
    }
}

RequestResult<std::string> Client::send_raw(const std::string& command) {
    if (!connected_) {
        return RequestResult<std::string>::error(RequestStatus::ConnectionError, "Client is not connected");
    }

    try {
        io_context_.restart();

        boost::asio::steady_timer timer(io_context_);
        timer.expires_after(io_timeout_);

        boost::system::error_code op_ec = boost::asio::error::would_block;
        bool timed_out = false;
        boost::asio::streambuf response;

        std::string request = command + "\r\n";

        boost::asio::async_write(
            socket_,
            boost::asio::buffer(request),
            [&](const boost::system::error_code& ec, std::size_t) {
                if (!ec) {
                    boost::asio::async_read_until(
                        socket_,
                        response,
                        "\r\n",
                        [&](const boost::system::error_code& read_ec, std::size_t) {
                            op_ec = read_ec;
                            timer.cancel();
                        }
                    );
                } else {
                    op_ec = ec;
                    timer.cancel();
                }
            }
        );

        timer.async_wait([&](const boost::system::error_code& ec) {
            if (!ec) {
                timed_out = true;
                boost::system::error_code close_ec;
                socket_.close(close_ec);
            }
        });

        io_context_.run();

        if (timed_out) {
            disconnect();
            return RequestResult<std::string>::error(RequestStatus::Timeout, "Operation timed out");
        }

        if (op_ec) {
            disconnect();
            return RequestResult<std::string>::error(RequestStatus::ConnectionError, op_ec.message());
        }

        std::istream is(&response);
        std::string line;
        std::getline(is, line);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        return RequestResult<std::string>::success(line);
    } catch (const std::exception& e) {
        disconnect();
        return RequestResult<std::string>::error(RequestStatus::ConnectionError, e.what());
    } catch (...) {
        disconnect();
        return RequestResult<std::string>::error(RequestStatus::ConnectionError, "Unknown socket error");
    }
}

RequestResult<void> Client::set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl) {
    std::string cmd = "SET " + key + " " + value;
    if (ttl.has_value()) {
        cmd += " TTL " + std::to_string(ttl.value());
    }
    auto raw = send_raw(cmd);
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<std::string> Client::get(const std::string& key) {
    auto raw = send_raw("GET " + key);
    if (!raw.is_ok()) return raw;
    if (raw.value == "NOT_FOUND") {
        return RequestResult<std::string>::not_found();
    }
    if (raw.value->rfind("VALUE ", 0) == 0) {
        return RequestResult<std::string>::success(raw.value->substr(6));
    }
    return RequestResult<std::string>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<void> Client::remove(const std::string& key) {
    auto raw = send_raw("DELETE " + key);
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    if (raw.value == "NOT_FOUND") return RequestResult<void>::not_found();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<void> Client::expire(const std::string& key, uint64_t ttl_seconds) {
    auto raw = send_raw("EXPIRE " + key + " " + std::to_string(ttl_seconds));
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    if (raw.value == "NOT_FOUND") return RequestResult<void>::not_found();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<int64_t> Client::ttl(const std::string& key) {
    auto raw = send_raw("TTL " + key);
    if (!raw.is_ok()) return RequestResult<int64_t>::error(raw.status, raw.message);
    if (raw.value == "NOT_FOUND") return RequestResult<int64_t>::not_found();
    if (raw.value->rfind("INT ", 0) == 0) {
        try {
            return RequestResult<int64_t>::success(std::stoll(raw.value->substr(4)));
        } catch (...) {
            return RequestResult<int64_t>::error(RequestStatus::ProtocolError, "Invalid integer response");
        }
    }
    return RequestResult<int64_t>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

bool Client::ping() {
    auto raw = send_raw("PING");
    return raw.is_ok() && raw.value == "PONG";
}

RequestResult<void> Client::replica_set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl) {
    std::string cmd = "REPL_SET " + key + " " + value;
    if (ttl.has_value()) {
        cmd += " TTL " + std::to_string(ttl.value());
    }
    auto raw = send_raw(cmd);
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<void> Client::replica_delete(const std::string& key) {
    auto raw = send_raw("REPL_DELETE " + key);
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    if (raw.value == "NOT_FOUND") return RequestResult<void>::not_found();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

RequestResult<void> Client::replica_expire(const std::string& key, uint64_t ttl_seconds) {
    auto raw = send_raw("REPL_EXPIRE " + key + " " + std::to_string(ttl_seconds));
    if (!raw.is_ok()) return RequestResult<void>::error(raw.status, raw.message);
    if (raw.value == "OK") return RequestResult<void>::success();
    if (raw.value == "NOT_FOUND") return RequestResult<void>::not_found();
    return RequestResult<void>::error(RequestStatus::ProtocolError, raw.value.value_or(""));
}

} // namespace shardcache::network
