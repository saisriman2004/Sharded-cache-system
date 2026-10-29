#include "client.hpp"
#include <array>
#include <functional>

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
    rx_.clear();
}

RequestResult<protocol::RespValue> Client::command(const std::vector<std::string>& args) {
    using Result = RequestResult<protocol::RespValue>;
    if (!connected_) {
        return Result::error(RequestStatus::ConnectionError, "Client is not connected");
    }

    try {
        io_context_.restart();

        boost::asio::steady_timer timer(io_context_);
        timer.expires_after(io_timeout_);

        const std::string request = protocol::resp::command(args);
        std::array<char, 16 * 1024> chunk{};
        protocol::RespValue reply;
        boost::system::error_code op_ec;
        bool timed_out = false;
        bool malformed = false;

        // Returns true once a complete reply (or a protocol error) is buffered.
        auto try_parse = [&]() {
            std::size_t used = 0;
            auto st = protocol::parse_reply(rx_, reply, used);
            if (st == protocol::ParseStatus::Ok) {
                rx_.erase(0, used);
                return true;
            }
            malformed = st == protocol::ParseStatus::Error;
            return malformed;
        };

        std::function<void()> read_more = [&]() {
            socket_.async_read_some(
                boost::asio::buffer(chunk),
                [&](const boost::system::error_code& ec, std::size_t n) {
                    if (ec) {
                        op_ec = ec;
                        timer.cancel();
                        return;
                    }
                    rx_.append(chunk.data(), n);
                    if (try_parse()) {
                        timer.cancel();
                        return;
                    }
                    read_more();
                }
            );
        };

        boost::asio::async_write(
            socket_,
            boost::asio::buffer(request),
            [&](const boost::system::error_code& ec, std::size_t) {
                if (ec) {
                    op_ec = ec;
                    timer.cancel();
                    return;
                }
                read_more();
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
            return Result::error(RequestStatus::Timeout, "Operation timed out");
        }
        if (op_ec) {
            disconnect();
            return Result::error(RequestStatus::ConnectionError, op_ec.message());
        }
        if (malformed) {
            disconnect();
            return Result::error(RequestStatus::ProtocolError, "Malformed RESP reply");
        }
        return Result::success(std::move(reply));
    } catch (const std::exception& e) {
        disconnect();
        return Result::error(RequestStatus::ConnectionError, e.what());
    } catch (...) {
        disconnect();
        return Result::error(RequestStatus::ConnectionError, "Unknown socket error");
    }
}

RequestResult<void> Client::expect_ok(const std::vector<std::string>& args) {
    auto res = command(args);
    if (!res.is_ok()) return RequestResult<void>::error(res.status, res.message);
    if (res.value->type == protocol::RespValue::Type::SimpleString && res.value->str == "OK") {
        return RequestResult<void>::success();
    }
    return RequestResult<void>::error(RequestStatus::ProtocolError, res.value->str);
}

RequestResult<void> Client::set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl_ms) {
    if (ttl_ms.has_value()) {
        return expect_ok({"SET", key, value, "PX", std::to_string(ttl_ms.value())});
    }
    return expect_ok({"SET", key, value});
}

RequestResult<std::string> Client::get(const std::string& key) {
    auto res = command({"GET", key});
    if (!res.is_ok()) return RequestResult<std::string>::error(res.status, res.message);
    if (res.value->is_null()) return RequestResult<std::string>::not_found();
    if (res.value->type == protocol::RespValue::Type::BulkString) {
        return RequestResult<std::string>::success(std::move(res.value->str));
    }
    return RequestResult<std::string>::error(RequestStatus::ProtocolError, res.value->str);
}

namespace {

// Maps an integer reply of 1/0 (DEL, EXPIRE) to Ok/NotFound.
RequestResult<void> integer_to_result(const RequestResult<protocol::RespValue>& res) {
    if (!res.is_ok()) return RequestResult<void>::error(res.status, res.message);
    if (res.value->type != protocol::RespValue::Type::Integer) {
        return RequestResult<void>::error(RequestStatus::ProtocolError, res.value->str);
    }
    return res.value->integer > 0 ? RequestResult<void>::success() : RequestResult<void>::not_found();
}

} // namespace

RequestResult<void> Client::remove(const std::string& key) {
    return integer_to_result(command({"DEL", key}));
}

RequestResult<void> Client::expire(const std::string& key, uint64_t ttl_ms) {
    return integer_to_result(command({"PEXPIRE", key, std::to_string(ttl_ms)}));
}

RequestResult<int64_t> Client::pttl(const std::string& key) {
    auto res = command({"PTTL", key});
    if (!res.is_ok()) return RequestResult<int64_t>::error(res.status, res.message);
    if (res.value->type != protocol::RespValue::Type::Integer) {
        return RequestResult<int64_t>::error(RequestStatus::ProtocolError, res.value->str);
    }
    if (res.value->integer == -2) return RequestResult<int64_t>::not_found();
    return RequestResult<int64_t>::success(res.value->integer);
}

bool Client::ping() {
    auto res = command({"PING"});
    return res.is_ok() && res.value->str == "PONG";
}

RequestResult<void> Client::replica_set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl_ms) {
    if (ttl_ms.has_value()) {
        return expect_ok({"REPL_SET", key, value, "PX", std::to_string(ttl_ms.value())});
    }
    return expect_ok({"REPL_SET", key, value});
}

RequestResult<void> Client::replica_delete(const std::string& key) {
    return expect_ok({"REPL_DELETE", key});
}

RequestResult<void> Client::replica_expire(const std::string& key, uint64_t ttl_ms) {
    return expect_ok({"REPL_PEXPIRE", key, std::to_string(ttl_ms)});
}

} // namespace shardcache::network
