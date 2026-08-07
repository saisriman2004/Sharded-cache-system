#include "client.hpp"
#include <iostream>

namespace shardcache::network {

Client::Client(const std::string& host, uint16_t port)
    : host_(host), port_(port), socket_(io_context_) {}

Client::~Client() {
    disconnect();
}

bool Client::connect() {
    try {
        boost::asio::ip::tcp::resolver resolver(io_context_);
        auto endpoints = resolver.resolve(host_, std::to_string(port_));
        boost::asio::connect(socket_, endpoints);
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

std::string Client::send_raw(const std::string& command) {
    if (!connected_) return "";
    try {
        boost::asio::write(socket_, boost::asio::buffer(command + "\r\n"));
        boost::asio::streambuf response;
        boost::asio::read_until(socket_, response, "\r\n");
        std::istream is(&response);
        std::string line;
        std::getline(is, line);
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        return line;
    } catch (...) {
        return "";
    }
}

bool Client::set(const std::string& key, const std::string& value, std::optional<uint64_t> ttl) {
    std::string cmd = "SET " + key + " " + value;
    if (ttl.has_value()) {
        cmd += " TTL " + std::to_string(ttl.value());
    }
    return send_raw(cmd) == "OK";
}

std::optional<std::string> Client::get(const std::string& key) {
    std::string resp = send_raw("GET " + key);
    if (resp.rfind("VALUE ", 0) == 0) {
        return resp.substr(6);
    }
    return std::nullopt;
}

bool Client::remove(const std::string& key) {
    return send_raw("DELETE " + key) == "OK";
}

bool Client::ping() {
    return send_raw("PING") == "PONG";
}

} // namespace shardcache::network
