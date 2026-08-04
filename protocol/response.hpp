#ifndef SHARDCACHE_RESPONSE_HPP
#define SHARDCACHE_RESPONSE_HPP

#include <string>

namespace shardcache::protocol {

class Response {
public:
    static std::string ok() {
        return "OK\r\n";
    }

    static std::string value(const std::string& val) {
        return "VALUE " + val + "\r\n";
    }

    static std::string not_found() {
        return "NOT_FOUND\r\n";
    }

    static std::string pong() {
        return "PONG\r\n";
    }

    static std::string error(const std::string& msg) {
        return "ERR " + msg + "\r\n";
    }

    static std::string integer(int64_t val) {
        return "INT " + std::to_string(val) + "\r\n";
    }

    static std::string stats(const std::string& stats_body) {
        return "STATS\r\n" + stats_body + "END\r\n";
    }
};

} // namespace shardcache::protocol

#endif // SHARDCACHE_RESPONSE_HPP
