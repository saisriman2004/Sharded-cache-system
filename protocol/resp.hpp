#ifndef SHARDCACHE_RESP_HPP
#define SHARDCACHE_RESP_HPP

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace shardcache::protocol {

// RESP2 (REdis Serialization Protocol) codec. Speaking RESP makes the server
// usable from redis-cli, redis-benchmark, memtier_benchmark and any Redis
// client library.

struct RespValue {
    enum class Type { SimpleString, Error, Integer, BulkString, Null, Array };

    Type type{Type::Null};
    std::string str{};
    int64_t integer{0};
    std::vector<RespValue> elements{};

    bool is_error() const { return type == Type::Error; }
    bool is_null() const { return type == Type::Null; }
};

enum class ParseStatus { Ok, Incomplete, Error };

// Hard limits that keep a malicious client from forcing huge allocations.
inline constexpr std::size_t kMaxBulkLength = 64 * 1024 * 1024;
inline constexpr std::size_t kMaxArrayLength = 1024 * 1024;
inline constexpr std::size_t kMaxInlineLength = 64 * 1024;

// Parses one client request from the front of `buf`: either a RESP array of
// bulk strings or an inline command ("SET k v\r\n"). On Ok, `args` holds the
// command and `consumed` the number of bytes it occupied. An empty inline
// line parses as Ok with empty `args`.
ParseStatus parse_request(std::string_view buf, std::vector<std::string>& args,
                          std::size_t& consumed, std::string& error);

// Parses one reply value from the front of `buf`.
ParseStatus parse_reply(std::string_view buf, RespValue& out, std::size_t& consumed);

namespace resp {

inline void append_simple(std::string& out, std::string_view s) {
    out += '+';
    out += s;
    out += "\r\n";
}

inline void append_error(std::string& out, std::string_view s) {
    out += '-';
    out += s;
    out += "\r\n";
}

inline void append_integer(std::string& out, int64_t n) {
    out += ':';
    out += std::to_string(n);
    out += "\r\n";
}

inline void append_bulk(std::string& out, std::string_view s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

inline void append_null(std::string& out) {
    out += "$-1\r\n";
}

inline void append_array_header(std::string& out, std::size_t n) {
    out += '*';
    out += std::to_string(n);
    out += "\r\n";
}

// Encodes a command as a RESP array of bulk strings.
inline std::string command(const std::vector<std::string>& args) {
    std::string out;
    append_array_header(out, args.size());
    for (const auto& a : args) {
        append_bulk(out, a);
    }
    return out;
}

} // namespace resp

} // namespace shardcache::protocol

#endif // SHARDCACHE_RESP_HPP
