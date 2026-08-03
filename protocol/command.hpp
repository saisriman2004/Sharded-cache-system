#ifndef SHARDCACHE_COMMAND_HPP
#define SHARDCACHE_COMMAND_HPP

#include <string>
#include <optional>
#include <cstdint>

namespace shardcache::protocol {

enum class CommandType {
    Get,
    Set,
    Delete,
    Exists,
    Expire,
    Ttl,
    Stats,
    Ping,
    Unknown
};

struct Command {
    CommandType type{CommandType::Unknown};
    std::string key{};
    std::string value{};
    std::optional<uint64_t> ttl_seconds{std::nullopt};
};

} // namespace shardcache::protocol

#endif // SHARDCACHE_COMMAND_HPP
