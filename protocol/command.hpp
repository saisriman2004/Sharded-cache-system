#ifndef SHARDCACHE_COMMAND_HPP
#define SHARDCACHE_COMMAND_HPP

#include <string>
#include <vector>
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
    Pttl,
    Stats,
    Ping,
    Echo,
    DbSize,
    FlushAll,
    // Connection-level commands that Redis tooling sends during handshakes.
    CommandDocs,
    Config,
    Select,
    Client,
    Quit,
    // Internal replication commands — these write directly to the
    // receiving node's local cache and must never be routed through
    // the cluster hash ring again.
    ReplSet,     // REPL_SET key value version [PX ms]
    ReplDelete,  // REPL_DEL key version
    ReplGet,     // REPL_GET key
    Unknown
};

struct Command {
    CommandType type{CommandType::Unknown};
    std::string key{};               // first key argument
    std::vector<std::string> keys{}; // all key arguments (DEL / EXISTS)
    std::string value{};
    std::optional<uint64_t> ttl_ms{std::nullopt};
    uint64_t version{0};             // internal replication commands only
    std::string error{};             // set when type == Unknown
};

} // namespace shardcache::protocol

#endif // SHARDCACHE_COMMAND_HPP
