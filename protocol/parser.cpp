#include "parser.hpp"
#include <algorithm>
#include <charconv>
#include <sstream>

namespace shardcache::protocol {

namespace {

std::string to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::toupper(c); });
    return str;
}

std::string to_lower(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
    return str;
}

std::optional<uint64_t> parse_u64(const std::string& s) {
    uint64_t v = 0;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), v);
    if (ec != std::errc() || ptr != s.data() + s.size() || s.empty()) return std::nullopt;
    return v;
}

Command invalid(std::string message) {
    Command cmd;
    cmd.type = CommandType::Unknown;
    cmd.error = std::move(message);
    return cmd;
}

Command wrong_arity(const std::string& verb) {
    return invalid("ERR wrong number of arguments for '" + to_lower(verb) + "' command");
}

constexpr uint64_t kMaxTtlSeconds = 10ull * 365 * 24 * 3600;

// Parses SET-style trailing options: [EX seconds | PX milliseconds | TTL seconds].
bool parse_set_options(const std::vector<std::string>& args, std::size_t from, Command& cmd) {
    for (std::size_t i = from; i < args.size(); ++i) {
        std::string opt = to_upper(args[i]);
        if ((opt == "EX" || opt == "PX" || opt == "TTL") && i + 1 < args.size()) {
            auto n = parse_u64(args[i + 1]);
            if (!n.has_value() || n.value() == 0) return false;
            uint64_t ms = opt == "PX" ? n.value() : n.value() * 1000;
            if (ms > kMaxTtlSeconds * 1000) return false;
            cmd.ttl_ms = ms;
            ++i;
        } else {
            return false;
        }
    }
    return true;
}

} // namespace

Command Parser::parse(const std::vector<std::string>& args) {
    if (args.empty()) {
        return invalid("ERR empty command");
    }

    const std::string verb = to_upper(args[0]);
    const std::size_t argc = args.size();
    Command cmd;

    if (verb == "PING") {
        if (argc > 2) return wrong_arity(verb);
        cmd.type = CommandType::Ping;
        if (argc == 2) cmd.value = args[1];
    } else if (verb == "ECHO") {
        if (argc != 2) return wrong_arity(verb);
        cmd.type = CommandType::Echo;
        cmd.value = args[1];
    } else if (verb == "STATS" || verb == "INFO") {
        cmd.type = CommandType::Stats;
    } else if (verb == "DBSIZE") {
        cmd.type = CommandType::DbSize;
    } else if (verb == "FLUSHALL" || verb == "FLUSHDB") {
        cmd.type = CommandType::FlushAll;
    } else if (verb == "COMMAND") {
        cmd.type = CommandType::CommandDocs;
    } else if (verb == "CONFIG") {
        cmd.type = CommandType::Config;
        if (argc >= 3 && to_upper(args[1]) == "GET") {
            cmd.keys.assign(args.begin() + 2, args.end());
        }
    } else if (verb == "SELECT") {
        if (argc != 2) return wrong_arity(verb);
        cmd.type = CommandType::Select;
        cmd.value = args[1];
    } else if (verb == "CLUSTER") {
        if (argc < 2) return wrong_arity(verb);
        const std::string sub = to_upper(args[1]);
        if (sub == "GOSSIP") {
            if ((argc - 2) % 5 != 0) return invalid("ERR malformed gossip payload");
            cmd.type = CommandType::ClusterGossip;
            cmd.keys.assign(args.begin() + 2, args.end());
        } else if (sub == "NODES") {
            cmd.type = CommandType::ClusterNodes;
        } else {
            return invalid("ERR unknown CLUSTER subcommand '" + args[1] + "'");
        }
    } else if (verb == "CLIENT") {
        cmd.type = CommandType::Client;
    } else if (verb == "QUIT") {
        cmd.type = CommandType::Quit;
    } else if (verb == "GET") {
        if (argc != 2) return wrong_arity(verb);
        cmd.type = CommandType::Get;
        cmd.key = args[1];
    } else if (verb == "SET") {
        if (argc < 3) return wrong_arity(verb);
        cmd.type = CommandType::Set;
        cmd.key = args[1];
        cmd.value = args[2];
        if (!parse_set_options(args, 3, cmd)) return invalid("ERR syntax error");
    } else if (verb == "REPL_SET") {
        if (argc < 4) return wrong_arity(verb);
        auto version = parse_u64(args[3]);
        if (!version.has_value()) return invalid("ERR invalid version");
        cmd.type = CommandType::ReplSet;
        cmd.key = args[1];
        cmd.value = args[2];
        cmd.version = version.value();
        if (!parse_set_options(args, 4, cmd)) return invalid("ERR syntax error");
    } else if (verb == "REPL_DEL") {
        if (argc != 3) return wrong_arity(verb);
        auto version = parse_u64(args[2]);
        if (!version.has_value()) return invalid("ERR invalid version");
        cmd.type = CommandType::ReplDelete;
        cmd.key = args[1];
        cmd.version = version.value();
    } else if (verb == "REPL_GET") {
        if (argc != 2) return wrong_arity(verb);
        cmd.type = CommandType::ReplGet;
        cmd.key = args[1];
    } else if (verb == "DEL" || verb == "DELETE" || verb == "EXISTS") {
        if (argc < 2) return wrong_arity(verb);
        cmd.type = verb == "EXISTS" ? CommandType::Exists : CommandType::Delete;
        cmd.key = args[1];
        cmd.keys.assign(args.begin() + 1, args.end());
    } else if (verb == "EXPIRE" || verb == "PEXPIRE") {
        if (argc != 3) return wrong_arity(verb);
        cmd.type = CommandType::Expire;
        cmd.key = args[1];
        auto n = parse_u64(args[2]);
        if (!n.has_value() || n.value() > kMaxTtlSeconds * 1000) {
            return invalid("ERR value is not an integer or out of range");
        }
        bool millis = verb == "PEXPIRE";
        cmd.ttl_ms = millis ? n.value() : n.value() * 1000;
    } else if (verb == "TTL" || verb == "PTTL") {
        if (argc != 2) return wrong_arity(verb);
        cmd.type = verb == "TTL" ? CommandType::Ttl : CommandType::Pttl;
        cmd.key = args[1];
    } else {
        return invalid("ERR unknown command '" + args[0] + "'");
    }

    return cmd;
}

Command Parser::parse(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }
    return parse(tokens);
}

} // namespace shardcache::protocol
