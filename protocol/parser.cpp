#include "parser.hpp"
#include <sstream>
#include <vector>
#include <algorithm>

namespace shardcache::protocol {

static std::string to_upper(std::string str) {
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::toupper(c); });
    return str;
}

Command Parser::parse(const std::string& line) {
    std::istringstream iss(line);
    std::vector<std::string> tokens;
    std::string token;
    while (iss >> token) {
        tokens.push_back(token);
    }

    Command cmd;
    if (tokens.empty()) {
        cmd.type = CommandType::Unknown;
        return cmd;
    }

    std::string verb = to_upper(tokens[0]);

    if (verb == "PING") {
        cmd.type = CommandType::Ping;
    } else if (verb == "STATS") {
        cmd.type = CommandType::Stats;
    } else if (verb == "GET" && tokens.size() >= 2) {
        cmd.type = CommandType::Get;
        cmd.key = tokens[1];
    } else if (verb == "DELETE" && tokens.size() >= 2) {
        cmd.type = CommandType::Delete;
        cmd.key = tokens[1];
    } else if (verb == "EXISTS" && tokens.size() >= 2) {
        cmd.type = CommandType::Exists;
        cmd.key = tokens[1];
    } else if (verb == "SET" && tokens.size() >= 3) {
        cmd.type = CommandType::Set;
        cmd.key = tokens[1];
        cmd.value = tokens[2];

        if (tokens.size() >= 5 && to_upper(tokens[3]) == "TTL") {
            try {
                cmd.ttl_seconds = std::stoull(tokens[4]);
            } catch (...) {
                cmd.type = CommandType::Unknown;
            }
        }
    } else if (verb == "EXPIRE" && tokens.size() >= 3) {
        cmd.type = CommandType::Expire;
        cmd.key = tokens[1];
        try {
            cmd.ttl_seconds = std::stoull(tokens[2]);
        } catch (...) {
            cmd.type = CommandType::Unknown;
        }
    } else if (verb == "TTL" && tokens.size() >= 2) {
        cmd.type = CommandType::Ttl;
        cmd.key = tokens[1];
    } else {
        cmd.type = CommandType::Unknown;
    }

    return cmd;
}

} // namespace shardcache::protocol
