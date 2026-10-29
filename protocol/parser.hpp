#ifndef SHARDCACHE_PARSER_HPP
#define SHARDCACHE_PARSER_HPP

#include "command.hpp"
#include <string>
#include <vector>

namespace shardcache::protocol {

class Parser {
public:
    // Builds a command from already-split arguments (a decoded RESP array).
    static Command parse(const std::vector<std::string>& args);

    // Convenience overload for inline, whitespace-separated commands.
    static Command parse(const std::string& line);
};

} // namespace shardcache::protocol

#endif // SHARDCACHE_PARSER_HPP
