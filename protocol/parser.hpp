#ifndef SHARDCACHE_PARSER_HPP
#define SHARDCACHE_PARSER_HPP

#include "command.hpp"
#include <string>

namespace shardcache::protocol {

class Parser {
public:
    static Command parse(const std::string& line);
};

} // namespace shardcache::protocol

#endif // SHARDCACHE_PARSER_HPP
