#ifndef SHARDCACHE_COMMAND_HANDLER_HPP
#define SHARDCACHE_COMMAND_HANDLER_HPP

#include "shardcache/cluster.hpp"
#include "protocol/command.hpp"

#include <string>

namespace shardcache::network {

// Executes parsed commands against the cluster and encodes RESP replies.
class CommandHandler {
public:
    explicit CommandHandler(Cluster& cluster) : cluster_(cluster) {}

    // Appends the RESP reply for `cmd` to `out`.
    void execute(const protocol::Command& cmd, std::string& out);

private:
    Cluster& cluster_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_COMMAND_HANDLER_HPP
