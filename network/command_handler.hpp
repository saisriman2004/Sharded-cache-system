#ifndef SHARDCACHE_COMMAND_HANDLER_HPP
#define SHARDCACHE_COMMAND_HANDLER_HPP

#include "shardcache/cluster.hpp"
#include "protocol/command.hpp"

#include <boost/asio/thread_pool.hpp>

#include <string>

namespace shardcache::network {

// Executes parsed commands against the cluster and encodes RESP replies.
class CommandHandler {
public:
    // `rpc_threads` sizes the executor that runs commands which must wait on
    // peer nodes; those threads mostly sit blocked on network I/O.
    explicit CommandHandler(Cluster& cluster, std::size_t rpc_threads = 64);
    ~CommandHandler();

    // True when `cmd` can be answered from this node alone. Anything else
    // blocks on peer RPCs and must run on rpc_executor(), never on an I/O
    // thread: otherwise nodes forwarding to each other can exhaust every I/O
    // thread and deadlock the cluster.
    bool runs_locally(const protocol::Command& cmd) const;

    boost::asio::thread_pool& rpc_executor() { return rpc_pool_; }

    // Appends the RESP reply for `cmd` to `out`.
    void execute(const protocol::Command& cmd, std::string& out);

private:
    Cluster& cluster_;
    boost::asio::thread_pool rpc_pool_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_COMMAND_HANDLER_HPP
