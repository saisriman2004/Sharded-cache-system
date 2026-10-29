#ifndef SHARDCACHE_COMMAND_HANDLER_HPP
#define SHARDCACHE_COMMAND_HANDLER_HPP

#include "shardcache/cluster.hpp"
#include "protocol/command.hpp"
#include "shardcache/metrics.hpp"

#include <boost/asio/thread_pool.hpp>

#include <array>
#include <atomic>
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

    struct CommandMetrics {
        StripedCounter calls;
        StripedCounter errors;
        LatencyHistogram latency;
    };
    static constexpr std::size_t kCommandTypes = static_cast<std::size_t>(protocol::CommandType::Unknown) + 1;

    const CommandMetrics& metrics(protocol::CommandType t) const { return metrics_[static_cast<std::size_t>(t)]; }
    Cluster& cluster() { return cluster_; }

    void connection_opened() { connections_total_.add(); connections_active_.fetch_add(1, std::memory_order_relaxed); }
    void connection_closed() { connections_active_.fetch_sub(1, std::memory_order_relaxed); }
    uint64_t connections_total() const { return connections_total_.value(); }
    int64_t connections_active() const { return connections_active_.load(std::memory_order_relaxed); }

private:
    void dispatch(const protocol::Command& cmd, std::string& out);

    std::array<CommandMetrics, kCommandTypes> metrics_{};
    StripedCounter connections_total_;
    std::atomic<int64_t> connections_active_{0};
    Cluster& cluster_;
    boost::asio::thread_pool rpc_pool_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_COMMAND_HANDLER_HPP
