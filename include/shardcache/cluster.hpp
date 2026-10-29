#ifndef SHARDCACHE_CLUSTER_HPP
#define SHARDCACHE_CLUSTER_HPP

#include "shardcache/consistent_hash.hpp"
#include "shardcache/sharded_cache.hpp"
#include "shardcache/health_checker.hpp"
#include "shardcache/hybrid_clock.hpp"
#include "shardcache/routing_table.hpp"

#include <boost/asio/thread_pool.hpp>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <thread>
#include <unordered_map>

namespace shardcache {

namespace network { class PeerPool; class PeerChannels; }

enum class Consistency {
    // R/W quorums with last-writer-wins and asynchronous read repair. Fast,
    // but not linearizable: a read can return a value from a write still in
    // flight and a later read an older one (see docs/consistency.md).
    Quorum,
    // ABD-style: reads write the value they return back to a write quorum
    // before replying, and writes first read the highest version from a read
    // quorum. Linearizable while a quorum is reachable; costs up to one
    // extra round trip per operation.
    Linearizable,
};

struct ClusterConfig {
    // N: copies of each key, placed on consecutive distinct nodes of the ring.
    std::size_t replication_factor{3};
    // R and W; 0 means a majority of N. R + W > N makes every read quorum
    // overlap the latest write quorum.
    std::size_t read_quorum{0};
    std::size_t write_quorum{0};
    std::size_t cache_capacity{100000};
    std::chrono::milliseconds rpc_timeout{1000};
    // Must exceed the longest expected replication / hint-replay delay, or a
    // delayed older write can resurrect a deleted key.
    std::chrono::milliseconds tombstone_ttl{std::chrono::seconds(60)};
    std::chrono::milliseconds health_check_interval{1000};
    std::size_t rpc_threads{32};
    // Pipelined async connections per peer for quorum fan-out, and the
    // threads that drive them.
    std::size_t peer_channels{2};
    std::size_t peer_io_threads{2};
    std::size_t max_hints_per_node{100000};
    Consistency consistency{Consistency::Quorum};
    std::chrono::milliseconds gossip_interval{500};
};

enum class MemberStatus { Alive, Left };

// One node's entry in the gossiped membership table. Each node owns its own
// entry and bumps `generation` whenever it changes it (it starts at the
// node's boot time in ms, so a restarted node supersedes its old self), so
// merging is simply "higher generation wins".
struct MemberInfo {
    CacheNode node;
    uint64_t generation{0};
    MemberStatus status{MemberStatus::Alive};
};

enum class ReadStatus { Found, NotFound, Unavailable };

struct ReadResult {
    ReadStatus status{ReadStatus::Unavailable};
    VersionedValue value{};
};

// Monotonic counters exported as Prometheus metrics.
struct ClusterStats {
    uint64_t read_quorum_failures{0};
    uint64_t write_quorum_failures{0};
    uint64_t read_repairs{0};     // stale replicas repaired by reads
    uint64_t hints_delivered{0};
    uint64_t hints_dropped{0};    // overflowed max_hints_per_node
    uint64_t keys_moved{0};       // pushed to new owners by rebalancing
    std::size_t hints_pending{0};
    std::size_t healthy_nodes{0};
    std::size_t ring_nodes{0};
};

struct WriteResult {
    bool ok{false};       // write quorum reached
    bool existed{false};  // key was live on at least one acknowledging replica
    std::size_t acks{0};
};

// Leaderless, Dynamo-style replication. Any node coordinates any request:
// writes get a hybrid-logical-clock version and go to all N replicas in
// parallel, succeeding once W acknowledge; reads collect R replies and
// return the newest version, repairing stale replicas in the background.
// Writes a replica misses are kept as hints and replayed when it recovers.
class Cluster {
public:
    Cluster(const CacheNode& local_node, ClusterConfig config);

    explicit Cluster(
        const CacheNode& local_node,
        std::size_t replication_factor = 2,
        std::size_t cache_capacity = 100000
    );

    /// Convenience constructor using just an ID (local address defaults to 127.0.0.1:7001).
    explicit Cluster(
        const std::string& local_node_id,
        std::size_t replication_factor = 2,
        std::size_t cache_capacity = 100000
    );

    ~Cluster();

    void start();
    void stop();

    void add_node(const CacheNode& node);
    void remove_node(const std::string& node_id);

    // --- Membership (gossip) and data placement ---
    // Address of a node to gossip with until real peers are known (join).
    void add_seed(const std::string& host, uint16_t port);
    // Merges a peer's membership view into ours; returns our merged view.
    std::vector<MemberInfo> merge_gossip(const std::vector<MemberInfo>& remote);
    std::vector<MemberInfo> members() const;
    // One push-pull gossip round with a random peer (normally periodic).
    void gossip_once();
    // Graceful departure: announce Left to every peer, then hand all local
    // data to its new owners. The node keeps serving as a coordinator.
    void leave();
    // Streams keys whose preference list changed since the last placement to
    // their new owners and drops keys this node no longer owns. Runs in the
    // background after membership changes; public for tests.
    void rebalance_now();
    uint64_t keys_moved() const { return keys_moved_.load(); }
    ClusterStats stats() const;

    // --- Coordinated operations ---
    ReadResult read(const std::string& key);
    WriteResult put(const std::string& key, const std::string& value,
                    std::optional<std::chrono::milliseconds> ttl = std::nullopt);
    WriteResult del(const std::string& key);
    // Read-modify-write of the TTL; `existed` reports whether the key was found.
    WriteResult expire_key(const std::string& key, std::chrono::milliseconds ttl);

    // Convenience wrappers; unavailability reads as absence / failure.
    bool set(
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt
    );
    std::optional<std::string> get(const std::string& key);
    bool exists(const std::string& key);
    bool remove(const std::string& key);
    bool expire(const std::string& key, std::chrono::milliseconds ttl);
    // Redis PTTL semantics: -2 absent, -1 no expiry, else remaining milliseconds.
    int64_t pttl(const std::string& key);

    std::string get_or_load(
        const std::string& key,
        std::function<std::string()> loader,
        std::optional<std::chrono::milliseconds> ttl = std::nullopt
    );

    // --- Replica-side handlers for internal RPCs ---
    // Returns whether the key was live before the write.
    bool apply_replica_write(const std::string& key, const VersionedValue& v);
    std::optional<VersionedValue> replica_read(const std::string& key);

    // True if the operation needs no peer RPC (so it can run on an I/O thread).
    bool can_serve_locally(const std::string& key, bool is_write) const;

    std::size_t node_count() const;
    std::optional<CacheNode> locate(const std::string& key) const;
    std::vector<CacheNode> locate_replicas(const std::string& key, std::size_t count) const;
    // The N nodes responsible for `key`, in ring order.
    std::vector<CacheNode> preference_list(const std::string& key) const;
    bool is_node_healthy(const std::string& node_id) const;
    void set_node_health(const std::string& node_id, bool healthy);
    ShardedCache& local_cache() { return local_cache_; }
    const CacheNode& local_node() const { return local_node_; }
    const ClusterConfig& config() const { return config_; }

    std::size_t pending_hints() const;
    // Delivers queued hints to every healthy node now; normally runs in the background.
    void replay_hints();

private:
    struct Hint {
        std::string key;
        VersionedValue value;
        std::optional<std::chrono::steady_clock::time_point> deadline;
    };

    struct QuorumReplies {
        bool ok{false};  // at least R replicas answered
        std::vector<CacheNode> targets;
        std::vector<std::optional<VersionedValue>> values;
        std::vector<bool> replied;
    };

    void init_common();
    // Queries every replica of `key` and waits for R replies.
    QuorumReplies collect(const std::string& key);
    // Version for a new write; nullopt if the version query found no quorum.
    std::optional<uint64_t> next_version(const std::string& key);
    // Routing snapshot for the calling thread; refreshed only when membership
    // changes, so the request path takes no lock.
    const RoutingTable& routing() const;
    void publish_routing_locked();
    WriteResult replicate(const std::string& key, const VersionedValue& v);
    bool send_write(const CacheNode& node, const std::string& key, const VersionedValue& v, bool* was_live);
    void read_repair(const std::string& key, const VersionedValue& winner, const std::vector<CacheNode>& stale);
    void add_hint(const CacheNode& node, const std::string& key, const VersionedValue& v);
    void hint_loop();
    void gossip_loop();
    void rebalance_loop();
    // Applies one membership entry; returns true if the ring changed.
    bool apply_member_locked(const MemberInfo& m, std::vector<CacheNode>& added, std::vector<std::string>& removed);
    void after_membership_change(const std::vector<CacheNode>& added, const std::vector<std::string>& removed);
    bool exchange_gossip(const std::string& host, uint16_t port);
    std::size_t effective_quorum(std::size_t configured, std::size_t replicas) const;

    ClusterConfig config_;
    CacheNode local_node_;
    HybridClock clock_;
    std::shared_ptr<network::PeerPool> peer_pool_;          // blocking calls (hints, rebalance, gossip)
    std::unique_ptr<network::PeerChannels> peer_channels_;  // quorum reads and writes
    ConsistentHashRing ring_;
    ShardedCache local_cache_;
    std::unique_ptr<HealthChecker> health_checker_;
    std::unordered_map<std::string, CacheNode> nodes_;
    std::unordered_map<std::string, bool> node_health_;
    mutable std::shared_mutex membership_mutex_;

    std::unordered_map<std::string, MemberInfo> members_;
    std::vector<std::pair<std::string, uint16_t>> seeds_;

    std::mutex rebalance_mutex_;  // one rebalance at a time
    std::mutex rebalance_wake_mutex_;
    std::condition_variable rebalance_cv_;
    bool rebalance_requested_{false};
    std::shared_ptr<const RoutingTable> placed_for_;  // table the local data was last placed for
    std::atomic<uint64_t> keys_moved_{0};
    std::atomic<uint64_t> read_quorum_failures_{0};
    std::atomic<uint64_t> write_quorum_failures_{0};
    std::atomic<uint64_t> read_repairs_{0};
    std::atomic<uint64_t> hints_delivered_{0};
    std::atomic<uint64_t> hints_dropped_{0};

    std::mutex gossip_wake_mutex_;
    std::condition_variable gossip_cv_;
    std::thread gossip_thread_;
    std::thread rebalance_thread_;

    mutable std::mutex routing_mutex_;
    std::shared_ptr<const RoutingTable> routing_;
    std::atomic<uint64_t> routing_epoch_{0};

    mutable std::mutex hints_mutex_;
    std::unordered_map<std::string, std::deque<Hint>> hints_;
    std::condition_variable hints_cv_;
    std::atomic<bool> running_{false};
    std::thread hint_thread_;

    // Declared last so it is destroyed first, before anything its tasks use.
    boost::asio::thread_pool rpc_pool_;
};

} // namespace shardcache

#endif // SHARDCACHE_CLUSTER_HPP
