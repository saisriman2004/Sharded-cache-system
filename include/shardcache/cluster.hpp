#ifndef SHARDCACHE_CLUSTER_HPP
#define SHARDCACHE_CLUSTER_HPP

#include "shardcache/consistent_hash.hpp"
#include "shardcache/sharded_cache.hpp"
#include "shardcache/health_checker.hpp"
#include "shardcache/hybrid_clock.hpp"

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

namespace network { class PeerPool; }

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
    std::size_t max_hints_per_node{100000};
};

enum class ReadStatus { Found, NotFound, Unavailable };

struct ReadResult {
    ReadStatus status{ReadStatus::Unavailable};
    VersionedValue value{};
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

    void init_common();
    WriteResult replicate(const std::string& key, const VersionedValue& v);
    bool send_write(const CacheNode& node, const std::string& key, const VersionedValue& v, bool* was_live);
    void read_repair(const std::string& key, const VersionedValue& winner, const std::vector<CacheNode>& stale);
    void add_hint(const CacheNode& node, const std::string& key, const VersionedValue& v);
    void hint_loop();
    std::size_t effective_quorum(std::size_t configured, std::size_t replicas) const;

    ClusterConfig config_;
    CacheNode local_node_;
    HybridClock clock_;
    std::shared_ptr<network::PeerPool> peer_pool_;
    ConsistentHashRing ring_;
    ShardedCache local_cache_;
    std::unique_ptr<HealthChecker> health_checker_;
    std::unordered_map<std::string, CacheNode> nodes_;
    std::unordered_map<std::string, bool> node_health_;
    mutable std::shared_mutex membership_mutex_;

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
