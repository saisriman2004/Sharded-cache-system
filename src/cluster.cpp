#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"
#include "network/peer_pool.hpp"

#include <boost/asio/post.hpp>

namespace shardcache {

namespace {

ClusterConfig legacy_config(std::size_t replication_factor, std::size_t cache_capacity) {
    ClusterConfig config;
    config.replication_factor = replication_factor;
    config.cache_capacity = cache_capacity;
    return config;
}

// Last-writer-wins comparison between two replica responses.
bool newer(const VersionedValue& a, const VersionedValue& b) {
    return supersedes(a.version, a.tombstone, a.value, CacheEntry{b.value, std::nullopt, b.version, b.tombstone});
}

bool same_version(const VersionedValue& a, const VersionedValue& b) {
    return a.version == b.version && a.tombstone == b.tombstone && a.value == b.value;
}

// Epochs are unique across all Cluster instances, so a thread's cached table
// can never be mistaken for another cluster's.
std::atomic<uint64_t> g_routing_epoch{0};

struct ThreadRouting {
    uint64_t epoch{0};
    std::shared_ptr<const RoutingTable> table;
};
thread_local ThreadRouting t_routing;

} // namespace

const RoutingTable& Cluster::routing() const {
    if (t_routing.epoch != routing_epoch_.load(std::memory_order_acquire)) {
        std::lock_guard lock(routing_mutex_);
        t_routing.table = routing_;
        t_routing.epoch = routing_epoch_.load(std::memory_order_relaxed);
    }
    return *t_routing.table;
}

void Cluster::publish_routing_locked() {
    auto table = std::make_shared<const RoutingTable>(ring_, config_.replication_factor, local_node_.id);
    std::lock_guard lock(routing_mutex_);
    routing_ = std::move(table);
    routing_epoch_.store(g_routing_epoch.fetch_add(1) + 1, std::memory_order_release);
}

void Cluster::init_common() {
    health_checker_ = std::make_unique<HealthChecker>([this](const std::string& node_id, bool healthy) {
        set_node_health(node_id, healthy);
    }, config_.health_check_interval);

    ring_.add_node(local_node_);
    nodes_[local_node_.id] = local_node_;
    node_health_[local_node_.id] = true;
    publish_routing_locked();
}

Cluster::Cluster(const CacheNode& local_node, ClusterConfig config)
    : config_(config),
      local_node_(local_node),
      peer_pool_(std::make_shared<network::PeerPool>(64, std::chrono::milliseconds(500), config.rpc_timeout)),
      ring_(100),
      local_cache_(config.cache_capacity, 16, std::chrono::milliseconds(1000), config.tombstone_ttl),
      rpc_pool_(config.rpc_threads) {
    init_common();
}

Cluster::Cluster(const CacheNode& local_node, std::size_t replication_factor, std::size_t cache_capacity)
    : Cluster(local_node, legacy_config(replication_factor, cache_capacity)) {}

Cluster::Cluster(const std::string& local_node_id, std::size_t replication_factor, std::size_t cache_capacity)
    : Cluster(CacheNode{local_node_id, "127.0.0.1", 7001}, legacy_config(replication_factor, cache_capacity)) {}

Cluster::~Cluster() {
    stop();
    rpc_pool_.join();
}

void Cluster::start() {
    if (health_checker_) {
        health_checker_->start();
    }
    if (!running_.exchange(true)) {
        hint_thread_ = std::thread([this]() { hint_loop(); });
    }
}

void Cluster::stop() {
    if (health_checker_) {
        health_checker_->stop();
    }
    if (running_.exchange(false)) {
        {
            std::lock_guard lock(hints_mutex_);
        }
        hints_cv_.notify_all();
        if (hint_thread_.joinable()) hint_thread_.join();
    }
}

void Cluster::add_node(const CacheNode& node) {
    {
        std::unique_lock lock(membership_mutex_);
        nodes_[node.id] = node;
        ring_.add_node(node);
        node_health_[node.id] = true;
        publish_routing_locked();
    }
    if (node.id != local_node_.id) {
        health_checker_->add_node(node);
    }
}

void Cluster::remove_node(const std::string& node_id) {
    {
        std::unique_lock lock(membership_mutex_);
        ring_.remove_node(node_id);
        nodes_.erase(node_id);
        node_health_.erase(node_id);
        publish_routing_locked();
    }
    health_checker_->remove_node(node_id);
}

std::size_t Cluster::node_count() const {
    return routing().node_count();
}

std::vector<CacheNode> Cluster::preference_list(const std::string& key) const {
    return routing().preference_list(key);
}

std::optional<CacheNode> Cluster::locate(const std::string& key) const {
    // First healthy node of the preference list (the "primary" for routing views).
    auto replicas = preference_list(key);
    std::shared_lock lock(membership_mutex_);
    for (const auto& node : replicas) {
        auto it = node_health_.find(node.id);
        if (it == node_health_.end() || it->second) {
            return node;
        }
    }
    if (!replicas.empty()) return replicas.front();
    return std::nullopt;
}

std::vector<CacheNode> Cluster::locate_replicas(const std::string& key, std::size_t count) const {
    std::shared_lock lock(membership_mutex_);
    return ring_.locate_replicas(key, count);
}

bool Cluster::is_node_healthy(const std::string& node_id) const {
    std::shared_lock lock(membership_mutex_);
    auto it = node_health_.find(node_id);
    if (it != node_health_.end()) {
        return it->second;
    }
    return true;
}

void Cluster::set_node_health(const std::string& node_id, bool healthy) {
    std::optional<CacheNode> node;
    {
        std::unique_lock lock(membership_mutex_);
        node_health_[node_id] = healthy;
        if (auto it = nodes_.find(node_id); it != nodes_.end()) {
            node = it->second;
        }
    }
    if (!healthy) {
        Logger::instance().warning("Node " + node_id + " marked unhealthy in cluster routing.");
        if (node) peer_pool_->drop(node->host, node->port);
    } else {
        Logger::instance().info("Node " + node_id + " marked healthy in cluster routing.");
        hints_cv_.notify_all();
    }
}

std::size_t Cluster::effective_quorum(std::size_t configured, std::size_t replicas) const {
    std::size_t q = configured != 0 ? configured : config_.replication_factor / 2 + 1;
    return std::max<std::size_t>(1, std::min(q, replicas));
}

bool Cluster::can_serve_locally(const std::string& key, bool is_write) const {
    const auto& table = routing();
    if (table.empty()) return false;
    const std::size_t point = table.point_for(key);
    if (!table.local_is_replica(point)) return false;
    const std::size_t replicas = table.replicas(point).size();
    if (is_write || config_.consistency == Consistency::Linearizable) return replicas == 1;
    return effective_quorum(config_.read_quorum, replicas) == 1;
}

// ---------------------------------------------------------------------------
// Writes

bool Cluster::send_write(const CacheNode& node, const std::string& key, const VersionedValue& v, bool* was_live) {
    auto client = peer_pool_->acquire(node.host, node.port);
    if (!client) return false;
    auto res = (*client)->replica_write(key, v);
    if (!res.is_ok()) {
        if (Logger::instance().enabled(LogLevel::DEBUG)) {
            Logger::instance().debug("Replica write to " + node.id + " failed: " + res.message);
        }
        return false;
    }
    if (was_live) *was_live = res.value.value_or(false);
    return true;
}

WriteResult Cluster::replicate(const std::string& key, const VersionedValue& v) {
    if (can_serve_locally(key, true)) {
        bool live = false;
        local_cache_.apply(key, v, &live);
        return WriteResult{true, live, 1};
    }
    const auto targets = preference_list(key);
    if (targets.empty()) return {};
    const std::size_t needed = effective_quorum(config_.write_quorum, targets.size());

    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        std::size_t done{0};
        std::size_t acks{0};
        bool existed{false};
    };
    auto state = std::make_shared<State>();
    auto record = [state](bool ok, bool live) {
        std::lock_guard lock(state->mutex);
        ++state->done;
        if (ok) {
            ++state->acks;
            state->existed = state->existed || live;
        }
        state->cv.notify_all();
    };

    for (const auto& node : targets) {
        if (node.id == local_node_.id) {
            bool live = false;
            local_cache_.apply(key, v, &live);
            record(true, live);
        } else if (!is_node_healthy(node.id)) {
            add_hint(node, key, v);
            record(false, false);
        } else {
            boost::asio::post(rpc_pool_, [this, node, key, v, record]() {
                bool live = false;
                bool ok = send_write(node, key, v, &live);
                if (!ok) add_hint(node, key, v);
                record(ok, live);
            });
        }
    }

    std::unique_lock lock(state->mutex);
    state->cv.wait_for(lock, config_.rpc_timeout * 2, [&]() {
        return state->acks >= needed || state->done == targets.size();
    });
    return WriteResult{state->acks >= needed, state->existed, state->acks};
}

std::optional<uint64_t> Cluster::next_version(const std::string& key) {
    if (config_.consistency == Consistency::Linearizable && !can_serve_locally(key, true)) {
        // ABD write phase 1: learn the highest version a read quorum holds and
        // write above it. Without this, a coordinator that missed an earlier,
        // completed write could stamp a lower version and silently lose.
        auto replies = collect(key);
        if (!replies.ok) return std::nullopt;
        for (std::size_t i = 0; i < replies.values.size(); ++i) {
            if (replies.replied[i] && replies.values[i].has_value()) {
                clock_.observe(replies.values[i]->version);
            }
        }
    }
    return clock_.now();
}

WriteResult Cluster::put(const std::string& key, const std::string& value,
                         std::optional<std::chrono::milliseconds> ttl) {
    auto version = next_version(key);
    if (!version) return {};
    return replicate(key, VersionedValue{value, version.value(), false, ttl});
}

WriteResult Cluster::del(const std::string& key) {
    auto version = next_version(key);
    if (!version) return {};
    return replicate(key, VersionedValue{"", version.value(), true, std::nullopt});
}

WriteResult Cluster::expire_key(const std::string& key, std::chrono::milliseconds ttl) {
    // The read already observes the newest version, so it doubles as the
    // version query of a linearizable write.
    auto current = read(key);
    if (current.status == ReadStatus::Unavailable) return {};
    if (current.status == ReadStatus::NotFound) return WriteResult{true, false, 0};
    auto result = replicate(key, VersionedValue{current.value.value, clock_.now(), false, ttl});
    result.existed = true;
    return result;
}

// ---------------------------------------------------------------------------
// Reads

Cluster::QuorumReplies Cluster::collect(const std::string& key) {
    QuorumReplies out;
    out.targets = preference_list(key);
    if (out.targets.empty()) return out;
    const std::size_t needed = effective_quorum(config_.read_quorum, out.targets.size());

    struct State {
        std::mutex mutex;
        std::condition_variable cv;
        std::size_t done{0};
        std::size_t replies{0};
        std::vector<std::optional<VersionedValue>> values;
        std::vector<bool> replied;
    };
    auto state = std::make_shared<State>();
    state->values.resize(out.targets.size());
    state->replied.resize(out.targets.size(), false);

    auto record = [state](std::size_t i, bool ok, std::optional<VersionedValue> value) {
        std::lock_guard lock(state->mutex);
        ++state->done;
        if (ok) {
            ++state->replies;
            state->replied[i] = true;
            state->values[i] = std::move(value);
        }
        state->cv.notify_all();
    };

    for (std::size_t i = 0; i < out.targets.size(); ++i) {
        const auto& node = out.targets[i];
        if (node.id == local_node_.id) {
            record(i, true, local_cache_.get_versioned(key));
        } else if (!is_node_healthy(node.id)) {
            record(i, false, std::nullopt);
        } else {
            boost::asio::post(rpc_pool_, [this, i, node, key, record]() {
                auto client = peer_pool_->acquire(node.host, node.port);
                if (!client) {
                    record(i, false, std::nullopt);
                    return;
                }
                auto res = (*client)->replica_get(key);
                if (res.is_ok()) {
                    record(i, true, std::move(res.value));
                } else {
                    record(i, res.is_not_found(), std::nullopt);
                }
            });
        }
    }

    std::unique_lock lock(state->mutex);
    state->cv.wait_for(lock, config_.rpc_timeout * 2, [&]() {
        return state->replies >= needed || state->done == out.targets.size();
    });
    out.ok = state->replies >= needed;
    out.values = state->values;
    out.replied = state->replied;
    return out;
}

ReadResult Cluster::read(const std::string& key) {
    auto to_result = [](std::optional<VersionedValue>&& v) {
        if (!v.has_value() || v->tombstone) return ReadResult{ReadStatus::NotFound, {}};
        return ReadResult{ReadStatus::Found, std::move(v.value())};
    };

    // Fast path: this node's copy alone satisfies the read quorum.
    if (can_serve_locally(key, false)) {
        return to_result(local_cache_.get_versioned(key));
    }

    auto replies = collect(key);
    if (!replies.ok) return {};
    const auto& targets = replies.targets;

    std::optional<VersionedValue> winner;
    for (std::size_t i = 0; i < targets.size(); ++i) {
        if (replies.replied[i] && replies.values[i].has_value() &&
            (!winner || newer(replies.values[i].value(), winner.value()))) {
            winner = replies.values[i];
        }
    }
    if (!winner.has_value()) return to_result(std::move(winner));

    clock_.observe(winner->version);
    std::vector<CacheNode> stale;
    std::size_t up_to_date = 0;
    for (std::size_t i = 0; i < targets.size(); ++i) {
        if (!replies.replied[i]) continue;
        if (replies.values[i].has_value() && same_version(replies.values[i].value(), winner.value())) {
            ++up_to_date;
        } else {
            stale.push_back(targets[i]);
        }
    }

    const std::size_t write_quorum = effective_quorum(config_.write_quorum, targets.size());
    if (config_.consistency == Consistency::Linearizable && up_to_date < write_quorum) {
        // ABD read phase 2: the winner may come from a write still in flight.
        // Before returning it, make sure a write quorum stores it, so that no
        // later read can observe an older value.
        if (!replicate(key, winner.value()).ok) return {};
    } else if (!stale.empty()) {
        read_repair(key, winner.value(), stale);
    }
    return to_result(std::move(winner));
}

void Cluster::read_repair(const std::string& key, const VersionedValue& winner, const std::vector<CacheNode>& stale) {
    for (const auto& node : stale) {
        if (node.id == local_node_.id) {
            local_cache_.apply(key, winner);
            continue;
        }
        boost::asio::post(rpc_pool_, [this, node, key, winner]() {
            send_write(node, key, winner, nullptr);
        });
    }
}

// ---------------------------------------------------------------------------
// Convenience wrappers

bool Cluster::set(const std::string& key, const std::string& value, std::optional<std::chrono::milliseconds> ttl) {
    return put(key, value, ttl).ok;
}

std::optional<std::string> Cluster::get(const std::string& key) {
    auto r = read(key);
    if (r.status != ReadStatus::Found) return std::nullopt;
    return std::move(r.value.value);
}

bool Cluster::exists(const std::string& key) {
    return read(key).status == ReadStatus::Found;
}

bool Cluster::remove(const std::string& key) {
    auto r = del(key);
    return r.ok && r.existed;
}

bool Cluster::expire(const std::string& key, std::chrono::milliseconds ttl) {
    auto r = expire_key(key, ttl);
    return r.ok && r.existed;
}

int64_t Cluster::pttl(const std::string& key) {
    auto r = read(key);
    if (r.status != ReadStatus::Found) return -2;
    return r.value.ttl.has_value() ? r.value.ttl->count() : -1;
}

std::string Cluster::get_or_load(
    const std::string& key,
    std::function<std::string()> loader,
    std::optional<std::chrono::milliseconds> ttl
) {
    return local_cache_.get_or_load(key, loader, ttl);
}

// ---------------------------------------------------------------------------
// Replica side

bool Cluster::apply_replica_write(const std::string& key, const VersionedValue& v) {
    clock_.observe(v.version);
    bool was_live = false;
    local_cache_.apply(key, v, &was_live);
    return was_live;
}

std::optional<VersionedValue> Cluster::replica_read(const std::string& key) {
    return local_cache_.get_versioned(key);
}

// ---------------------------------------------------------------------------
// Hinted handoff

void Cluster::add_hint(const CacheNode& node, const std::string& key, const VersionedValue& v) {
    std::optional<std::chrono::steady_clock::time_point> deadline;
    if (v.ttl.has_value()) deadline = std::chrono::steady_clock::now() + v.ttl.value();

    std::lock_guard lock(hints_mutex_);
    auto& queue = hints_[node.id];
    if (queue.size() >= config_.max_hints_per_node) {
        queue.pop_front();
    }
    queue.push_back(Hint{key, v, deadline});
}

std::size_t Cluster::pending_hints() const {
    std::lock_guard lock(hints_mutex_);
    std::size_t total = 0;
    for (const auto& [_, queue] : hints_) total += queue.size();
    return total;
}

void Cluster::replay_hints() {
    constexpr std::size_t kBatch = 1024;

    std::vector<std::string> node_ids;
    {
        std::lock_guard lock(hints_mutex_);
        for (const auto& [id, queue] : hints_) {
            if (!queue.empty()) node_ids.push_back(id);
        }
    }

    for (const auto& id : node_ids) {
        if (!is_node_healthy(id)) continue;
        std::optional<CacheNode> node;
        {
            std::shared_lock lock(membership_mutex_);
            if (auto it = nodes_.find(id); it != nodes_.end()) node = it->second;
        }

        std::deque<Hint> batch;
        {
            std::lock_guard lock(hints_mutex_);
            auto& queue = hints_[id];
            if (!node) {
                queue.clear();  // node left the cluster
                continue;
            }
            while (!queue.empty() && batch.size() < kBatch) {
                batch.push_back(std::move(queue.front()));
                queue.pop_front();
            }
        }

        const auto now = std::chrono::steady_clock::now();
        std::size_t delivered = 0;
        while (!batch.empty()) {
            Hint& hint = batch.front();
            if (hint.deadline.has_value()) {
                if (hint.deadline.value() <= now) {
                    batch.pop_front();  // expired while queued
                    continue;
                }
                hint.value.ttl = std::chrono::duration_cast<std::chrono::milliseconds>(hint.deadline.value() - now);
            }
            if (!send_write(node.value(), hint.key, hint.value, nullptr)) {
                break;
            }
            batch.pop_front();
            ++delivered;
        }

        if (!batch.empty()) {
            // Peer failed mid-replay: put the rest back, oldest first.
            std::lock_guard lock(hints_mutex_);
            auto& queue = hints_[id];
            while (!batch.empty()) {
                queue.push_front(std::move(batch.back()));
                batch.pop_back();
            }
        }
        if (delivered > 0) {
            Logger::instance().info("Hinted handoff delivered " + std::to_string(delivered) + " writes to " + id);
        }
    }
}

void Cluster::hint_loop() {
    while (running_.load()) {
        {
            std::unique_lock lock(hints_mutex_);
            hints_cv_.wait_for(lock, std::chrono::milliseconds(500), [this]() { return !running_.load(); });
        }
        if (!running_.load()) break;
        replay_hints();
    }
}

} // namespace shardcache
