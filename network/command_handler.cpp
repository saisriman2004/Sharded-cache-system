#include "command_handler.hpp"
#include "protocol/resp.hpp"

#include <algorithm>
#include <chrono>

namespace shardcache::network {

namespace resp = protocol::resp;
using protocol::CommandType;

namespace {

constexpr const char* kWriteUnavailable = "ERR write quorum not reached";
constexpr const char* kReadUnavailable = "ERR read quorum not reached";

std::optional<std::chrono::milliseconds> to_ttl(const std::optional<uint64_t>& ttl_ms) {
    if (!ttl_ms.has_value()) return std::nullopt;
    return std::chrono::milliseconds(ttl_ms.value());
}

std::string stats_body(Cluster& cluster) {
    auto& cache = cluster.local_cache();
    return "# Stats\r\n"
           "node_id:" + cluster.local_node().id + "\r\n"
           "cluster_nodes:" + std::to_string(cluster.node_count()) + "\r\n"
           "entries:" + std::to_string(cache.size()) + "\r\n"
           "capacity:" + std::to_string(cache.capacity()) + "\r\n"
           "hits:" + std::to_string(cache.total_hits()) + "\r\n"
           "misses:" + std::to_string(cache.total_misses()) + "\r\n"
           "hit_rate:" + std::to_string(cache.overall_hit_rate()) + "%\r\n";
}

} // namespace

CommandHandler::CommandHandler(Cluster& cluster, std::size_t rpc_threads)
    : cluster_(cluster), rpc_pool_(rpc_threads) {}

CommandHandler::~CommandHandler() {
    rpc_pool_.join();
}

bool CommandHandler::runs_locally(const protocol::Command& cmd) const {
    switch (cmd.type) {
        case CommandType::Get:
        case CommandType::Ttl:
        case CommandType::Pttl:
            return cluster_.can_serve_locally(cmd.key, false);
        case CommandType::Exists:
            return cmd.keys.size() == 1 && cluster_.can_serve_locally(cmd.key, false);
        case CommandType::Set:
            return cluster_.can_serve_locally(cmd.key, true);
        case CommandType::Delete:
            return cmd.keys.size() == 1 && cluster_.can_serve_locally(cmd.key, true);
        case CommandType::Expire:
            // Read-modify-write: local only if both halves are.
            return cluster_.can_serve_locally(cmd.key, true) && cluster_.can_serve_locally(cmd.key, false);
        default:
            return true;
    }
}

void CommandHandler::execute(const protocol::Command& cmd, std::string& out) {
    switch (cmd.type) {
        case CommandType::Ping:
            if (cmd.value.empty()) {
                resp::append_simple(out, "PONG");
            } else {
                resp::append_bulk(out, cmd.value);
            }
            return;
        case CommandType::Echo:
            resp::append_bulk(out, cmd.value);
            return;
        case CommandType::Stats:
            resp::append_bulk(out, stats_body(cluster_));
            return;
        case CommandType::DbSize:
            resp::append_integer(out, static_cast<int64_t>(cluster_.local_cache().size()));
            return;
        case CommandType::FlushAll:
            cluster_.local_cache().clear();
            resp::append_simple(out, "OK");
            return;
        case CommandType::CommandDocs:
            resp::append_array_header(out, 0);
            return;
        case CommandType::Config:
            // Persistence settings that Redis tooling probes: report them as empty.
            resp::append_array_header(out, cmd.keys.size() * 2);
            for (const auto& name : cmd.keys) {
                resp::append_bulk(out, name);
                resp::append_bulk(out, "");
            }
            return;
        case CommandType::Select:
            if (cmd.value == "0") {
                resp::append_simple(out, "OK");
            } else {
                resp::append_error(out, "ERR DB index is out of range");
            }
            return;
        case CommandType::Client:
        case CommandType::Quit:
            resp::append_simple(out, "OK");
            return;

        // --- Coordinated commands: quorum reads and writes across replicas ---
        case CommandType::Set: {
            auto result = cluster_.put(cmd.key, cmd.value, to_ttl(cmd.ttl_ms));
            if (result.ok) {
                resp::append_simple(out, "OK");
            } else {
                resp::append_error(out, kWriteUnavailable);
            }
            return;
        }
        case CommandType::Get: {
            auto r = cluster_.read(cmd.key);
            if (r.status == ReadStatus::Found) {
                resp::append_bulk(out, r.value.value);
            } else if (r.status == ReadStatus::NotFound) {
                resp::append_null(out);
            } else {
                resp::append_error(out, kReadUnavailable);
            }
            return;
        }
        case CommandType::Delete: {
            int64_t removed = 0;
            for (const auto& key : cmd.keys) {
                auto result = cluster_.del(key);
                if (!result.ok) {
                    resp::append_error(out, kWriteUnavailable);
                    return;
                }
                removed += result.existed ? 1 : 0;
            }
            resp::append_integer(out, removed);
            return;
        }
        case CommandType::Exists: {
            int64_t found = 0;
            for (const auto& key : cmd.keys) {
                auto r = cluster_.read(key);
                if (r.status == ReadStatus::Unavailable) {
                    resp::append_error(out, kReadUnavailable);
                    return;
                }
                found += r.status == ReadStatus::Found ? 1 : 0;
            }
            resp::append_integer(out, found);
            return;
        }
        case CommandType::Expire: {
            auto result = cluster_.expire_key(cmd.key, std::chrono::milliseconds(cmd.ttl_ms.value_or(0)));
            if (!result.ok) {
                resp::append_error(out, kWriteUnavailable);
            } else {
                resp::append_integer(out, result.existed ? 1 : 0);
            }
            return;
        }
        case CommandType::Ttl:
        case CommandType::Pttl: {
            auto r = cluster_.read(cmd.key);
            if (r.status == ReadStatus::Unavailable) {
                resp::append_error(out, kReadUnavailable);
                return;
            }
            int64_t ms = -2;
            if (r.status == ReadStatus::Found) {
                ms = r.value.ttl.has_value() ? r.value.ttl->count() : -1;
            }
            // TTL rounds up like Redis so a key with 0 < ttl < 1s reports 1, not 0.
            int64_t reply = (cmd.type == CommandType::Ttl && ms > 0) ? (ms + 999) / 1000 : ms;
            resp::append_integer(out, reply);
            return;
        }

        // --- Internal replication RPCs: applied to the local cache only ---
        // These must never be routed through the cluster again.
        case CommandType::ReplSet:
            resp::append_integer(out, cluster_.apply_replica_write(
                cmd.key, VersionedValue{cmd.value, cmd.version, false, to_ttl(cmd.ttl_ms)}) ? 1 : 0);
            return;
        case CommandType::ReplDelete:
            resp::append_integer(out, cluster_.apply_replica_write(
                cmd.key, VersionedValue{"", cmd.version, true, std::nullopt}) ? 1 : 0);
            return;
        case CommandType::ReplGet: {
            auto v = cluster_.replica_read(cmd.key);
            if (!v.has_value()) {
                resp::append_null(out);
                return;
            }
            resp::append_array_header(out, 4);
            resp::append_bulk(out, v->value);
            resp::append_integer(out, static_cast<int64_t>(v->version));
            resp::append_integer(out, v->tombstone ? 1 : 0);
            resp::append_integer(out, v->ttl.has_value() ? std::max<int64_t>(1, v->ttl->count()) : -1);
            return;
        }

        case CommandType::Unknown:
            resp::append_error(out, cmd.error.empty() ? "ERR unknown command" : cmd.error);
            return;
    }
}

} // namespace shardcache::network
