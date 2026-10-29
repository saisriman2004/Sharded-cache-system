#include "command_handler.hpp"
#include "protocol/resp.hpp"

#include <chrono>

namespace shardcache::network {

namespace resp = protocol::resp;
using protocol::CommandType;

namespace {

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

        // --- External commands routed through Cluster ---
        case CommandType::Set:
            if (cluster_.set(cmd.key, cmd.value, to_ttl(cmd.ttl_ms))) {
                resp::append_simple(out, "OK");
            } else {
                resp::append_error(out, "ERR write failed: replication not acknowledged");
            }
            return;
        case CommandType::Get: {
            auto val = cluster_.get(cmd.key);
            if (val.has_value()) {
                resp::append_bulk(out, val.value());
            } else {
                resp::append_null(out);
            }
            return;
        }
        case CommandType::Delete: {
            int64_t removed = 0;
            for (const auto& key : cmd.keys) {
                removed += cluster_.remove(key) ? 1 : 0;
            }
            resp::append_integer(out, removed);
            return;
        }
        case CommandType::Exists: {
            int64_t found = 0;
            for (const auto& key : cmd.keys) {
                found += cluster_.exists(key) ? 1 : 0;
            }
            resp::append_integer(out, found);
            return;
        }
        case CommandType::Expire:
            resp::append_integer(out, cluster_.expire(cmd.key, std::chrono::milliseconds(cmd.ttl_ms.value_or(0))) ? 1 : 0);
            return;
        case CommandType::Ttl: {
            int64_t ms = cluster_.pttl(cmd.key);
            // Round up like Redis so a key with 0 < ttl < 1s reports 1, not 0.
            resp::append_integer(out, ms < 0 ? ms : (ms + 999) / 1000);
            return;
        }
        case CommandType::Pttl:
            resp::append_integer(out, cluster_.pttl(cmd.key));
            return;

        // --- Internal replication commands: write DIRECTLY to local cache ---
        // These must never be routed through Cluster again.
        case CommandType::ReplSet:
            cluster_.local_cache().set(cmd.key, cmd.value, to_ttl(cmd.ttl_ms));
            resp::append_simple(out, "OK");
            return;
        case CommandType::ReplDelete:
            cluster_.local_cache().remove(cmd.key);
            resp::append_simple(out, "OK");
            return;
        case CommandType::ReplExpire:
            cluster_.local_cache().expire(cmd.key, std::chrono::milliseconds(cmd.ttl_ms.value_or(0)));
            resp::append_simple(out, "OK");
            return;

        case CommandType::Unknown:
            resp::append_error(out, cmd.error.empty() ? "ERR unknown command" : cmd.error);
            return;
    }
}

} // namespace shardcache::network
