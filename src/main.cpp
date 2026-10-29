#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"
#include "tcp_server.hpp"
#include "io_context_pool.hpp"

#include <iostream>
#include <sstream>
#include <thread>
#include <vector>
#include <boost/asio.hpp>
#include <cstdlib>
#include <algorithm>
#include <csignal>

int main(int argc, char* argv[]) {
    std::string node_id = "nodeA";
    uint16_t port = 7001;
    std::string advertise_host = "127.0.0.1";
    std::string peers_str = "";

    if (const char* env_node = std::getenv("NODE_ID")) {
        node_id = env_node;
    }
    if (const char* env_port = std::getenv("PORT")) {
        port = static_cast<uint16_t>(std::atoi(env_port));
    }
    if (const char* env_host = std::getenv("ADVERTISE_HOST")) {
        advertise_host = env_host;
    }
    if (const char* env_peers = std::getenv("CLUSTER_PEERS")) {
        peers_str = env_peers;
    }
    std::size_t io_threads = std::max(1u, std::thread::hardware_concurrency());
    if (const char* env = std::getenv("IO_THREADS")) {
        io_threads = static_cast<std::size_t>(std::max(1, std::atoi(env)));
    }
    std::size_t rpc_threads = 64;
    if (const char* env = std::getenv("RPC_THREADS")) {
        rpc_threads = static_cast<std::size_t>(std::max(1, std::atoi(env)));
    }
    if (const char* env = std::getenv("LOG_LEVEL")) {
        std::string lvl = env;
        if (lvl == "debug") shardcache::Logger::instance().set_level(shardcache::LogLevel::DEBUG);
        if (lvl == "warn") shardcache::Logger::instance().set_level(shardcache::LogLevel::WARNING);
        if (lvl == "error") shardcache::Logger::instance().set_level(shardcache::LogLevel::ERR);
    }

    if (argc > 1) {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }
    if (argc > 2) {
        node_id = argv[2];
    }
    if (argc > 3) {
        peers_str = argv[3];
    }

    shardcache::Logger::instance().info("Starting ShardCache Distributed Engine [Node: " + node_id + ", Port: " + std::to_string(port) + ", Advertise: " + advertise_host + "]...");


    auto env_size = [](const char* name, std::size_t fallback) {
        const char* v = std::getenv(name);
        return v ? static_cast<std::size_t>(std::max(0, std::atoi(v))) : fallback;
    };
    shardcache::ClusterConfig config;
    config.replication_factor = std::max<std::size_t>(1, env_size("REPLICATION_FACTOR", 3));
    config.read_quorum = env_size("READ_QUORUM", 0);
    config.write_quorum = env_size("WRITE_QUORUM", 0);
    config.cache_capacity = env_size("CACHE_CAPACITY", 1000000);
    if (const char* mode = std::getenv("CONSISTENCY")) {
        std::string m = mode;
        if (m == "linearizable") {
            config.consistency = shardcache::Consistency::Linearizable;
        } else if (m != "quorum") {
            shardcache::Logger::instance().error("Unknown CONSISTENCY '" + m + "' (expected quorum or linearizable)");
            return 1;
        }
    }

    shardcache::CacheNode local_node{node_id, advertise_host, port};
    shardcache::Cluster cluster(local_node, config);

    if (!peers_str.empty()) {
        std::istringstream iss(peers_str);
        std::string peer_item;
        while (std::getline(iss, peer_item, ',')) {
            std::istringstream pss(peer_item);
            std::string p_id, p_host, p_port_str;
            if (std::getline(pss, p_id, ':') && std::getline(pss, p_host, ':') && std::getline(pss, p_port_str)) {
                uint16_t p_port = static_cast<uint16_t>(std::atoi(p_port_str.c_str()));
                if (p_id != node_id) {
                    shardcache::Logger::instance().info("Registering peer node " + p_id + " (" + p_host + ":" + std::to_string(p_port) + ")");
                    cluster.add_node({p_id, p_host, p_port});
                }
            }
        }
    }

    if (const char* env_seeds = std::getenv("SEEDS")) {
        // Join an existing cluster through any of its members ("host:port,...").
        std::istringstream iss(env_seeds);
        std::string seed;
        while (std::getline(iss, seed, ',')) {
            auto colon = seed.rfind(':');
            if (colon == std::string::npos) continue;
            cluster.add_seed(seed.substr(0, colon), static_cast<uint16_t>(std::atoi(seed.c_str() + colon + 1)));
            shardcache::Logger::instance().info("Joining via seed " + seed);
        }
    }

    cluster.start();

    try {
        // The acceptor gets its own context; connections are spread across
        // one io_context per I/O thread.
        boost::asio::io_context accept_context;
        shardcache::network::IoContextPool io_pool(io_threads);
        shardcache::network::TCPServer server(accept_context, port, cluster, &io_pool, rpc_threads);

        shardcache::Logger::instance().info("ShardCache Engine running with " + std::to_string(io_threads) +
                                            " I/O threads and " + std::to_string(rpc_threads) +
                                            " RPC threads. Listening on port " + std::to_string(port) + "...");
        // SIGTERM/SIGINT: leave gracefully (hand data to the remaining nodes)
        // unless LEAVE_ON_SHUTDOWN=0, then stop serving.
        boost::asio::signal_set signals(accept_context, SIGINT, SIGTERM);
        signals.async_wait([&](const boost::system::error_code& ec, int /*signo*/) {
            if (ec) return;
            const char* leave = std::getenv("LEAVE_ON_SHUTDOWN");
            if (!leave || std::string(leave) != "0") {
                cluster.leave();
            }
            cluster.stop();
            io_pool.stop();
            accept_context.stop();
        });

        io_pool.start();
        accept_context.run();
        io_pool.join();
    } catch (const std::exception& e) {
        shardcache::Logger::instance().error("Fatal server error: " + std::string(e.what()));
        return 1;
    }

    return 0;
}
