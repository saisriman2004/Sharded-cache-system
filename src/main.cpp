#include "shardcache/cluster.hpp"
#include "shardcache/logger.hpp"
#include "tcp_server.hpp"

#include <iostream>
#include <sstream>
#include <boost/asio.hpp>
#include <cstdlib>

int main(int argc, char* argv[]) {
    std::string node_id = "nodeA";
    uint16_t port = 7001;
    std::string peers_str = "";

    if (const char* env_node = std::getenv("NODE_ID")) {
        node_id = env_node;
    }
    if (const char* env_port = std::getenv("PORT")) {
        port = static_cast<uint16_t>(std::atoi(env_port));
    }
    if (const char* env_peers = std::getenv("CLUSTER_PEERS")) {
        peers_str = env_peers;
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

    shardcache::Logger::instance().info("Starting ShardCache Distributed Engine [Node: " + node_id + ", Port: " + std::to_string(port) + "]...");

    shardcache::Cluster cluster(node_id, 2, 100000);

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

    cluster.local_cache().set("welcome", "ShardCache-v1.0-C++20");

    try {
        boost::asio::io_context io_context;
        shardcache::network::TCPServer server(io_context, port, cluster.local_cache());

        shardcache::Logger::instance().info("ShardCache Engine running. Listening on port " + std::to_string(port) + "...");
        io_context.run();
    } catch (const std::exception& e) {
        shardcache::Logger::instance().error("Fatal server error: " + std::string(e.what()));
        return 1;
    }

    return 0;
}
