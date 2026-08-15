#include "shardcache/sharded_cache.hpp"
#include "shardcache/logger.hpp"
#include "tcp_server.hpp"

#include <iostream>
#include <boost/asio.hpp>
#include <cstdlib>

int main(int argc, char* argv[]) {
    uint16_t port = 7001;
    if (argc > 1) {
        port = static_cast<uint16_t>(std::atoi(argv[1]));
    }

    shardcache::Logger::instance().info("Starting ShardCache Distributed Engine...");

    shardcache::ShardedCache cache(100000, 16);

    cache.set("welcome", "ShardCache-v1.0-C++20");

    try {
        boost::asio::io_context io_context;
        shardcache::network::TCPServer server(io_context, port, cache);

        shardcache::Logger::instance().info("ShardCache Engine running. Listening for client connections on port " + std::to_string(port) + "...");
        io_context.run();
    } catch (const std::exception& e) {
        shardcache::Logger::instance().error("Fatal server error: " + std::string(e.what()));
        return 1;
    }

    return 0;
}
