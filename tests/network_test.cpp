#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"
#include "tcp_server.hpp"
#include "client.hpp"

#include <thread>
#include <chrono>

TEST(NetworkTest, ServerClientInteraction) {
    shardcache::Cluster cluster("test_node", 2, 1000);
    boost::asio::io_context io_context;

    constexpr uint16_t test_port = 7099;
    auto server = std::make_unique<shardcache::network::TCPServer>(io_context, test_port, cluster);

    std::thread io_thread([&io_context]() {
        io_context.run();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    shardcache::network::Client client("127.0.0.1", test_port);
    ASSERT_TRUE(client.connect());

    EXPECT_TRUE(client.ping());
    EXPECT_TRUE(client.set("net_key", "net_val"));
    auto val = client.get("net_key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(val.value(), "net_val");

    EXPECT_TRUE(client.remove("net_key"));
    EXPECT_FALSE(client.get("net_key").has_value());

    client.disconnect();

    io_context.stop();
    if (io_thread.joinable()) {
        io_thread.join();
    }
}
