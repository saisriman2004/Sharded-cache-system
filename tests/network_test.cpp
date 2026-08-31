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
    EXPECT_TRUE(client.set("net_key", "net_val").is_ok());
    auto val = client.get("net_key");
    ASSERT_TRUE(val.is_ok());
    ASSERT_TRUE(val.value.has_value());
    EXPECT_EQ(val.value.value(), "net_val");

    EXPECT_TRUE(client.remove("net_key").is_ok());
    auto missing = client.get("net_key");
    EXPECT_TRUE(missing.is_not_found());
    EXPECT_FALSE(missing.is_ok());

    client.disconnect();

    // After disconnect, operations return ConnectionError, NOT NotFound
    auto disconnected_get = client.get("net_key");
    EXPECT_TRUE(disconnected_get.is_error());
    EXPECT_EQ(disconnected_get.status, shardcache::network::RequestStatus::ConnectionError);
    EXPECT_FALSE(disconnected_get.is_not_found());

    io_context.stop();
    if (io_thread.joinable()) {
        io_thread.join();
    }
}
