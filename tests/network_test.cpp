#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"
#include "tcp_server.hpp"
#include "client.hpp"
#include "metrics_server.hpp"
#include <array>

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

TEST(NetworkTest, ConnectTimeout) {
    // Attempting to connect to an unreachable black-hole address with 100ms timeout
    shardcache::network::Client client("10.255.255.1", 81, std::chrono::milliseconds(100), std::chrono::milliseconds(100));
    auto start = std::chrono::steady_clock::now();
    bool connected = client.connect();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start);
    EXPECT_FALSE(connected);
    EXPECT_LT(elapsed.count(), 2000);
}

TEST(NetworkTest, MetricsEndpointServesPrometheusText) {
    shardcache::Cluster cluster("metrics_node", 1, 1000);
    boost::asio::io_context io_context;
    shardcache::network::TCPServer server(io_context, 7097, cluster);
    shardcache::network::MetricsServer metrics(io_context, 7098, server.handler());
    std::thread io_thread([&io_context]() { io_context.run(); });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));

    shardcache::network::Client client("127.0.0.1", 7097);
    ASSERT_TRUE(client.connect());
    ASSERT_TRUE(client.set("m", "1").is_ok());
    ASSERT_TRUE(client.get("m").is_ok());
    EXPECT_TRUE(client.get("missing").is_not_found());

    auto http_get = [](const std::string& path) {
        boost::asio::io_context io;
        boost::asio::ip::tcp::socket sock(io);
        sock.connect({boost::asio::ip::make_address("127.0.0.1"), 7098});
        std::string req = "GET " + path + " HTTP/1.1\r\nHost: x\r\n\r\n";
        boost::asio::write(sock, boost::asio::buffer(req));
        std::string body;
        boost::system::error_code ec;
        std::array<char, 4096> buf{};
        while (std::size_t n = sock.read_some(boost::asio::buffer(buf), ec)) {
            body.append(buf.data(), n);
            if (ec) break;
        }
        return body;
    };

    auto text = http_get("/metrics");
    EXPECT_NE(text.find("HTTP/1.1 200 OK"), std::string::npos);
    EXPECT_NE(text.find("shardcache_commands_total{cmd=\"set\"} 1\n"), std::string::npos);
    EXPECT_NE(text.find("shardcache_commands_total{cmd=\"get\"} 2\n"), std::string::npos);
    EXPECT_NE(text.find("shardcache_cache_hits_total 1\n"), std::string::npos);
    EXPECT_NE(text.find("shardcache_cache_misses_total 1\n"), std::string::npos);
    EXPECT_NE(text.find("shardcache_command_duration_seconds_count{cmd=\"get\"} 2\n"), std::string::npos);
    EXPECT_NE(http_get("/health").find("\r\n\r\nok\n"), std::string::npos);
    EXPECT_NE(http_get("/nope").find("404"), std::string::npos);

    client.disconnect();
    io_context.stop();
    io_thread.join();
}
