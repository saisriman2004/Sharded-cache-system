#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"
#include "tcp_server.hpp"
#include "client.hpp"

#include <thread>
#include <vector>
#include <memory>
#include <chrono>

using shardcache::ReadStatus;
using shardcache::VersionedValue;

// Three real nodes on localhost, each a Cluster + TCPServer, with N = 3,
// R = W = 2. Nodes can be stopped and restarted to simulate failures while
// keeping their in-memory state (like a network partition healing).
class ThreeNodeClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
        nodes_ = {
            {"nodeA", "127.0.0.1", 17001},
            {"nodeB", "127.0.0.1", 17002},
            {"nodeC", "127.0.0.1", 17003}
        };

        shardcache::ClusterConfig config;
        config.replication_factor = 3;
        config.cache_capacity = 1000;
        config.rpc_timeout = std::chrono::milliseconds(300);

        for (std::size_t i = 0; i < 3; ++i) {
            clusters_.push_back(std::make_unique<shardcache::Cluster>(nodes_[i], config));
        }
        for (std::size_t i = 0; i < 3; ++i) {
            for (std::size_t j = 0; j < 3; ++j) {
                if (i != j) clusters_[i]->add_node(nodes_[j]);
            }
        }

        servers_.resize(3);
        for (std::size_t i = 0; i < 3; ++i) {
            start_node(i);
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    void TearDown() override {
        for (std::size_t i = 0; i < 3; ++i) {
            stop_node(i);
        }
    }

    void start_node(std::size_t i) {
        auto& s = servers_[i];
        s.io_context = std::make_unique<boost::asio::io_context>();
        s.server = std::make_unique<shardcache::network::TCPServer>(*s.io_context, nodes_[i].port, *clusters_[i]);
        boost::asio::io_context* ctx = s.io_context.get();
        for (int t = 0; t < 2; ++t) {
            s.threads.emplace_back([ctx]() { ctx->run(); });
        }
    }

    // Closes the listener and stops serving; the node's data survives.
    void stop_node(std::size_t i) {
        auto& s = servers_[i];
        if (!s.io_context) return;
        s.io_context->stop();
        for (auto& th : s.threads) {
            if (th.joinable()) th.join();
        }
        s.threads.clear();
        s.server.reset();
        s.io_context.reset();
    }

    // Tells the surviving nodes what the failure detector would eventually conclude.
    void mark_health(std::size_t i, bool healthy) {
        for (std::size_t j = 0; j < 3; ++j) {
            if (j != i) clusters_[j]->set_node_health(nodes_[i].id, healthy);
        }
    }

    std::unique_ptr<shardcache::network::Client> connect(std::size_t i) {
        auto c = std::make_unique<shardcache::network::Client>("127.0.0.1", nodes_[i].port);
        EXPECT_TRUE(c->connect());
        return c;
    }

    // Replication beyond the write quorum completes asynchronously.
    bool eventually(const std::function<bool()>& pred) {
        for (int i = 0; i < 100; ++i) {
            if (pred()) return true;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        return pred();
    }

    struct ServerState {
        std::unique_ptr<boost::asio::io_context> io_context;
        std::unique_ptr<shardcache::network::TCPServer> server;
        std::vector<std::thread> threads;
    };

    std::vector<shardcache::CacheNode> nodes_;
    std::vector<std::unique_ptr<shardcache::Cluster>> clusters_;
    std::vector<ServerState> servers_;
};

TEST_F(ThreeNodeClusterTest, CrossNodeSetAndGet) {
    auto clientA = connect(0);
    auto clientC = connect(2);

    EXPECT_TRUE(clientA->set("cluster:item1", "distributed_data").is_ok());
    auto get_res = clientC->get("cluster:item1");
    ASSERT_TRUE(get_res.is_ok());
    EXPECT_EQ(get_res.value.value_or(""), "distributed_data");
}

TEST_F(ThreeNodeClusterTest, EveryReplicaStoresTheWrite) {
    auto clientA = connect(0);
    ASSERT_TRUE(clientA->set("replicated", "hello").is_ok());

    for (std::size_t i = 0; i < 3; ++i) {
        EXPECT_TRUE(eventually([&]() { return clusters_[i]->local_cache().get("replicated") == "hello"; }))
            << "replica " << nodes_[i].id;
    }
    // All replicas agree on the version, not just the value.
    auto v0 = clusters_[0]->local_cache().get_versioned("replicated");
    ASSERT_TRUE(v0.has_value());
    for (std::size_t i = 1; i < 3; ++i) {
        EXPECT_EQ(clusters_[i]->local_cache().get_versioned("replicated")->version, v0->version);
    }
}

TEST_F(ThreeNodeClusterTest, WritesAndReadsSurviveOneNodeDown) {
    stop_node(1);
    mark_health(1, false);

    auto clientA = connect(0);
    auto clientC = connect(2);
    EXPECT_TRUE(clientA->set("resilient", "still_works").is_ok());
    auto res = clientC->get("resilient");
    ASSERT_TRUE(res.is_ok());
    EXPECT_EQ(res.value.value_or(""), "still_works");
}

TEST_F(ThreeNodeClusterTest, UndetectedFailureStillMeetsQuorum) {
    // B is down but nobody has noticed yet: its RPCs time out instead of
    // being skipped, and the write still succeeds on A + C.
    stop_node(1);
    auto clientA = connect(0);
    auto start = std::chrono::steady_clock::now();
    EXPECT_TRUE(clientA->set("unnoticed", "v").is_ok());
    EXPECT_LT(std::chrono::steady_clock::now() - start, std::chrono::milliseconds(500));
    EXPECT_EQ(connect(2)->get("unnoticed").value.value_or(""), "v");
}

TEST_F(ThreeNodeClusterTest, QuorumLostFailsSafely) {
    stop_node(1);
    stop_node(2);
    mark_health(1, false);
    mark_health(2, false);

    auto clientA = connect(0);
    auto set_res = clientA->set("doomed", "value");
    EXPECT_FALSE(set_res.is_ok());
    EXPECT_NE(set_res.message.find("write quorum not reached"), std::string::npos);

    auto get_res = clientA->get("doomed");
    EXPECT_TRUE(get_res.is_error());  // an error, never a silent "not found"
    EXPECT_NE(get_res.message.find("read quorum not reached"), std::string::npos);
}

TEST_F(ThreeNodeClusterTest, HintedHandoffRepairsRecoveredNode) {
    stop_node(2);
    mark_health(2, false);

    auto clientA = connect(0);
    ASSERT_TRUE(clientA->set("handoff", "missed_by_c").is_ok());
    EXPECT_FALSE(clusters_[2]->local_cache().get("handoff").has_value());
    EXPECT_GE(clusters_[0]->pending_hints(), 1u);

    start_node(2);
    mark_health(2, true);
    clusters_[0]->replay_hints();
    clusters_[1]->replay_hints();

    EXPECT_EQ(clusters_[0]->pending_hints(), 0u);
    EXPECT_EQ(clusters_[2]->local_cache().get("handoff").value_or(""), "missed_by_c");
}

TEST_F(ThreeNodeClusterTest, ReadRepairFixesStaleReplica) {
    auto clientA = connect(0);
    ASSERT_TRUE(clientA->set("repair_me", "v1").is_ok());
    ASSERT_TRUE(eventually([&]() { return clusters_[2]->local_cache().get("repair_me") == "v1"; }));

    // Simulate C missing a newer write that reached A and B.
    auto old_version = clusters_[0]->local_cache().get_versioned("repair_me")->version;
    VersionedValue newer{"v2", old_version + 1};
    clusters_[0]->local_cache().apply("repair_me", newer);
    clusters_[1]->local_cache().apply("repair_me", newer);

    // A read coordinated by C sees v2 from a peer and repairs its own copy.
    auto res = connect(2)->get("repair_me");
    ASSERT_TRUE(res.is_ok());
    EXPECT_EQ(res.value.value_or(""), "v2");
    EXPECT_TRUE(eventually([&]() { return clusters_[2]->local_cache().get("repair_me") == "v2"; }));
}

TEST_F(ThreeNodeClusterTest, DeletePropagationAcrossCluster) {
    auto clientA = connect(0);
    auto clientB = connect(1);
    auto clientC = connect(2);

    EXPECT_TRUE(clientA->set("user:delete_test", "active").is_ok());
    EXPECT_TRUE(clientB->get("user:delete_test").is_ok());

    EXPECT_TRUE(clientB->remove("user:delete_test").is_ok());
    EXPECT_TRUE(clientB->remove("user:delete_test").is_not_found());

    EXPECT_TRUE(clientA->get("user:delete_test").is_not_found());
    EXPECT_TRUE(clientB->get("user:delete_test").is_not_found());
    EXPECT_TRUE(clientC->get("user:delete_test").is_not_found());
}

TEST_F(ThreeNodeClusterTest, ConcurrentWritersConverge) {
    constexpr int kWrites = 200;
    std::vector<std::thread> writers;
    for (std::size_t n = 0; n < 3; ++n) {
        writers.emplace_back([this, n]() {
            shardcache::network::Client c("127.0.0.1", nodes_[n].port);
            ASSERT_TRUE(c.connect());
            for (int i = 0; i < kWrites; ++i) {
                ASSERT_TRUE(c.set("contended", nodes_[n].id + ":" + std::to_string(i)).is_ok());
            }
        });
    }
    for (auto& w : writers) w.join();

    // Every replica ends with the same winning version.
    ASSERT_TRUE(eventually([&]() {
        auto a = clusters_[0]->local_cache().get_versioned("contended");
        auto b = clusters_[1]->local_cache().get_versioned("contended");
        auto c = clusters_[2]->local_cache().get_versioned("contended");
        return a && b && c && a->version == b->version && b->version == c->version && a->value == c->value;
    }));
    auto winner = clusters_[0]->local_cache().get("contended").value_or("");
    EXPECT_NE(winner.find(":" + std::to_string(kWrites - 1)), std::string::npos)
        << "the winner must be some node's last write, got " << winner;
}

TEST_F(ThreeNodeClusterTest, InternalReplicationIsolation) {
    // REPL_SET writes only the receiving node's cache, with no further routing.
    auto clientB = connect(1);
    auto res = clientB->replica_write("direct_repl_key", VersionedValue{"replicated_val", 1});
    EXPECT_TRUE(res.is_ok());
    EXPECT_EQ(clusters_[1]->local_cache().get("direct_repl_key").value_or(""), "replicated_val");
    EXPECT_FALSE(clusters_[0]->local_cache().contains("direct_repl_key"));
    EXPECT_FALSE(clusters_[2]->local_cache().contains("direct_repl_key"));
}

TEST_F(ThreeNodeClusterTest, PipelinedCommandsKeepOrder) {
    // Quorum ops run on the RPC executor; replies must still come back in
    // request order when many are pipelined in one write.
    boost::asio::io_context io;
    boost::asio::ip::tcp::socket sock(io);
    sock.connect({boost::asio::ip::make_address("127.0.0.1"), nodes_[0].port});

    std::string batch = "SET p 1\r\nGET p\r\nPING\r\nDEL p\r\nGET p\r\nECHO done\r\n";
    boost::asio::write(sock, boost::asio::buffer(batch));

    const std::string expected = "+OK\r\n$1\r\n1\r\n+PONG\r\n:1\r\n$-1\r\n$4\r\ndone\r\n";
    std::string got;
    std::array<char, 256> buf{};
    while (got.size() < expected.size()) {
        std::size_t n = sock.read_some(boost::asio::buffer(buf));
        got.append(buf.data(), n);
    }
    EXPECT_EQ(got, expected);
}

TEST_F(ThreeNodeClusterTest, StatusDistinguishability) {
    auto clientA = connect(0);

    auto res_not_found = clientA->get("non_existent_key_xyz");
    EXPECT_TRUE(res_not_found.is_not_found());
    EXPECT_EQ(res_not_found.status, shardcache::network::RequestStatus::NotFound);

    clientA->disconnect();
    auto res_conn_err = clientA->get("non_existent_key_xyz");
    EXPECT_TRUE(res_conn_err.is_error());
    EXPECT_EQ(res_conn_err.status, shardcache::network::RequestStatus::ConnectionError);
}

TEST_F(ThreeNodeClusterTest, DistributedExistsAndTtl) {
    auto clientA = connect(0);
    EXPECT_TRUE(clientA->set("exists_test_key", "val", 60000).is_ok());

    EXPECT_TRUE(clusters_[2]->exists("exists_test_key"));
    EXPECT_FALSE(clusters_[2]->exists("non_existent_key_123"));

    auto ttl = connect(2)->pttl("exists_test_key");
    ASSERT_TRUE(ttl.is_ok());
    EXPECT_GT(ttl.value.value_or(0), 55000);
    EXPECT_LE(ttl.value.value_or(0), 60000);

    EXPECT_TRUE(connect(1)->expire("exists_test_key", 5000).is_ok());
    EXPECT_LE(clusters_[0]->pttl("exists_test_key"), 5000);
}
