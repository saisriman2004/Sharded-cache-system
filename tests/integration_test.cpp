#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"
#include "tcp_server.hpp"
#include "client.hpp"

#include <thread>
#include <vector>
#include <memory>
#include <chrono>

class ThreeNodeClusterTest : public ::testing::Test {
protected:
    void SetUp() override {
        nodes_ = {
            {"nodeA", "127.0.0.1", 17001},
            {"nodeB", "127.0.0.1", 17002},
            {"nodeC", "127.0.0.1", 17003}
        };

        // Initialize 3 clusters
        for (std::size_t i = 0; i < 3; ++i) {
            clusters_.push_back(std::make_unique<shardcache::Cluster>(nodes_[i], 2, 1000));
        }

        // Cross-register peers
        for (std::size_t i = 0; i < 3; ++i) {
            for (std::size_t j = 0; j < 3; ++j) {
                if (i != j) {
                    clusters_[i]->add_node(nodes_[j]);
                }
            }
        }

        // Start servers with multi-threaded worker pool to handle concurrent inter-node RPCs
        for (std::size_t i = 0; i < 3; ++i) {
            io_contexts_.push_back(std::make_unique<boost::asio::io_context>());
            servers_.push_back(std::make_unique<shardcache::network::TCPServer>(
                *io_contexts_[i],
                nodes_[i].port,
                *clusters_[i]
            ));
            for (int t = 0; t < 4; ++t) {
                io_threads_.emplace_back([this, i]() {
                    io_contexts_[i]->run();
                });
            }
        }

        // Give servers a moment to bind and listen
        std::this_thread::sleep_for(std::chrono::milliseconds(150));
    }

    void TearDown() override {
        for (auto& ctx : io_contexts_) {
            ctx->stop();
        }
        for (auto& th : io_threads_) {
            if (th.joinable()) {
                th.join();
            }
        }
    }

    std::vector<shardcache::CacheNode> nodes_;
    std::vector<std::unique_ptr<shardcache::Cluster>> clusters_;
    std::vector<std::unique_ptr<boost::asio::io_context>> io_contexts_;
    std::vector<std::unique_ptr<shardcache::network::TCPServer>> servers_;
    std::vector<std::thread> io_threads_;
};

TEST_F(ThreeNodeClusterTest, CrossNodeRoutingSetAndGet) {
    // Client connects to Node A (port 17001)
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());

    // Client connects to Node C (port 17003)
    shardcache::network::Client clientC("127.0.0.1", 17003);
    ASSERT_TRUE(clientC.connect());

    // SET on Node A
    auto set_res = clientA.set("cluster:item1", "distributed_data");
    EXPECT_TRUE(set_res.is_ok());

    // GET from Node C (which may not be the primary, testing cross-node routing)
    auto get_res = clientC.get("cluster:item1");
    ASSERT_TRUE(get_res.is_ok());
    ASSERT_TRUE(get_res.value.has_value());
    EXPECT_EQ(get_res.value.value(), "distributed_data");
}

TEST_F(ThreeNodeClusterTest, ABAReplicationFlow) {
    // Pick a key where Primary = nodeB, and Replica = nodeA
    std::string target_key;
    for (int i = 0; i < 10000; ++i) {
        std::string k = "aba:key:" + std::to_string(i);
        auto primary = clusters_[0]->locate(k);
        if (primary.has_value() && primary->id == "nodeB") {
            auto replicas = clusters_[0]->locate_replicas(k, 2);
            bool has_nodeA_replica = false;
            for (const auto& rep : replicas) {
                if (rep.id == "nodeA") {
                    has_nodeA_replica = true;
                    break;
                }
            }
            if (has_nodeA_replica) {
                target_key = k;
                break;
            }
        }
    }
    ASSERT_FALSE(target_key.empty());

    // Connect client to nodeA (17001)
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());

    auto start_time = std::chrono::steady_clock::now();

    // SET test-key hello via Node A
    auto set_res = clientA.set(target_key, "hello");
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - start_time);

    EXPECT_TRUE(set_res.is_ok());
    // Must complete fast without socket timeouts
    EXPECT_LT(duration.count(), 1500);

    // GET from A -> hello
    auto getA = clientA.get(target_key);
    ASSERT_TRUE(getA.is_ok());
    ASSERT_TRUE(getA.value.has_value());
    EXPECT_EQ(getA.value.value(), "hello");

    // GET from B -> hello
    shardcache::network::Client clientB("127.0.0.1", 17002);
    ASSERT_TRUE(clientB.connect());
    auto getB = clientB.get(target_key);
    ASSERT_TRUE(getB.is_ok());
    ASSERT_TRUE(getB.value.has_value());
    EXPECT_EQ(getB.value.value(), "hello");

    // GET from C -> hello
    shardcache::network::Client clientC("127.0.0.1", 17003);
    ASSERT_TRUE(clientC.connect());
    auto getC = clientC.get(target_key);
    ASSERT_TRUE(getC.is_ok());
    ASSERT_TRUE(getC.value.has_value());
    EXPECT_EQ(getC.value.value(), "hello");

    // Verify Node A's local cache contains the replica data
    EXPECT_TRUE(clusters_[0]->local_cache().contains(target_key));
    EXPECT_EQ(clusters_[0]->local_cache().get(target_key).value_or(""), "hello");
}

TEST_F(ThreeNodeClusterTest, PrimaryDownReadFailoverToReplica) {
    // 1. Pick a key where Primary = nodeB and Replica = nodeA
    std::string key;
    for (int i = 0; i < 10000; ++i) {
        std::string k = "failover:key:" + std::to_string(i);
        auto primary = clusters_[0]->locate(k);
        if (primary.has_value() && primary->id == "nodeB") {
            auto replicas = clusters_[0]->locate_replicas(k, 2);
            bool has_nodeA_replica = false;
            for (const auto& rep : replicas) {
                if (rep.id == "nodeA") {
                    has_nodeA_replica = true;
                    break;
                }
            }
            if (has_nodeA_replica) {
                key = k;
                break;
            }
        }
    }
    ASSERT_FALSE(key.empty());

    // 2. SET key value
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());
    EXPECT_TRUE(clientA.set(key, "resilient_data").is_ok());

    // Confirm initial GET works
    auto initial_get = clientA.get(key);
    ASSERT_TRUE(initial_get.is_ok());
    EXPECT_EQ(initial_get.value.value_or(""), "resilient_data");

    // 3. Kill Node B (stop its io_context)
    io_contexts_[1]->stop();

    // Mark Node B unhealthy in cluster routing for remaining nodes A and C
    clusters_[0]->set_node_health("nodeB", false);
    clusters_[2]->set_node_health("nodeB", false);

    // 4. Now GET through A -> value (reads local replica A)
    auto getA = clientA.get(key);
    ASSERT_TRUE(getA.is_ok());
    ASSERT_TRUE(getA.value.has_value());
    EXPECT_EQ(getA.value.value(), "resilient_data");

    // 5. GET through C -> value (C forwards to healthy replica A instead of dead primary B)
    shardcache::network::Client clientC("127.0.0.1", 17003);
    ASSERT_TRUE(clientC.connect());
    auto getC = clientC.get(key);
    ASSERT_TRUE(getC.is_ok());
    ASSERT_TRUE(getC.value.has_value());
    EXPECT_EQ(getC.value.value(), "resilient_data");
}

TEST_F(ThreeNodeClusterTest, DeletePropagationAcrossCluster) {
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());
    shardcache::network::Client clientB("127.0.0.1", 17002);
    ASSERT_TRUE(clientB.connect());
    shardcache::network::Client clientC("127.0.0.1", 17003);
    ASSERT_TRUE(clientC.connect());

    EXPECT_TRUE(clientA.set("user:delete_test", "active").is_ok());
    EXPECT_TRUE(clientB.get("user:delete_test").is_ok());

    // Delete from node B
    EXPECT_TRUE(clientB.remove("user:delete_test").is_ok());

    // Must be gone from all nodes
    EXPECT_TRUE(clientA.get("user:delete_test").is_not_found());
    EXPECT_TRUE(clientB.get("user:delete_test").is_not_found());
    EXPECT_TRUE(clientC.get("user:delete_test").is_not_found());
}

TEST_F(ThreeNodeClusterTest, InternalReplicationIsolation) {
    // Direct REPL_SET to Node B should write to its local cache only
    // without triggering forwarding or infinite loop
    shardcache::network::Client clientB("127.0.0.1", 17002);
    ASSERT_TRUE(clientB.connect());

    EXPECT_TRUE(clientB.replica_set("direct_repl_key", "replicated_val").is_ok());
    EXPECT_TRUE(clusters_[1]->local_cache().contains("direct_repl_key"));
    auto local_val = clusters_[1]->local_cache().get("direct_repl_key");
    ASSERT_TRUE(local_val.has_value());
    EXPECT_EQ(local_val.value(), "replicated_val");
}

TEST_F(ThreeNodeClusterTest, StatusDistinguishability) {
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());

    // 1. Missing key returns NotFound, NOT ConnectionError
    auto res_not_found = clientA.get("non_existent_key_xyz");
    EXPECT_TRUE(res_not_found.is_not_found());
    EXPECT_FALSE(res_not_found.is_ok());
    EXPECT_EQ(res_not_found.status, shardcache::network::RequestStatus::NotFound);

    // 2. Disconnected client returns ConnectionError, NOT NotFound
    clientA.disconnect();
    auto res_conn_err = clientA.get("non_existent_key_xyz");
    EXPECT_TRUE(res_conn_err.is_error());
    EXPECT_FALSE(res_conn_err.is_not_found());
    EXPECT_EQ(res_conn_err.status, shardcache::network::RequestStatus::ConnectionError);
}

TEST_F(ThreeNodeClusterTest, DistributedExistsVerification) {
    shardcache::network::Client clientA("127.0.0.1", 17001);
    ASSERT_TRUE(clientA.connect());
    shardcache::network::Client clientC("127.0.0.1", 17003);
    ASSERT_TRUE(clientC.connect());

    // SET key on Node A
    EXPECT_TRUE(clientA.set("exists_test_key", "val").is_ok());

    // Verify Cluster::exists returns true from Node C across the cluster
    EXPECT_TRUE(clusters_[2]->exists("exists_test_key"));

    // Verify Cluster::exists returns false for non-existent key
    EXPECT_FALSE(clusters_[2]->exists("non_existent_key_123"));
}

