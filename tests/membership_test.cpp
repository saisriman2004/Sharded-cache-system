#include <gtest/gtest.h>
#include "shardcache/cluster.hpp"
#include "tcp_server.hpp"
#include "client.hpp"

#include <chrono>
#include <functional>
#include <memory>
#include <thread>
#include <vector>

using shardcache::MemberStatus;

namespace {

// A running node: cluster (with gossip, health checks and rebalancing
// started) plus its TCP server.
struct TestNode {
    TestNode(const shardcache::CacheNode& self, const shardcache::ClusterConfig& config) {
        cluster = std::make_unique<shardcache::Cluster>(self, config);
        server = std::make_unique<shardcache::network::TCPServer>(io, self.port, *cluster);
        for (int i = 0; i < 2; ++i) threads.emplace_back([this]() { io.run(); });
        cluster->start();
    }
    ~TestNode() {
        cluster->stop();
        io.stop();
        for (auto& t : threads) t.join();
    }

    boost::asio::io_context io;
    std::unique_ptr<shardcache::Cluster> cluster;
    std::unique_ptr<shardcache::network::TCPServer> server;
    std::vector<std::thread> threads;
};

bool eventually(const std::function<bool()>& pred, std::chrono::milliseconds limit = std::chrono::seconds(5)) {
    auto deadline = std::chrono::steady_clock::now() + limit;
    while (std::chrono::steady_clock::now() < deadline) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

std::size_t alive_members(const shardcache::Cluster& c) {
    std::size_t n = 0;
    for (const auto& m : c.members()) n += m.status == MemberStatus::Alive ? 1 : 0;
    return n;
}

class MembershipTest : public ::testing::Test {
protected:
    shardcache::ClusterConfig config(std::size_t replication_factor) {
        shardcache::ClusterConfig c;
        c.replication_factor = replication_factor;
        c.cache_capacity = 10000;
        c.rpc_timeout = std::chrono::milliseconds(300);
        c.gossip_interval = std::chrono::milliseconds(50);
        c.health_check_interval = std::chrono::milliseconds(100);
        return c;
    }

    TestNode& start(const std::string& id, uint16_t port, std::size_t rf, const TestNode* seed = nullptr) {
        nodes_.push_back(std::make_unique<TestNode>(shardcache::CacheNode{id, "127.0.0.1", port}, config(rf)));
        if (seed) nodes_.back()->cluster->add_seed("127.0.0.1", seed->cluster->local_node().port);
        return *nodes_.back();
    }

    // Every key is stored on exactly the nodes of its current preference list.
    bool placement_matches(const std::vector<std::string>& keys) {
        for (const auto& key : keys) {
            auto owners = nodes_.front()->cluster->preference_list(key);
            for (const auto& node : nodes_) {
                bool owner = std::any_of(owners.begin(), owners.end(), [&](const shardcache::CacheNode& o) {
                    return o.id == node->cluster->local_node().id;
                });
                if (node->cluster->local_cache().contains(key) != owner) return false;
            }
        }
        return true;
    }

    std::vector<std::unique_ptr<TestNode>> nodes_;
};

} // namespace

TEST_F(MembershipTest, NodesJoinThroughSeedsAndConverge) {
    auto& a = start("a", 17201, 2);
    auto& b = start("b", 17202, 2, &a);
    auto& c = start("c", 17203, 2, &b);  // knows only b

    for (auto* n : {&a, &b, &c}) {
        EXPECT_TRUE(eventually([&]() { return alive_members(*n->cluster) == 3 && n->cluster->node_count() == 3; }))
            << n->cluster->local_node().id << " sees " << alive_members(*n->cluster) << " members";
    }
}

TEST_F(MembershipTest, JoiningNodeReceivesItsShareOfData) {
    auto& a = start("a", 17211, 2);
    auto& b = start("b", 17212, 2, &a);
    ASSERT_TRUE(eventually([&]() { return a.cluster->node_count() == 2; }));

    std::vector<std::string> keys;
    for (int i = 0; i < 300; ++i) {
        keys.push_back("user:" + std::to_string(i));
        ASSERT_TRUE(a.cluster->set(keys.back(), "v" + std::to_string(i)));
    }
    ASSERT_TRUE(eventually([&]() { return placement_matches(keys); }));

    auto& c = start("c", 17213, 2, &a);
    ASSERT_TRUE(eventually([&]() { return a.cluster->node_count() == 3 && b.cluster->node_count() == 3; }));

    // Old owners stream keys to c and drop the ones they no longer own.
    EXPECT_TRUE(eventually([&]() { return placement_matches(keys); }));
    EXPECT_GT(c.cluster->local_cache().size(), 50u);
    EXPECT_GT(a.cluster->keys_moved() + b.cluster->keys_moved(), 0u);

    for (const auto& key : keys) {
        auto value = c.cluster->get(key);
        ASSERT_TRUE(value.has_value()) << key;
    }
}

TEST_F(MembershipTest, GracefulLeaveHandsOffData) {
    auto& a = start("a", 17221, 2);
    auto& b = start("b", 17222, 2, &a);
    auto& c = start("c", 17223, 2, &a);
    ASSERT_TRUE(eventually([&]() {
        return a.cluster->node_count() == 3 && b.cluster->node_count() == 3 && c.cluster->node_count() == 3;
    }));

    std::vector<std::string> keys;
    for (int i = 0; i < 300; ++i) {
        keys.push_back("item:" + std::to_string(i));
        ASSERT_TRUE(b.cluster->set(keys.back(), std::to_string(i)));
    }

    a.cluster->leave();
    EXPECT_EQ(a.cluster->local_cache().size(), 0u) << "a hands off and drops everything";

    for (auto* n : {&b, &c}) {
        EXPECT_TRUE(eventually([&]() { return n->cluster->node_count() == 2; }));
    }
    // With N = 2 and two nodes left, both remaining nodes hold every key.
    for (const auto& key : keys) {
        EXPECT_TRUE(b.cluster->local_cache().contains(key)) << key;
        EXPECT_TRUE(c.cluster->local_cache().contains(key)) << key;
        EXPECT_EQ(c.cluster->get(key).value_or(""), key.substr(5));
    }
}

TEST_F(MembershipTest, ClusterNodesCommandListsMembers) {
    auto& a = start("a", 17231, 2);
    start("b", 17232, 2, &a);
    ASSERT_TRUE(eventually([&]() { return a.cluster->node_count() == 2; }));

    shardcache::network::Client client("127.0.0.1", 17231);
    ASSERT_TRUE(client.connect());
    auto res = client.command({"CLUSTER", "NODES"});
    ASSERT_TRUE(res.is_ok());
    EXPECT_NE(res.value->str.find("a 127.0.0.1:17231"), std::string::npos);
    EXPECT_NE(res.value->str.find("myself"), std::string::npos);
    EXPECT_NE(res.value->str.find("b 127.0.0.1:17232"), std::string::npos);
}
