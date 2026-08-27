#include <gtest/gtest.h>
#include "parser.hpp"
#include "response.hpp"

TEST(ProtocolTest, ParseCommands) {
    using namespace shardcache::protocol;

    auto cmd_set = Parser::parse("SET session:abc userdata TTL 60");
    EXPECT_EQ(cmd_set.type, CommandType::Set);
    EXPECT_EQ(cmd_set.key, "session:abc");
    EXPECT_EQ(cmd_set.value, "userdata");
    ASSERT_TRUE(cmd_set.ttl_seconds.has_value());
    EXPECT_EQ(cmd_set.ttl_seconds.value(), 60);

    auto cmd_get = Parser::parse("GET name");
    EXPECT_EQ(cmd_get.type, CommandType::Get);
    EXPECT_EQ(cmd_get.key, "name");

    auto cmd_ping = Parser::parse("PING");
    EXPECT_EQ(cmd_ping.type, CommandType::Ping);

    auto cmd_stats = Parser::parse("STATS");
    EXPECT_EQ(cmd_stats.type, CommandType::Stats);

    auto cmd_del = Parser::parse("DELETE name");
    EXPECT_EQ(cmd_del.type, CommandType::Delete);
    EXPECT_EQ(cmd_del.key, "name");

    auto cmd_exp = Parser::parse("EXPIRE name 120");
    EXPECT_EQ(cmd_exp.type, CommandType::Expire);
    EXPECT_EQ(cmd_exp.key, "name");
    ASSERT_TRUE(cmd_exp.ttl_seconds.has_value());
    EXPECT_EQ(cmd_exp.ttl_seconds.value(), 120);

    auto cmd_ttl = Parser::parse("TTL name");
    EXPECT_EQ(cmd_ttl.type, CommandType::Ttl);
    EXPECT_EQ(cmd_ttl.key, "name");
}

TEST(ProtocolTest, GenerateResponses) {
    using namespace shardcache::protocol;

    EXPECT_EQ(Response::ok(), "OK\r\n");
    EXPECT_EQ(Response::value("Sai"), "VALUE Sai\r\n");
    EXPECT_EQ(Response::not_found(), "NOT_FOUND\r\n");
    EXPECT_EQ(Response::pong(), "PONG\r\n");
}

TEST(ProtocolTest, ParseInternalReplicationCommands) {
    using namespace shardcache::protocol;

    // REPL_SET with value and TTL
    auto cmd_rset = Parser::parse("REPL_SET session:abc userdata TTL 120");
    EXPECT_EQ(cmd_rset.type, CommandType::ReplSet);
    EXPECT_EQ(cmd_rset.key, "session:abc");
    EXPECT_EQ(cmd_rset.value, "userdata");
    ASSERT_TRUE(cmd_rset.ttl_seconds.has_value());
    EXPECT_EQ(cmd_rset.ttl_seconds.value(), 120);

    // REPL_SET without TTL
    auto cmd_rset_nottl = Parser::parse("REPL_SET mykey myval");
    EXPECT_EQ(cmd_rset_nottl.type, CommandType::ReplSet);
    EXPECT_EQ(cmd_rset_nottl.key, "mykey");
    EXPECT_EQ(cmd_rset_nottl.value, "myval");
    EXPECT_FALSE(cmd_rset_nottl.ttl_seconds.has_value());

    // REPL_DELETE
    auto cmd_rdel = Parser::parse("REPL_DELETE session:abc");
    EXPECT_EQ(cmd_rdel.type, CommandType::ReplDelete);
    EXPECT_EQ(cmd_rdel.key, "session:abc");

    // REPL_EXPIRE
    auto cmd_rexp = Parser::parse("REPL_EXPIRE session:abc 300");
    EXPECT_EQ(cmd_rexp.type, CommandType::ReplExpire);
    EXPECT_EQ(cmd_rexp.key, "session:abc");
    ASSERT_TRUE(cmd_rexp.ttl_seconds.has_value());
    EXPECT_EQ(cmd_rexp.ttl_seconds.value(), 300);

    // Malformed REPL_EXPIRE with non-numeric TTL
    auto cmd_bad = Parser::parse("REPL_EXPIRE key notanumber");
    EXPECT_EQ(cmd_bad.type, CommandType::Unknown);

    // REPL_SET must not parse as regular SET
    EXPECT_NE(cmd_rset.type, CommandType::Set);
    EXPECT_NE(cmd_rdel.type, CommandType::Delete);
}
