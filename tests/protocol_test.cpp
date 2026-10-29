#include <gtest/gtest.h>
#include "parser.hpp"
#include "resp.hpp"

using namespace shardcache::protocol;

TEST(ProtocolTest, ParseCommands) {
    auto cmd_set = Parser::parse("SET session:abc userdata TTL 60");
    EXPECT_EQ(cmd_set.type, CommandType::Set);
    EXPECT_EQ(cmd_set.key, "session:abc");
    EXPECT_EQ(cmd_set.value, "userdata");
    ASSERT_TRUE(cmd_set.ttl_ms.has_value());
    EXPECT_EQ(cmd_set.ttl_ms.value(), 60000u);

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
    ASSERT_TRUE(cmd_exp.ttl_ms.has_value());
    EXPECT_EQ(cmd_exp.ttl_ms.value(), 120000u);

    auto cmd_ttl = Parser::parse("TTL name");
    EXPECT_EQ(cmd_ttl.type, CommandType::Ttl);
    EXPECT_EQ(cmd_ttl.key, "name");
}

TEST(ProtocolTest, ParseRedisCompatibleOptions) {
    auto ex = Parser::parse(std::vector<std::string>{"set", "k", "v", "EX", "10"});
    EXPECT_EQ(ex.type, CommandType::Set);
    EXPECT_EQ(ex.ttl_ms.value_or(0), 10000u);

    auto px = Parser::parse(std::vector<std::string>{"SET", "k", "v", "px", "250"});
    EXPECT_EQ(px.ttl_ms.value_or(0), 250u);

    auto del = Parser::parse(std::vector<std::string>{"DEL", "a", "b", "c"});
    EXPECT_EQ(del.type, CommandType::Delete);
    EXPECT_EQ(del.keys.size(), 3u);

    auto bad_opt = Parser::parse(std::vector<std::string>{"SET", "k", "v", "NX"});
    EXPECT_EQ(bad_opt.type, CommandType::Unknown);
    EXPECT_EQ(bad_opt.error, "ERR syntax error");

    auto arity = Parser::parse(std::vector<std::string>{"GET"});
    EXPECT_EQ(arity.type, CommandType::Unknown);
    EXPECT_EQ(arity.error, "ERR wrong number of arguments for 'get' command");

    auto unknown = Parser::parse(std::vector<std::string>{"HSET", "h", "f", "v"});
    EXPECT_EQ(unknown.type, CommandType::Unknown);
}

TEST(ProtocolTest, ParseInternalReplicationCommands) {
    auto cmd_rset = Parser::parse("REPL_SET session:abc userdata TTL 120");
    EXPECT_EQ(cmd_rset.type, CommandType::ReplSet);
    EXPECT_EQ(cmd_rset.key, "session:abc");
    EXPECT_EQ(cmd_rset.value, "userdata");
    EXPECT_EQ(cmd_rset.ttl_ms.value_or(0), 120000u);

    auto cmd_rset_nottl = Parser::parse("REPL_SET mykey myval");
    EXPECT_EQ(cmd_rset_nottl.type, CommandType::ReplSet);
    EXPECT_FALSE(cmd_rset_nottl.ttl_ms.has_value());

    auto cmd_rdel = Parser::parse("REPL_DELETE session:abc");
    EXPECT_EQ(cmd_rdel.type, CommandType::ReplDelete);
    EXPECT_EQ(cmd_rdel.key, "session:abc");

    auto cmd_rexp = Parser::parse("REPL_EXPIRE session:abc 300");
    EXPECT_EQ(cmd_rexp.type, CommandType::ReplExpire);
    EXPECT_EQ(cmd_rexp.ttl_ms.value_or(0), 300000u);

    auto cmd_bad = Parser::parse("REPL_EXPIRE key notanumber");
    EXPECT_EQ(cmd_bad.type, CommandType::Unknown);

    EXPECT_NE(cmd_rset.type, CommandType::Set);
    EXPECT_NE(cmd_rdel.type, CommandType::Delete);
}

TEST(RespTest, ParsesMultibulkRequest) {
    std::string buf = "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$11\r\nhello world\r\n";
    std::vector<std::string> args;
    std::size_t consumed = 0;
    std::string err;
    ASSERT_EQ(parse_request(buf, args, consumed, err), ParseStatus::Ok);
    EXPECT_EQ(consumed, buf.size());
    ASSERT_EQ(args.size(), 3u);
    EXPECT_EQ(args[2], "hello world"); // binary-safe: spaces preserved
}

TEST(RespTest, IncompleteInputAtEveryPrefix) {
    std::string buf = resp::command({"SET", "k", "value\r\nwith\r\ncrlf"});
    std::vector<std::string> args;
    std::size_t consumed = 0;
    std::string err;
    for (std::size_t n = 0; n < buf.size(); ++n) {
        EXPECT_EQ(parse_request(std::string_view(buf).substr(0, n), args, consumed, err), ParseStatus::Incomplete) << n;
    }
    ASSERT_EQ(parse_request(buf, args, consumed, err), ParseStatus::Ok);
    EXPECT_EQ(args[2], "value\r\nwith\r\ncrlf");
}

TEST(RespTest, PipelinedRequestsParseSequentially) {
    std::string buf = resp::command({"PING"}) + "GET a\r\n" + resp::command({"DEL", "a", "b"});
    std::vector<std::vector<std::string>> parsed;
    std::size_t offset = 0;
    std::vector<std::string> args;
    std::string err;
    while (offset < buf.size()) {
        std::size_t consumed = 0;
        ASSERT_EQ(parse_request(std::string_view(buf).substr(offset), args, consumed, err), ParseStatus::Ok);
        parsed.push_back(args);
        offset += consumed;
    }
    ASSERT_EQ(parsed.size(), 3u);
    EXPECT_EQ(parsed[1], (std::vector<std::string>{"GET", "a"}));
    EXPECT_EQ(parsed[2].size(), 3u);
}

TEST(RespTest, RejectsMalformedAndOversizedInput) {
    std::vector<std::string> args;
    std::size_t consumed = 0;
    std::string err;
    EXPECT_EQ(parse_request("*1\r\n+PING\r\n", args, consumed, err), ParseStatus::Error);
    EXPECT_EQ(parse_request("*x\r\n", args, consumed, err), ParseStatus::Error);
    EXPECT_EQ(parse_request("*1\r\n$-5\r\n", args, consumed, err), ParseStatus::Error);
    EXPECT_EQ(parse_request("*1\r\n$999999999999\r\n", args, consumed, err), ParseStatus::Error);
    EXPECT_EQ(parse_request("*1\r\n$3\r\nGETXX", args, consumed, err), ParseStatus::Error);
    EXPECT_EQ(parse_request(std::string(kMaxInlineLength + 1, 'a'), args, consumed, err), ParseStatus::Error);
}

TEST(RespTest, ParsesReplies) {
    RespValue v;
    std::size_t used = 0;
    ASSERT_EQ(parse_reply("+OK\r\n", v, used), ParseStatus::Ok);
    EXPECT_EQ(v.type, RespValue::Type::SimpleString);
    EXPECT_EQ(v.str, "OK");

    ASSERT_EQ(parse_reply("$-1\r\n", v, used), ParseStatus::Ok);
    EXPECT_TRUE(v.is_null());

    ASSERT_EQ(parse_reply(":-2\r\n", v, used), ParseStatus::Ok);
    EXPECT_EQ(v.integer, -2);

    ASSERT_EQ(parse_reply("*2\r\n$1\r\na\r\n:7\r\n", v, used), ParseStatus::Ok);
    ASSERT_EQ(v.elements.size(), 2u);
    EXPECT_EQ(v.elements[0].str, "a");
    EXPECT_EQ(v.elements[1].integer, 7);

    EXPECT_EQ(parse_reply("$5\r\nhel", v, used), ParseStatus::Incomplete);
    EXPECT_EQ(parse_reply("-ERR boom\r\n", v, used), ParseStatus::Ok);
    EXPECT_TRUE(v.is_error());
}
