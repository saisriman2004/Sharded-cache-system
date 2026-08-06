#ifndef SHARDCACHE_SESSION_HPP
#define SHARDCACHE_SESSION_HPP

#include "shardcache/sharded_cache.hpp"
#include "parser.hpp"
#include "response.hpp"

#include <boost/asio.hpp>
#include <memory>
#include <array>
#include <string>

namespace shardcache::network {

class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    explicit ClientSession(boost::asio::ip::tcp::socket socket, ShardedCache& cache);
    ~ClientSession() = default;

    void start();

private:
    void do_read();
    void process_command_line(const std::string& line);
    void do_write(const std::string& response);

    boost::asio::ip::tcp::socket socket_;
    ShardedCache& cache_;
    boost::asio::streambuf buffer_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_SESSION_HPP
