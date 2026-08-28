#ifndef SHARDCACHE_SESSION_HPP
#define SHARDCACHE_SESSION_HPP

#include "shardcache/cluster.hpp"
#include "parser.hpp"
#include "response.hpp"

#include <boost/asio.hpp>
#include <memory>
#include <array>
#include <string>

namespace shardcache::network {

class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    explicit ClientSession(boost::asio::ip::tcp::socket socket, Cluster& cluster);
    ~ClientSession() = default;

    void start();

private:
    void do_read();
    void process_command_line(const std::string& line);
    void do_write(std::shared_ptr<std::string> response);

    boost::asio::ip::tcp::socket socket_;
    Cluster& cluster_;
    boost::asio::streambuf buffer_;
};

} // namespace shardcache::network

#endif // SHARDCACHE_SESSION_HPP
