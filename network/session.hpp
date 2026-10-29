#ifndef SHARDCACHE_SESSION_HPP
#define SHARDCACHE_SESSION_HPP

#include "command_handler.hpp"
#include "protocol/command.hpp"

#include <boost/asio.hpp>
#include <array>
#include <memory>
#include <string>

namespace shardcache::network {

// One client connection. Reads are parsed as RESP (or inline commands) and
// every complete command in the buffer is executed before replying, so
// pipelined requests are answered with a single write.
class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    ClientSession(boost::asio::ip::tcp::socket socket, CommandHandler& handler);
    ~ClientSession() { handler_.connection_closed(); }

    void start();

private:
    void do_read();
    void process_input();
    void run_remote(protocol::Command cmd);
    void flush();
    void close();

    boost::asio::ip::tcp::socket socket_;
    CommandHandler& handler_;
    std::array<char, 16 * 1024> read_chunk_{};
    std::string input_;
    std::string output_;
    std::string writing_;
    bool closing_{false};
    bool awaiting_rpc_{false};
};

} // namespace shardcache::network

#endif // SHARDCACHE_SESSION_HPP
