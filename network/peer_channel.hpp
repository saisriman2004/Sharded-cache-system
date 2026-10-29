#ifndef SHARDCACHE_PEER_CHANNEL_HPP
#define SHARDCACHE_PEER_CHANNEL_HPP

#include "network/request_result.hpp"
#include "protocol/resp.hpp"

#include <boost/asio.hpp>

#include <array>
#include <atomic>
#include <chrono>
#include <deque>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace shardcache::network {

// Asynchronous, pipelined RESP connection to one peer. Calls from any
// thread are queued; everything queued while a write is in flight goes out
// in the next single write, and replies are matched to callbacks in order
// (RESP answers requests in order). No thread blocks on an RPC.
//
// Failure handling: every call has a deadline (which also bounds time spent
// connecting); on timeout or any I/O error the connection is closed, every
// outstanding call fails, and the next call reconnects.
class PeerChannel : public std::enable_shared_from_this<PeerChannel> {
public:
    using Callback = std::function<void(RequestResult<protocol::RespValue>)>;

    PeerChannel(boost::asio::io_context& io, std::string host, uint16_t port,
                std::chrono::milliseconds request_timeout);

    void call(const std::vector<std::string>& args, Callback cb);
    void close();

private:
    struct Pending {
        Callback cb;
        std::chrono::steady_clock::time_point deadline;
    };

    void enqueue(std::string request, Callback cb);
    void connect();
    void flush();
    void read();
    void on_timer();
    void fail_all(RequestStatus status, const std::string& message);
    void reset_connection();

    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::string host_;
    uint16_t port_;
    std::chrono::milliseconds request_timeout_;

    boost::asio::ip::tcp::socket socket_;
    boost::asio::steady_timer timer_;
    enum class State { Disconnected, Connecting, Connected } state_{State::Disconnected};
    uint64_t generation_{0};  // bumped per connection; stale handlers bail out
    bool writing_{false};
    bool timer_armed_{false};
    std::string queued_;
    std::string in_write_;
    std::deque<Pending> pending_;
    std::string rx_;
    std::array<char, 64 * 1024> chunk_{};
};

// A few PeerChannels per peer on a dedicated I/O thread pool.
class PeerChannels {
public:
    PeerChannels(std::size_t io_threads, std::size_t channels_per_peer, std::chrono::milliseconds request_timeout);
    ~PeerChannels();

    void call(const std::string& host, uint16_t port, const std::vector<std::string>& args, PeerChannel::Callback cb);
    void drop(const std::string& host, uint16_t port);

private:
    boost::asio::io_context io_;
    boost::asio::executor_work_guard<boost::asio::io_context::executor_type> guard_;
    std::vector<std::thread> threads_;
    std::size_t per_peer_;
    std::chrono::milliseconds request_timeout_;
    std::mutex mutex_;
    std::unordered_map<std::string, std::vector<std::shared_ptr<PeerChannel>>> channels_;
    std::atomic<std::size_t> next_{0};
};

} // namespace shardcache::network

#endif // SHARDCACHE_PEER_CHANNEL_HPP
