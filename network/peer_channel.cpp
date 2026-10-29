#include "peer_channel.hpp"

namespace shardcache::network {

PeerChannel::PeerChannel(boost::asio::io_context& io, std::string host, uint16_t port,
                         std::chrono::milliseconds request_timeout)
    : strand_(boost::asio::make_strand(io)),
      host_(std::move(host)),
      port_(port),
      request_timeout_(request_timeout),
      socket_(strand_),
      timer_(strand_) {}

void PeerChannel::call(const std::vector<std::string>& args, Callback cb) {
    boost::asio::post(strand_, [self = shared_from_this(), request = protocol::resp::command(args),
                                cb = std::move(cb)]() mutable {
        self->enqueue(std::move(request), std::move(cb));
    });
}

void PeerChannel::close() {
    boost::asio::post(strand_, [self = shared_from_this()]() {
        self->fail_all(RequestStatus::ConnectionError, "Channel closed");
    });
}

void PeerChannel::enqueue(std::string request, Callback cb) {
    queued_ += request;
    pending_.push_back(Pending{std::move(cb), std::chrono::steady_clock::now() + request_timeout_});
    if (!timer_armed_) {
        timer_armed_ = true;
        timer_.expires_after(request_timeout_ / 4);
        timer_.async_wait([self = shared_from_this()](const boost::system::error_code&) { self->on_timer(); });
    }
    if (state_ == State::Disconnected) {
        connect();
    } else if (state_ == State::Connected) {
        flush();
    }
}

void PeerChannel::connect() {
    state_ = State::Connecting;
    const uint64_t gen = ++generation_;
    auto resolver = std::make_shared<boost::asio::ip::tcp::resolver>(strand_);
    resolver->async_resolve(host_, std::to_string(port_),
        [self = shared_from_this(), resolver, gen](const boost::system::error_code& ec,
                                                   boost::asio::ip::tcp::resolver::results_type results) {
            if (gen != self->generation_) return;
            if (ec) {
                self->fail_all(RequestStatus::ConnectionError, ec.message());
                return;
            }
            boost::asio::async_connect(self->socket_, results,
                [self, gen](const boost::system::error_code& cec, const boost::asio::ip::tcp::endpoint&) {
                    if (gen != self->generation_) return;
                    if (cec) {
                        self->fail_all(RequestStatus::ConnectionError, cec.message());
                        return;
                    }
                    boost::system::error_code ignored;
                    self->socket_.set_option(boost::asio::ip::tcp::no_delay(true), ignored);
                    self->state_ = State::Connected;
                    self->read();
                    self->flush();
                });
        });
}

void PeerChannel::flush() {
    if (writing_ || queued_.empty() || state_ != State::Connected) return;
    writing_ = true;
    in_write_.swap(queued_);
    queued_.clear();
    const uint64_t gen = generation_;
    boost::asio::async_write(socket_, boost::asio::buffer(in_write_),
        [self = shared_from_this(), gen](const boost::system::error_code& ec, std::size_t) {
            if (gen != self->generation_) return;
            self->writing_ = false;
            self->in_write_.clear();
            if (ec) {
                self->fail_all(RequestStatus::ConnectionError, ec.message());
                return;
            }
            self->flush();
        });
}

void PeerChannel::read() {
    const uint64_t gen = generation_;
    socket_.async_read_some(boost::asio::buffer(chunk_),
        [self = shared_from_this(), gen](const boost::system::error_code& ec, std::size_t n) {
            if (gen != self->generation_) return;
            if (ec) {
                self->fail_all(RequestStatus::ConnectionError, ec.message());
                return;
            }
            self->rx_.append(self->chunk_.data(), n);
            std::size_t offset = 0;
            while (!self->pending_.empty()) {
                protocol::RespValue reply;
                std::size_t used = 0;
                auto st = protocol::parse_reply(std::string_view(self->rx_).substr(offset), reply, used);
                if (st == protocol::ParseStatus::Incomplete) break;
                if (st == protocol::ParseStatus::Error) {
                    self->fail_all(RequestStatus::ProtocolError, "Malformed RESP reply");
                    return;
                }
                offset += used;
                auto cb = std::move(self->pending_.front().cb);
                self->pending_.pop_front();
                cb(RequestResult<protocol::RespValue>::success(std::move(reply)));
            }
            self->rx_.erase(0, offset);
            self->read();
        });
}

void PeerChannel::on_timer() {
    timer_armed_ = false;
    if (pending_.empty()) return;
    if (pending_.front().deadline <= std::chrono::steady_clock::now()) {
        fail_all(RequestStatus::Timeout, "Operation timed out");
        return;
    }
    timer_armed_ = true;
    timer_.expires_after(request_timeout_ / 4);
    timer_.async_wait([self = shared_from_this()](const boost::system::error_code&) { self->on_timer(); });
}

void PeerChannel::reset_connection() {
    ++generation_;  // invalidates every handler of the old connection
    boost::system::error_code ignored;
    socket_.close(ignored);
    socket_ = boost::asio::ip::tcp::socket(strand_);
    state_ = State::Disconnected;
    writing_ = false;
    queued_.clear();
    in_write_.clear();
    rx_.clear();
}

void PeerChannel::fail_all(RequestStatus status, const std::string& message) {
    reset_connection();
    auto failed = std::move(pending_);
    pending_.clear();
    for (auto& p : failed) {
        p.cb(RequestResult<protocol::RespValue>::error(status, message));
    }
}

PeerChannels::PeerChannels(std::size_t io_threads, std::size_t channels_per_peer,
                           std::chrono::milliseconds request_timeout)
    : guard_(boost::asio::make_work_guard(io_)),
      per_peer_(std::max<std::size_t>(1, channels_per_peer)),
      request_timeout_(request_timeout) {
    for (std::size_t i = 0; i < std::max<std::size_t>(1, io_threads); ++i) {
        threads_.emplace_back([this]() { io_.run(); });
    }
}

PeerChannels::~PeerChannels() {
    {
        std::lock_guard lock(mutex_);
        for (auto& [_, list] : channels_) {
            for (auto& ch : list) ch->close();
        }
    }
    guard_.reset();
    io_.stop();
    for (auto& t : threads_) t.join();
}

void PeerChannels::call(const std::string& host, uint16_t port, const std::vector<std::string>& args,
                        PeerChannel::Callback cb) {
    std::shared_ptr<PeerChannel> channel;
    {
        std::lock_guard lock(mutex_);
        auto& list = channels_[host + ":" + std::to_string(port)];
        if (list.empty()) {
            for (std::size_t i = 0; i < per_peer_; ++i) {
                list.push_back(std::make_shared<PeerChannel>(io_, host, port, request_timeout_));
            }
        }
        channel = list[next_.fetch_add(1, std::memory_order_relaxed) % list.size()];
    }
    channel->call(args, std::move(cb));
}

void PeerChannels::drop(const std::string& host, uint16_t port) {
    std::lock_guard lock(mutex_);
    auto it = channels_.find(host + ":" + std::to_string(port));
    if (it == channels_.end()) return;
    for (auto& ch : it->second) ch->close();
}

} // namespace shardcache::network
