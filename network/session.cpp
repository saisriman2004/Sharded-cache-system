#include "session.hpp"
#include "protocol/parser.hpp"
#include "protocol/resp.hpp"
#include "shardcache/logger.hpp"

namespace shardcache::network {

namespace {
// Stop parsing more pipelined commands once this many reply bytes are queued.
constexpr std::size_t kMaxPendingOutput = 1024 * 1024;
} // namespace

ClientSession::ClientSession(boost::asio::ip::tcp::socket socket, CommandHandler& handler)
    : socket_(std::move(socket)), handler_(handler) {
    handler_.connection_opened();
    boost::system::error_code ec;
    socket_.set_option(boost::asio::ip::tcp::no_delay(true), ec);
}

void ClientSession::start() {
    do_read();
}

void ClientSession::do_read() {
    auto self(shared_from_this());
    socket_.async_read_some(
        boost::asio::buffer(read_chunk_),
        [this, self](boost::system::error_code ec, std::size_t n) {
            if (ec) {
                close();
                return;
            }
            input_.append(read_chunk_.data(), n);
            process_input();
        }
    );
}

void ClientSession::process_input() {
    if (awaiting_rpc_) return;

    std::size_t offset = 0;
    std::vector<std::string> args;
    std::string error;

    while (!closing_ && offset < input_.size() && output_.size() < kMaxPendingOutput) {
        std::size_t consumed = 0;
        auto status = protocol::parse_request(std::string_view(input_).substr(offset), args, consumed, error);
        if (status == protocol::ParseStatus::Incomplete) {
            break;
        }
        if (status == protocol::ParseStatus::Error) {
            protocol::resp::append_error(output_, error);
            closing_ = true;
            break;
        }
        offset += consumed;
        if (args.empty()) {
            continue;
        }

        auto cmd = protocol::Parser::parse(args);
        if (!handler_.runs_locally(cmd)) {
            // Hand off to the RPC executor; pipelined commands behind this
            // one stay buffered so replies keep their order.
            awaiting_rpc_ = true;
            input_.erase(0, offset);
            run_remote(std::move(cmd));
            return;
        }
        handler_.execute(cmd, output_);
        if (cmd.type == protocol::CommandType::Quit) {
            closing_ = true;
        }
    }

    input_.erase(0, offset);
    flush();
}

void ClientSession::run_remote(protocol::Command cmd) {
    auto self(shared_from_this());
    boost::asio::post(handler_.rpc_executor(), [this, self, cmd = std::move(cmd)]() {
        auto reply = std::make_shared<std::string>();
        handler_.execute(cmd, *reply);
        boost::asio::post(socket_.get_executor(), [this, self, reply]() {
            output_ += *reply;
            awaiting_rpc_ = false;
            process_input();
        });
    });
}

void ClientSession::flush() {
    if (output_.empty()) {
        if (closing_) {
            close();
        } else {
            do_read();
        }
        return;
    }

    writing_.swap(output_);
    output_.clear();

    auto self(shared_from_this());
    boost::asio::async_write(
        socket_,
        boost::asio::buffer(writing_),
        [this, self](boost::system::error_code ec, std::size_t /*bytes_transferred*/) {
            if (ec) {
                close();
                return;
            }
            writing_.clear();
            // More pipelined commands may already be buffered.
            if (!closing_ && !input_.empty()) {
                process_input();
            } else {
                flush();
            }
        }
    );
}

void ClientSession::close() {
    boost::system::error_code ec;
    socket_.shutdown(boost::asio::ip::tcp::socket::shutdown_both, ec);
    socket_.close(ec);
}

} // namespace shardcache::network
