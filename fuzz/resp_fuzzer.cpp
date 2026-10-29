// Fuzz target for the request path that handles untrusted network input:
// RESP/inline framing, command parsing and reply decoding.
//
// Besides "does not crash", it checks a round-trip property: every request
// the parser accepts, re-encoded as RESP, must parse back to the same
// arguments and consume exactly the re-encoded bytes.
#include "protocol/parser.hpp"
#include "protocol/resp.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string_view>

using namespace shardcache::protocol;

namespace {
void check(bool ok) {
    if (!ok) std::abort();
}
} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view input(reinterpret_cast<const char*>(data), size);

    // Consume the buffer exactly like a session does with pipelined input.
    std::size_t offset = 0;
    std::vector<std::string> args;
    std::string error;
    while (offset < input.size()) {
        std::size_t consumed = 0;
        auto status = parse_request(input.substr(offset), args, consumed, error);
        if (status != ParseStatus::Ok) break;
        check(consumed > 0 && consumed <= input.size() - offset);
        offset += consumed;
        if (args.empty()) continue;

        Command cmd = Parser::parse(args);
        check(cmd.type != CommandType::Unknown || !cmd.error.empty());

        std::string encoded = resp::command(args);
        std::vector<std::string> again;
        std::size_t used = 0;
        check(parse_request(encoded, again, used, error) == ParseStatus::Ok);
        check(used == encoded.size());
        check(again == args);
    }

    // Reply decoding (used by the peer client on bytes from other nodes).
    RespValue value;
    std::size_t used = 0;
    if (parse_reply(input, value, used) == ParseStatus::Ok) {
        check(used > 0 && used <= input.size());
    }
    return 0;
}
