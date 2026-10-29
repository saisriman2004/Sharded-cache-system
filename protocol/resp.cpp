#include "resp.hpp"

#include <charconv>

namespace shardcache::protocol {

namespace {

// Finds the CRLF that terminates the line starting at `pos`.
std::size_t find_crlf(std::string_view buf, std::size_t pos) {
    auto idx = buf.find("\r\n", pos);
    return idx == std::string_view::npos ? std::string_view::npos : idx;
}

bool parse_int(std::string_view s, int64_t& out) {
    if (s.empty()) return false;
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && ptr == s.data() + s.size();
}

ParseStatus parse_inline(std::string_view buf, std::vector<std::string>& args,
                         std::size_t& consumed, std::string& error) {
    auto eol = buf.find('\n');
    if (eol == std::string_view::npos) {
        if (buf.size() > kMaxInlineLength) {
            error = "Protocol error: too big inline request";
            return ParseStatus::Error;
        }
        return ParseStatus::Incomplete;
    }
    std::string_view line = buf.substr(0, eol);
    if (!line.empty() && line.back() == '\r') line.remove_suffix(1);

    std::size_t i = 0;
    while (i < line.size()) {
        while (i < line.size() && (line[i] == ' ' || line[i] == '\t')) ++i;
        if (i >= line.size()) break;
        std::size_t start = i;
        while (i < line.size() && line[i] != ' ' && line[i] != '\t') ++i;
        args.emplace_back(line.substr(start, i - start));
    }
    consumed = eol + 1;
    return ParseStatus::Ok;
}

} // namespace

ParseStatus parse_request(std::string_view buf, std::vector<std::string>& args,
                          std::size_t& consumed, std::string& error) {
    args.clear();
    consumed = 0;
    if (buf.empty()) return ParseStatus::Incomplete;
    if (buf[0] != '*') return parse_inline(buf, args, consumed, error);

    auto eol = find_crlf(buf, 0);
    if (eol == std::string_view::npos) return ParseStatus::Incomplete;
    int64_t count = 0;
    if (!parse_int(buf.substr(1, eol - 1), count) || count < 0 ||
        static_cast<std::size_t>(count) > kMaxArrayLength) {
        error = "Protocol error: invalid multibulk length";
        return ParseStatus::Error;
    }

    std::size_t pos = eol + 2;
    args.reserve(static_cast<std::size_t>(count));
    for (int64_t i = 0; i < count; ++i) {
        if (pos >= buf.size()) return ParseStatus::Incomplete;
        if (buf[pos] != '$') {
            error = "Protocol error: expected '$'";
            return ParseStatus::Error;
        }
        auto len_eol = find_crlf(buf, pos);
        if (len_eol == std::string_view::npos) return ParseStatus::Incomplete;
        int64_t len = 0;
        if (!parse_int(buf.substr(pos + 1, len_eol - pos - 1), len) || len < 0 ||
            static_cast<std::size_t>(len) > kMaxBulkLength) {
            error = "Protocol error: invalid bulk length";
            return ParseStatus::Error;
        }
        std::size_t data_start = len_eol + 2;
        std::size_t data_end = data_start + static_cast<std::size_t>(len);
        if (data_end + 2 > buf.size()) return ParseStatus::Incomplete;
        if (buf[data_end] != '\r' || buf[data_end + 1] != '\n') {
            error = "Protocol error: bulk string not terminated by CRLF";
            return ParseStatus::Error;
        }
        args.emplace_back(buf.substr(data_start, static_cast<std::size_t>(len)));
        pos = data_end + 2;
    }
    consumed = pos;
    return ParseStatus::Ok;
}

ParseStatus parse_reply(std::string_view buf, RespValue& out, std::size_t& consumed) {
    consumed = 0;
    if (buf.empty()) return ParseStatus::Incomplete;
    auto eol = find_crlf(buf, 0);
    if (eol == std::string_view::npos) return ParseStatus::Incomplete;
    std::string_view line = buf.substr(1, eol - 1);

    switch (buf[0]) {
        case '+':
            out = RespValue{RespValue::Type::SimpleString, std::string(line)};
            consumed = eol + 2;
            return ParseStatus::Ok;
        case '-':
            out = RespValue{RespValue::Type::Error, std::string(line)};
            consumed = eol + 2;
            return ParseStatus::Ok;
        case ':': {
            int64_t n = 0;
            if (!parse_int(line, n)) return ParseStatus::Error;
            out = RespValue{RespValue::Type::Integer, {}, n};
            consumed = eol + 2;
            return ParseStatus::Ok;
        }
        case '$': {
            int64_t len = 0;
            if (!parse_int(line, len) || len < -1 || len > static_cast<int64_t>(kMaxBulkLength)) {
                return ParseStatus::Error;
            }
            if (len == -1) {
                out = RespValue{};
                consumed = eol + 2;
                return ParseStatus::Ok;
            }
            std::size_t start = eol + 2;
            std::size_t end = start + static_cast<std::size_t>(len);
            if (end + 2 > buf.size()) return ParseStatus::Incomplete;
            out = RespValue{RespValue::Type::BulkString, std::string(buf.substr(start, static_cast<std::size_t>(len)))};
            consumed = end + 2;
            return ParseStatus::Ok;
        }
        case '*': {
            int64_t count = 0;
            if (!parse_int(line, count) || count < -1 || count > static_cast<int64_t>(kMaxArrayLength)) {
                return ParseStatus::Error;
            }
            if (count == -1) {
                out = RespValue{};
                consumed = eol + 2;
                return ParseStatus::Ok;
            }
            RespValue arr{RespValue::Type::Array};
            arr.elements.reserve(static_cast<std::size_t>(count));
            std::size_t pos = eol + 2;
            for (int64_t i = 0; i < count; ++i) {
                RespValue elem;
                std::size_t used = 0;
                auto st = parse_reply(buf.substr(pos), elem, used);
                if (st != ParseStatus::Ok) return st;
                arr.elements.push_back(std::move(elem));
                pos += used;
            }
            out = std::move(arr);
            consumed = pos;
            return ParseStatus::Ok;
        }
        default:
            return ParseStatus::Error;
    }
}

} // namespace shardcache::protocol
