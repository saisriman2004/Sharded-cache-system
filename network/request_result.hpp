#ifndef SHARDCACHE_REQUEST_RESULT_HPP
#define SHARDCACHE_REQUEST_RESULT_HPP

#include <string>
#include <optional>

namespace shardcache::network {

enum class RequestStatus {
    Ok,
    NotFound,
    Timeout,
    ConnectionError,
    ProtocolError
};

template <typename T = void>
struct RequestResult {
    RequestStatus status{RequestStatus::Ok};
    std::optional<T> value{std::nullopt};
    std::string message{};

    bool is_ok() const { return status == RequestStatus::Ok; }
    bool is_not_found() const { return status == RequestStatus::NotFound; }
    bool is_error() const {
        return status == RequestStatus::Timeout ||
               status == RequestStatus::ConnectionError ||
               status == RequestStatus::ProtocolError;
    }

    static RequestResult<T> success(T val) {
        RequestResult<T> res;
        res.status = RequestStatus::Ok;
        res.value = std::move(val);
        return res;
    }

    static RequestResult<T> not_found() {
        RequestResult<T> res;
        res.status = RequestStatus::NotFound;
        return res;
    }

    static RequestResult<T> error(RequestStatus st, const std::string& msg = "") {
        RequestResult<T> res;
        res.status = st;
        res.message = msg;
        return res;
    }
};

template <>
struct RequestResult<void> {
    RequestStatus status{RequestStatus::Ok};
    std::string message{};

    bool is_ok() const { return status == RequestStatus::Ok; }
    bool is_not_found() const { return status == RequestStatus::NotFound; }
    bool is_error() const {
        return status == RequestStatus::Timeout ||
               status == RequestStatus::ConnectionError ||
               status == RequestStatus::ProtocolError;
    }

    static RequestResult<void> success() {
        RequestResult<void> res;
        res.status = RequestStatus::Ok;
        return res;
    }

    static RequestResult<void> not_found() {
        RequestResult<void> res;
        res.status = RequestStatus::NotFound;
        return res;
    }

    static RequestResult<void> error(RequestStatus st, const std::string& msg = "") {
        RequestResult<void> res;
        res.status = st;
        res.message = msg;
        return res;
    }
};

} // namespace shardcache::network

#endif // SHARDCACHE_REQUEST_RESULT_HPP
