#include "shardcache/single_flight.hpp"

namespace shardcache {

std::string SingleFlight::do_call(const std::string& key, FetchFn fn) {
    std::shared_ptr<Call> call;
    bool is_leader = false;

    {
        std::unique_lock lock(map_mutex_);
        auto it = calls_.find(key);
        if (it != calls_.end()) {
            call = it->second;
        } else {
            call = std::make_shared<Call>();
            calls_[key] = call;
            is_leader = true;
        }
    }

    if (!is_leader) {
        std::unique_lock call_lock(call->mutex);
        call->cv.wait(call_lock, [&call]() { return call->done; });
        if (call->exc) {
            std::rethrow_exception(call->exc);
        }
        return call->val;
    }

    std::string result;
    std::exception_ptr exc = nullptr;
    try {
        result = fn();
    } catch (...) {
        exc = std::current_exception();
    }

    {
        std::unique_lock call_lock(call->mutex);
        call->val = result;
        call->exc = exc;
        call->done = true;
    }
    call->cv.notify_all();

    {
        std::unique_lock lock(map_mutex_);
        calls_.erase(key);
    }

    if (exc) {
        std::rethrow_exception(exc);
    }

    return result;
}

} // namespace shardcache
