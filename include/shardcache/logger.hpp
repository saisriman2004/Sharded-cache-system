#ifndef SHARDCACHE_LOGGER_HPP
#define SHARDCACHE_LOGGER_HPP

#include <string>
#include <mutex>
#include <iostream>
#include <atomic>

namespace shardcache {

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERR
};

class Logger {
public:
    static Logger& instance();

    void set_level(LogLevel level);
    // Lock-free check so hot paths can skip building messages that would be dropped.
    bool enabled(LogLevel level) const { return level >= min_level_.load(std::memory_order_relaxed); }
    void log(LogLevel level, const std::string& message);

    void info(const std::string& message) { log(LogLevel::INFO, message); }
    void debug(const std::string& message) { log(LogLevel::DEBUG, message); }
    void warning(const std::string& message) { log(LogLevel::WARNING, message); }
    void error(const std::string& message) { log(LogLevel::ERR, message); }

private:
    Logger() = default;
    std::atomic<LogLevel> min_level_{LogLevel::INFO};
    std::mutex mutex_;
};

} // namespace shardcache

#endif // SHARDCACHE_LOGGER_HPP
