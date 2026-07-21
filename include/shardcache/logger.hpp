#ifndef SHARDCACHE_LOGGER_HPP
#define SHARDCACHE_LOGGER_HPP

#include <string>
#include <mutex>
#include <iostream>

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
    void log(LogLevel level, const std::string& message);

    void info(const std::string& message) { log(LogLevel::INFO, message); }
    void debug(const std::string& message) { log(LogLevel::DEBUG, message); }
    void warning(const std::string& message) { log(LogLevel::WARNING, message); }
    void error(const std::string& message) { log(LogLevel::ERR, message); }

private:
    Logger() = default;
    LogLevel min_level_{LogLevel::INFO};
    std::mutex mutex_;
};

} // namespace shardcache

#endif // SHARDCACHE_LOGGER_HPP
