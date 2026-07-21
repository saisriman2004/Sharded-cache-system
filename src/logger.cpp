#include "shardcache/logger.hpp"
#include <chrono>
#include <iomanip>
#include <ctime>

namespace shardcache {

Logger& Logger::instance() {
    static Logger inst;
    return inst;
}

void Logger::set_level(LogLevel level) {
    std::lock_guard lock(mutex_);
    min_level_ = level;
}

void Logger::log(LogLevel level, const std::string& message) {
    std::lock_guard lock(mutex_);
    if (level < min_level_) return;

    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);

    const char* lvl_str = "INFO";
    switch (level) {
        case LogLevel::DEBUG: lvl_str = "DEBUG"; break;
        case LogLevel::INFO: lvl_str = "INFO"; break;
        case LogLevel::WARNING: lvl_str = "WARN"; break;
        case LogLevel::ERR: lvl_str = "ERROR"; break;
    }

    std::cout << "[" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S") << "] ["
              << lvl_str << "] " << message << std::endl;
}

} // namespace shardcache
