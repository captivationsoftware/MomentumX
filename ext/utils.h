#ifndef MOMENTUMX_UTILS_H
#define MOMENTUMX_UTILS_H

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <boost/container/static_vector.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <chrono>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <tuple>

namespace MomentumX {

    namespace bip = boost::interprocess;

    class Context;  // forward declare context

    namespace Utils {

        struct FDMap {
            void put(int fd, std::string fname) {}
        };

        static const uint64_t NANOS_PER_SECOND = 1000000000;

        static void validate_stream(std::string& stream) {
            // convert to lowercase
            for (char& ch : stream) {
                ch = tolower(ch);
            }

            // remove the protocol string prefix (if present)
            std::string protocol("mx://");
            if (stream.find(protocol) == 0) {
                stream.erase(0, protocol.size());
            }

            if (stream.size() > 32) {
                throw std::runtime_error("Stream length must not exceed 32 characters");
            }

            for (size_t i = 0; i < stream.size(); i++) {
                if (stream[i] == '.') {
                    throw std::runtime_error("Stream must not contain '.' character");
                } else if (stream[i] == ' ' || stream[i] == '\n' || stream[i] == '\r') {
                    throw std::runtime_error("Stream must not contain whitespace characters");
                }
            }
        }

        static uint64_t now() {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
        }

        static void get_timestamps(int fd, uint64_t* read_ts, uint64_t* write_ts) {
            struct stat fileinfo;

            if (fstat(fd, &fileinfo) < 0) {
                // if file was deleted, then set read/write timestamps to null
                if (errno == ENOENT) {
                    if (read_ts != NULL)
                        *read_ts = 0;
                    if (write_ts != NULL)
                        *write_ts = 0;
                } else {
                    throw std::runtime_error("Failed to stat file [errno: " + std::to_string(errno) + "]");
                }
            } else {
                if (read_ts != NULL) {
                    *read_ts = fileinfo.st_atim.tv_sec * NANOS_PER_SECOND + fileinfo.st_atim.tv_nsec;
                }

                if (write_ts != NULL) {
                    *write_ts = fileinfo.st_mtim.tv_sec * NANOS_PER_SECOND + fileinfo.st_mtim.tv_nsec;
                }
            }
        }

        static void set_timestamps(int fd, uint64_t read_ts, uint64_t write_ts) {
            uint64_t existing_read_ts;
            uint64_t existing_write_ts;
            struct timespec updated_times[2];

            // get the existing timestamps
            get_timestamps(fd, &existing_read_ts, &existing_write_ts);

            uint64_t next_read_ts = read_ts == 0 ? existing_read_ts : read_ts;
            uint64_t next_write_ts = write_ts == 0 ? existing_write_ts : write_ts;

            updated_times[0].tv_sec = next_read_ts / NANOS_PER_SECOND;
            updated_times[0].tv_nsec = next_read_ts - (updated_times[0].tv_sec * NANOS_PER_SECOND);
            updated_times[1].tv_sec = next_write_ts / NANOS_PER_SECOND;
            updated_times[1].tv_nsec = next_write_ts - (updated_times[1].tv_sec * NANOS_PER_SECOND);

            if (futimens(fd, updated_times) < 0) {
                throw std::runtime_error("Failed to update file timestamp [errno: " + std::to_string(errno) + "]");
            }
        }

        static size_t page_aligned_size(size_t size) {
            return ceil(size / (double)getpagesize()) * getpagesize();
        }

        class Logger {
           public:
            enum Level { DEBUG = 0, INFO = 1, WARNING = 2, ERROR = 3 };

            static Logger& get_logger() {
                static Logger logger;
                return logger;
            }

            Level get_level() const { return _level; }

            void set_level(Level level) { _level = level; }

            void debug(std::string message) { print(Level::DEBUG, message); }

            void info(std::string message) { print(Level::INFO, message); }

            void warning(std::string message) { print(Level::WARNING, message); }

            void error(std::string message) { print(Level::ERROR, message); }

            Logger(Logger const&) = delete;
            void operator=(Logger const&) = delete;

           private:
            Level _level;

            Logger() : _level(Level::INFO){};

            void print(Level level, std::string message) {
                if (level < _level)
                    return;

                std::lock_guard<std::mutex> lock(_mutex);

                timeval tv;
                gettimeofday(&tv, NULL);
                char dt[sizeof("YYYY-MM-DDTHH:MM:SS.MMMZ")];
                strftime(dt, sizeof(dt), "%FT%T", gmtime(&tv.tv_sec));
                sprintf(dt + strlen(dt), ".%03ldZ", tv.tv_usec / 1000);

                std::cout << dt << " [";
                switch (level) {
                    case Level::DEBUG:
                        std::cout << "DEBUG";
                        break;
                    case Level::INFO:
                        std::cout << "INFO";
                        break;
                    case Level::WARNING:
                        std::cout << "WARNING";
                        break;
                    case Level::ERROR:
                        std::cout << "ERROR";
                        break;
                }
                std::cout << "] MomentumX - " << message << std::endl;
            }

            std::mutex _mutex;
        };

        struct PathConfig {
            explicit PathConfig(const std::string& arg_context_path, const std::string& arg_stream_name)
                : context_path(arg_context_path),
                  stream_name(arg_stream_name),
                  stream_mutex("mx." + stream_name + ".mutex"),
                  stream_path(context_path + "/mx." + arg_stream_name),
                  buffer_mutex_base("mx." + arg_stream_name + ".buffer"),
                  buffer_path_base(stream_path + ".buffer") {}

            ~PathConfig() = default;
            const std::string context_path;
            const std::string stream_name;
            const std::string stream_mutex;
            const std::string stream_path;
            const std::string buffer_mutex_base;
            const std::string buffer_path_base;

            std::string buffer_mutex_name(uint16_t id) const { return buffer_mutex_base + std::string(".") + std::to_string(id) + ".mutex"; }
            std::string buffer_path(uint16_t id) const { return buffer_path_base + std::string(".") + std::to_string(id); }

            inline friend std::ostream& operator<<(std::ostream& os, const PathConfig& paths) {
                os << "PathConfig{ context_path: " << paths.context_path;
                os << ", stream_name: " << paths.stream_name;
                os << ", stream_mutex: " << paths.stream_mutex;
                os << ", stream_path: " << paths.stream_path;
                os << ", buffer_path_base: " << paths.buffer_path_base;
                os << " }";

                return os;
            }
        };

        inline bool is_condition_enabled() {
            const static bool disable = [] {
                // stuff this into a static subroutine, so it only is initialized/logged once
                const char* disable_str = std::getenv("MX_DISABLE_CONDITION");
                const bool disable = disable_str != nullptr && std::strcmp(disable_str, "1") == 0;
                if (disable) {
                    Utils::Logger::get_logger().info(std::string("Disabling condition variables for all producers"));
                }
                return disable;
            }();

            return !disable;
        }

        using OmniMutex = bip::interprocess_mutex;
        using OmniWriteLock = bip::scoped_lock<OmniMutex>;
        struct OmniCondition {
            OmniCondition() = default;

            bip::interprocess_condition inner{};

            /// Compatibility flag to disable conditional and fall back to polling.
            /// This is enabled or disabled based on the environment of the producer,
            /// but the consumer will use the same value since it's stored in shared memory.
            ///
            /// If true, notify and conditional wait is used. If false, polling is used.
            bool enabled{is_condition_enabled()};

            inline void notify_one() {
                if (enabled) {
                    inner.notify_one();
                }
            }

            inline void notify_all() {
                if (enabled) {
                    inner.notify_all();
                }
            }

            template <typename L, class TimePoint, typename Pr>
            inline bool timed_wait(L& lock, const TimePoint& abs_time, Pr pred) {
                if (enabled) {
                    return inner.timed_wait(lock, abs_time, pred);
                } else {
                    // Always check first before looping check
                    if (pred()) {
                        return true;
                    }
                    constexpr auto increment_dur = std::chrono::milliseconds(50);  // arbitrary

                    // Incrementally sleep until we hit our cutoff time
                    while (boost::posix_time::microsec_clock::universal_time() < abs_time) {
                        lock.unlock();
                        std::this_thread::sleep_for(increment_dur);
                        lock.lock();
                        if (pred()) {
                            return true;
                        }
                    }
                    return false;
                }
            }
        };

        template <typename T, size_t Capacity>
        using StaticVector = boost::container::static_vector<T, Capacity>;
    }  // namespace Utils

    template <typename T, size_t Capacity>
    inline std::ostream& operator<<(std::ostream& os, const boost::container::static_vector<T, Capacity>& vec) {
        const size_t sz = vec.size();
        os << "StaticVector";
        os << "{ size:" << sz;
        os << ", values: ";
        {  // Add each value. Braces just for visual clarity.
            const auto beg = vec.begin();
            const auto end = vec.end();
            auto it = beg;

            if (it == end) {
                os << "{}";
            } else {
                os << "{\n" << *it;
                while (++it != end) {
                    os << ",\n" << *it;
                }
                os << "}";
            }
        }
        os << "}";
        return os;
    }

}  // namespace MomentumX

// convenience macros for quick print-based debugging
#ifndef MX_TRACE
#define MX_TRACE std::cout << "[MX_TRACE]: " << __FILE__ << ":" << __LINE__ << " " << __func__ << std::endl;
#define MX_TRACE_ITEM(item) std::cout << "[MX_TRACE]: " << __FILE__ << ":" << __LINE__ << " " << __func__ << " -- " #item ": " << item << std::endl;
#endif

#endif