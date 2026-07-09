#ifndef MOMENTUMX_UTILS_H
#define MOMENTUMX_UTILS_H

#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>
#include <atomic>
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

        inline struct timespec timespec_now() {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            return ts;
        }

        inline bool timespec_lt(const struct timespec& a, const struct timespec& b) {
            if (a.tv_sec != b.tv_sec) {
                return a.tv_sec < b.tv_sec;
            }
            return a.tv_nsec < b.tv_nsec;
        }

        /// A mutex that lives in POSIX shared memory (PTHREAD_PROCESS_SHARED) and is
        /// robust: if the process holding it dies without unlocking, the next locker
        /// gets EOWNERDEAD instead of blocking forever, and can recover the mutex via
        /// pthread_mutex_consistent(). This is handled transparently in lock()/try_lock()/
        /// timed_lock() below, so callers never see EOWNERDEAD directly.
        class ProcessSharedMutex {
           public:
            ProcessSharedMutex() {
                pthread_mutexattr_t attr;
                if (pthread_mutexattr_init(&attr) != 0) {
                    throw std::runtime_error("pthread_mutexattr_init failed");
                }
                pthread_mutexattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
                pthread_mutexattr_setrobust(&attr, PTHREAD_MUTEX_ROBUST);
                const int rc = pthread_mutex_init(&_mutex, &attr);
                pthread_mutexattr_destroy(&attr);
                if (rc != 0) {
                    throw std::runtime_error("pthread_mutex_init failed [errno: " + std::to_string(rc) + "]");
                }
            }

            ~ProcessSharedMutex() { pthread_mutex_destroy(&_mutex); }

            ProcessSharedMutex(const ProcessSharedMutex&) = delete;
            ProcessSharedMutex& operator=(const ProcessSharedMutex&) = delete;
            ProcessSharedMutex(ProcessSharedMutex&&) = delete;
            ProcessSharedMutex& operator=(ProcessSharedMutex&&) = delete;

            void lock() { recover_if_needed(pthread_mutex_lock(&_mutex)); }

            bool try_lock() {
                const int rc = pthread_mutex_trylock(&_mutex);
                if (rc == EBUSY) {
                    return false;
                }
                recover_if_needed(rc);
                return true;
            }

            bool timed_lock(const struct timespec& abs_time) {
                const int rc = pthread_mutex_timedlock(&_mutex, &abs_time);
                if (rc == ETIMEDOUT) {
                    return false;
                }
                recover_if_needed(rc);
                return true;
            }

            void unlock() { pthread_mutex_unlock(&_mutex); }

            pthread_mutex_t* native_handle() { return &_mutex; }

           private:
            void recover_if_needed(int rc) {
                if (rc == 0) {
                    return;
                }
                if (rc == EOWNERDEAD) {
                    // Previous holder died while holding the lock. We now own it;
                    // mark it consistent so future lockers aren't stuck forever.
                    pthread_mutex_consistent(&_mutex);
                    Logger::get_logger().warning(
                        "Recovered a mutex whose previous holder died while holding it");
                    return;
                }
                throw std::runtime_error("pthread mutex operation failed [errno: " + std::to_string(rc) + "]");
            }

            pthread_mutex_t _mutex{};
        };

        struct defer_lock_t {};
        inline constexpr defer_lock_t defer_lock{};

        /// RAII lock for ProcessSharedMutex, matching the subset of
        /// boost::interprocess::scoped_lock's interface this codebase relies on:
        /// construct-and-lock, construct-deferred (via defer_lock), owns(), timed_lock(),
        /// and unlock-on-destruction.
        class OmniWriteLock {
           public:
            OmniWriteLock() = default;

            explicit OmniWriteLock(ProcessSharedMutex& m) : _mutex(&m) {
                _mutex->lock();
                _owns = true;
            }

            OmniWriteLock(ProcessSharedMutex& m, defer_lock_t) : _mutex(&m), _owns(false) {}

            OmniWriteLock(OmniWriteLock&& other) noexcept : _mutex(other._mutex), _owns(other._owns) {
                other._mutex = nullptr;
                other._owns = false;
            }

            OmniWriteLock& operator=(OmniWriteLock&& other) noexcept {
                if (this != &other) {
                    if (_owns && _mutex) {
                        _mutex->unlock();
                    }
                    _mutex = other._mutex;
                    _owns = other._owns;
                    other._mutex = nullptr;
                    other._owns = false;
                }
                return *this;
            }

            OmniWriteLock(const OmniWriteLock&) = delete;
            OmniWriteLock& operator=(const OmniWriteLock&) = delete;

            ~OmniWriteLock() {
                if (_owns && _mutex) {
                    _mutex->unlock();
                }
            }

            void lock() {
                _mutex->lock();
                _owns = true;
            }

            bool try_lock() {
                _owns = _mutex->try_lock();
                return _owns;
            }

            bool timed_lock(const struct timespec& abs_time) {
                _owns = _mutex->timed_lock(abs_time);
                return _owns;
            }

            void unlock() {
                _mutex->unlock();
                _owns = false;
            }

            bool owns() const { return _owns; }

            ProcessSharedMutex* mutex() const { return _mutex; }

           private:
            ProcessSharedMutex* _mutex{nullptr};
            bool _owns{false};
        };

        /// A condition variable that lives in POSIX shared memory (PTHREAD_PROCESS_SHARED).
        class ProcessSharedCondition {
           public:
            ProcessSharedCondition() {
                pthread_condattr_t attr;
                if (pthread_condattr_init(&attr) != 0) {
                    throw std::runtime_error("pthread_condattr_init failed");
                }
                pthread_condattr_setpshared(&attr, PTHREAD_PROCESS_SHARED);
                const int rc = pthread_cond_init(&_cond, &attr);
                pthread_condattr_destroy(&attr);
                if (rc != 0) {
                    throw std::runtime_error("pthread_cond_init failed [errno: " + std::to_string(rc) + "]");
                }
            }

            ~ProcessSharedCondition() { pthread_cond_destroy(&_cond); }

            ProcessSharedCondition(const ProcessSharedCondition&) = delete;
            ProcessSharedCondition& operator=(const ProcessSharedCondition&) = delete;
            ProcessSharedCondition(ProcessSharedCondition&&) = delete;
            ProcessSharedCondition& operator=(ProcessSharedCondition&&) = delete;

            void notify_one() { pthread_cond_signal(&_cond); }

            void notify_all() { pthread_cond_broadcast(&_cond); }

            /// Waits until pred() is true or abs_time is reached, re-checking pred()
            /// after every wakeup (including spurious ones). Returns pred()'s value
            /// at the time this returns.
            template <typename Pred>
            bool timed_wait(OmniWriteLock& lock, const struct timespec& abs_time, Pred pred) {
                while (!pred()) {
                    const int rc = pthread_cond_timedwait(&_cond, lock.mutex()->native_handle(), &abs_time);
                    if (rc == 0 || rc == EINTR) {
                        continue;
                    } else if (rc == ETIMEDOUT) {
                        return pred();
                    } else if (rc == EOWNERDEAD) {
                        pthread_mutex_consistent(lock.mutex()->native_handle());
                        Logger::get_logger().warning(
                            "Recovered a mutex whose previous holder died mid-condition-wait");
                        continue;
                    } else {
                        throw std::runtime_error("pthread_cond_timedwait failed [errno: " + std::to_string(rc) + "]");
                    }
                }
                return true;
            }

           private:
            pthread_cond_t _cond{};
        };

        using OmniMutex = ProcessSharedMutex;

        struct OmniCondition {
            OmniCondition() = default;

            ProcessSharedCondition inner{};

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

            template <typename L, typename Pr>
            inline bool timed_wait(L& lock, const struct timespec& abs_time, Pr pred) {
                if (enabled) {
                    return inner.timed_wait(lock, abs_time, pred);
                } else {
                    // Always check first before looping check
                    if (pred()) {
                        return true;
                    }
                    constexpr auto increment_dur = std::chrono::milliseconds(50);  // arbitrary

                    // Incrementally sleep until we hit our cutoff time
                    while (timespec_lt(timespec_now(), abs_time)) {
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

        /// Fixed-capacity, inline-storage vector for placing non-movable elements
        /// directly in shared memory (no heap allocation).
        template <typename T, size_t Capacity>
        class StaticVector {
           public:
            StaticVector() = default;
            ~StaticVector() { clear(); }

            StaticVector(const StaticVector&) = delete;
            StaticVector& operator=(const StaticVector&) = delete;
            StaticVector(StaticVector&&) = delete;
            StaticVector& operator=(StaticVector&&) = delete;

            template <typename... Args>
            T& emplace_back(Args&&... args) {
                if (_size >= Capacity) {
                    throw std::length_error("StaticVector capacity exceeded");
                }
                T* slot = ptr_at(_size);
                new (slot) T(std::forward<Args>(args)...);
                ++_size;
                return *slot;
            }

            T& at(size_t idx) {
                if (idx >= _size) {
                    throw std::out_of_range("StaticVector index out of range");
                }
                return *ptr_at(idx);
            }

            const T& at(size_t idx) const {
                if (idx >= _size) {
                    throw std::out_of_range("StaticVector index out of range");
                }
                return *ptr_at(idx);
            }

            size_t size() const { return _size; }

            T* begin() { return ptr_at(0); }
            T* end() { return ptr_at(_size); }
            const T* begin() const { return ptr_at(0); }
            const T* end() const { return ptr_at(_size); }

           private:
            T* ptr_at(size_t idx) { return reinterpret_cast<T*>(_storage) + idx; }
            const T* ptr_at(size_t idx) const { return reinterpret_cast<const T*>(_storage) + idx; }

            void clear() {
                for (size_t i = 0; i < _size; ++i) {
                    ptr_at(i)->~T();
                }
                _size = 0;
            }

            alignas(T) unsigned char _storage[sizeof(T) * Capacity];
            size_t _size{0};
        };
    }  // namespace Utils

    template <typename T, size_t Capacity>
    inline std::ostream& operator<<(std::ostream& os, const Utils::StaticVector<T, Capacity>& vec) {
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
