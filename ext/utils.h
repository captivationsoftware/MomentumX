#ifndef MOMENTUMX_UTILS_H
#define MOMENTUMX_UTILS_H

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <atomic>
#include <boost/interprocess/sync/named_sharable_mutex.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/sharable_lock.hpp>
#include <chrono>
#include <cmath>
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

namespace bip = boost::interprocess;

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

        static std::mutex& _file_lock_mutex() {
            static std::mutex _mutex;
            return _mutex;
        };

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
                os << "{ context_path: " << paths.context_path;
                os << ", stream_name: " << paths.stream_name;
                os << ", stream_mutex: " << paths.stream_mutex;
                os << ", stream_path: " << paths.stream_path;
                os << ", buffer_path_base: " << paths.buffer_path_base;
                os << " }";

                return os;
            }
        };

        static std::mutex fnames_m;
        inline std::map<int, std::string>& fnames() {
            static std::map<int, std::string> singleton;
            return singleton;
        }

        struct OmniMutex2 {
            using LType = int;
            using Counter = int32_t;
            using ThreadMutex = std::shared_mutex;
            // using OmniTuple = std::tuple<LType, Counter, ThreadMutex>;
            struct OmniTuple {
                int l_type{};
                int32_t counter{};
                std::shared_mutex mutex{};
            };

           public:
            explicit OmniMutex2(int fd) : _fd(fd) {}
            ~OmniMutex2() = default;

            inline void pprint(const char* func, int fd) {
                std::lock_guard<std::mutex> lock(fnames_m);
                std::string fname = fnames()[fd];
                if (fname.empty()) {
                    fname = std::to_string(fd);
                }
                std::cout << fname << " - " << func << std::endl;
                // std::lock_guard<std::mutex> lock2(_map_mutex);

                // const auto tup = [this] {
                //     return _fd_to_ltype_counter[_fd];
                // }();
                // const std::string ltype = std::get<0>(tup) == F_WRLCK ? "write" : "read";
                // const int count = std::get<1>(tup);
                // std::cout << fname << " - " << func << " l_type=" << ltype << " c=" << count << std::endl;
            }

            void lock() {
                while (!try_lock()) {  // temporary workaround
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
            }

            bool try_lock() {
                constexpr LType exp_ltype = F_WRLCK;
                std::lock_guard<std::mutex> lock(_map_mutex);
                OmniTuple& omni_ref = get_or_create(exp_ltype);

                if (!omni_ref.mutex.try_lock()) {
                    // cannot obtain thread lock
                    return false;
                }

                if (omni_ref.l_type != exp_ltype) {
                    // previously obtained with different lock type
                    return false;
                }

                if (omni_ref.counter == 0) {
                    // try to lock
                    struct flock lock {
                        exp_ltype, SEEK_SET, 0, 0
                    };

                    const bool has_flock = (fcntl(_fd, F_SETLK, &lock) > -1);
                    if (!has_flock) {
                        // cannot obtain file lock
                        omni_ref.mutex.unlock_shared();
                        return false;
                    }
                }

                // successfully obtained lock
                ++omni_ref.counter;
                return true;
            }

            void unlock() {
                constexpr LType exp_ltype = F_WRLCK;
                std::lock_guard<std::mutex> lock(_map_mutex);
                OmniTuple& omni_ref = get_or_throw(exp_ltype);

                --omni_ref.counter;

                if (omni_ref.counter == 0) {
                    // unlock file lock
                    struct flock unlock {
                        F_UNLCK, SEEK_SET, 0, 0
                    };
                    const int unlock_rc = fcntl(_fd, F_SETLK, &unlock);
                    if (unlock_rc == -1) {
                        Utils::Logger::get_logger().error("Error: an error occurred while write-unlocking file: " + std::to_string(unlock_rc));
                    }

                    // unlock thread lock
                    omni_ref.mutex.unlock();

                    // remove tracking
                    _fd_to_ltype_counter_mutex.erase(_fd);
                }
            }

            void lock_shared() {
                while (!try_lock_shared()) {  // temporary workaround
                    std::this_thread::sleep_for(std::chrono::milliseconds(2));
                }
                // _mutex.lock_shared();
                // inc(F_RDLCK);
                // pprint(__func__, _fd);
                // struct flock lock {
                //     F_RDLCK, SEEK_SET, 0, 0
                // };
                // const int lock_rc = fcntl(_fd, F_SETLKW, &lock);
                // if (lock_rc < 0) {
                //     Utils::Logger::get_logger().error("Error: an error occurred while (blocking) read-locking file: " + std::to_string(lock_rc));
                // }
            }

            bool try_lock_shared() {
                constexpr LType exp_ltype = F_RDLCK;
                std::lock_guard<std::mutex> lock(_map_mutex);
                OmniTuple& omni_ref = get_or_create(exp_ltype);

                if (!omni_ref.mutex.try_lock_shared()) {
                    // cannot obtain thread lock
                    return false;
                }

                if (omni_ref.l_type != exp_ltype) {
                    // previously obtained with different lock type
                    return false;
                }

                if (omni_ref.counter == 0) {
                    // try to lock
                    struct flock lock {
                        exp_ltype, SEEK_SET, 0, 0
                    };

                    const bool has_flock = (fcntl(_fd, F_SETLK, &lock) > -1);
                    if (!has_flock) {
                        // cannot obtain file lock
                        omni_ref.mutex.unlock_shared();
                        return false;
                    }
                }

                // successfully obtained lock
                ++omni_ref.counter;
                return true;
            }

            void unlock_shared() {
                constexpr LType exp_ltype = F_RDLCK;
                std::lock_guard<std::mutex> lock(_map_mutex);
                OmniTuple& omni_ref = get_or_throw(exp_ltype);

                --omni_ref.counter;

                if (omni_ref.counter == 0) {
                    // unlock file lock
                    struct flock unlock {
                        F_UNLCK, SEEK_SET, 0, 0
                    };
                    const int unlock_rc = fcntl(_fd, F_SETLK, &unlock);
                    if (unlock_rc == -1) {
                        Utils::Logger::get_logger().error("Error: an error occurred while read-unlocking file: " + std::to_string(unlock_rc));
                    }

                    // unlock thread lock
                    omni_ref.mutex.unlock_shared();

                    // remove tracking
                    _fd_to_ltype_counter_mutex.erase(_fd);
                }
            }

           private:
            inline OmniTuple& get_or_create(LType l_type) {
                if (_fd_to_ltype_counter_mutex.count(_fd) == 0) {
                    auto ptr = std::make_unique<OmniTuple>();
                    ptr->l_type = l_type;
                }

                return *_fd_to_ltype_counter_mutex[_fd];
            }

            inline OmniTuple& get_or_throw(LType l_type) {
                if (_fd_to_ltype_counter_mutex.count(_fd) == 0) {
                    throw std::logic_error("bookkeeping error: negative counter");
                }
                return *_fd_to_ltype_counter_mutex[_fd];
            }

            // inline int32_t inc(int l_type) {
            //     // if (!_fd_to_ltype_counter_mutex.count(_fd)) {
            //     //     _fd_to_ltype_counter_mutex[_fd] = std::make_tuple(l_type, 0);
            //     // }

            //     auto& tup = _fd_to_ltype_counter[_fd];
            //     std::get<1>(tup)++;
            //     return std::get<1>(tup);
            // }
            // inline int32_t dec(int l_type) {
            //     // if (!_fd_to_ltype_counter.count(_fd)) {
            //     //     throw std::logic_error("bookkeeping error: negative counter");
            //     // }

            //     auto& tup = _fd_to_ltype_counter[_fd];
            //     std::get<1>(tup)--;
            //     return std::get<1>(tup);
            // }

            // std::shared_mutex _mutex;
            int _fd;

            inline static std::mutex _map_mutex;                                                 // shared across process
            inline static std::map<int, std::unique_ptr<OmniTuple>> _fd_to_ltype_counter_mutex;  // shared across process
        };

        // using OmniReadLock = std::shared_lock<OmniMutex>;
        // using OmniWriteLock = std::unique_lock<OmniMutex>;
        using OmniMutex = bip::named_sharable_mutex;
        using OmniReadLock = bip::sharable_lock<OmniMutex>;
        using OmniWriteLock = bip::scoped_lock<OmniMutex>;

        // boost-less version of boost::static_vector for trivial types
        template <typename T, size_t Capacity>
        class StaticVector {
           public:
            size_t capacity() const { return Capacity; }
            size_t size() const { return _size; }

            bool empty() const { return _size == 0; }
            bool full() const { return _size == Capacity; }

            void push_back(const T& value) {
                if (_size == Capacity) {
                    throw std::out_of_range("Cannot push back onto full StaticVector");
                }
                _data.at(_size) = value;
                _size++;
            }

            void pop_back() {
                if (_size == 0) {
                    throw std::out_of_range("Cannot pop back from empty StaticVector");
                }
                _size--;
            }

            T* erase(T* t) {
                if (t < begin() || end() <= t) {
                    throw std::out_of_range("Cannot erase out of bounds iterator");
                }

                std::memmove(t, t + 1, (end() - t) * sizeof(T));  // Shift everything left, clobbering `t`
                pop_back();                                       // Delete final value from end
                return t;
            }

            T& at(size_t i) {
                if (i >= _size) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(i);
            }

            const T& at(size_t i) const {
                if (i >= _size) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(i);
            }

            T* begin() { return _data.begin(); }
            const T* begin() const { return _data.begin(); }

            T* end() { return begin() + _size; }
            const T* end() const { return begin() + _size; }

            T& front() {
                if (_size == 0) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(0);
            }
            const T& front() const {
                if (_size == 0) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(0);
            }

            T& back() {
                if (_size == 0) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(_size - 1);
            }
            const T& back() const {
                if (_size == 0) {
                    throw std::out_of_range("bad index");
                }
                return _data.at(_size - 1);
            }

           private:
            size_t _size{0};
            std::array<T, Capacity> _data{};

            static_assert(std::is_trivially_copyable<T>::value, "Trivally-copyable required for std::memcpy");
            // static_assert(std::is_trivially_copyable<StaticVector<T, Capacity>>::value, "Trivally-copyable required for std::memcpy");
            // static_assert(sizeof(StaticVector<T, Capacity>) == sizeof(T) * Capacity, "Serialized size is unexpected");
        };

    }  // namespace Utils

}  // namespace MomentumX

#endif