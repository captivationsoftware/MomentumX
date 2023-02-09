#ifndef MOMENTUMX_UTILS_H
#define MOMENTUMX_UTILS_H

#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>
#include <chrono>
#include <cmath>
#include <cstring>
#include <ctime>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <tuple>

namespace MomentumX {

    class Context;  // forward declare context

    namespace Utils {

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

        static void read_lock(int fd, off_t from = 0, off_t size = 0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_RDLCK, SEEK_SET, from, size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_read_lock(int fd, off_t from = 0, off_t size = 0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_RDLCK, SEEK_SET, from, size
            };
            return fcntl(fd, F_SETLK, &lock) > -1;
        }

        static void write_lock(int fd, off_t from = 0, off_t size = 0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_WRLCK, SEEK_SET, from, size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_write_lock(int fd, off_t from = 0, off_t size = 0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_WRLCK, SEEK_SET, from, size
            };
            return fcntl(fd, F_SETLK, &lock) > -1;
        }

        static void unlock(int fd, off_t from = 0, off_t size = 0) {
            struct flock lock {
                F_UNLCK, SEEK_SET, from, size
            };
            fcntl(fd, F_SETLK, &lock);
        }

        template <typename T>
        static void with_write_lock(int fd, const T& func) {
            try {
                write_lock(fd);
                func();
                unlock(fd);
            } catch (...) {
                unlock(fd);
                throw;
            }
        }

        template <typename T>
        static void with_read_lock(int fd, const T& func) {
            try {
                read_lock(fd);
                func();
                unlock(fd);
            } catch (...) {
                unlock(fd);
                throw;
            }
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
                  stream_path(context_path + "/mx." + arg_stream_name),
                  buffer_path_base(stream_path + ".buffer") {}

            ~PathConfig() = default;
            const std::string context_path;
            const std::string stream_name;
            const std::string stream_path;
            const std::string buffer_path_base;

            std::string buffer_path(uint16_t id) const { return buffer_path_base + std::string(".") + std::to_string(id); }

            inline friend std::ostream& operator<<(std::ostream& os, const PathConfig& paths) {
                os << "{ context_path: " << paths.context_path;
                os << ", stream_name: " << paths.stream_name;
                os << ", stream_path: " << paths.stream_path;
                os << ", buffer_path_base: " << paths.buffer_path_base;
                os << " }";

                return os;
            }
        };

        class ScopedReadLock {
           public:
            ScopedReadLock(int fd, off_t start = 0, off_t len = 0) : _fd(fd), _start(start), _len(len) { read_lock(fd, start, len); }
            ~ScopedReadLock() { unlock(_fd, _start, _len); }

           private:
            int _fd;
            off_t _start;
            off_t _len;
        };

        struct ScopedWriteLock {
           public:
            ScopedWriteLock(int fd, off_t start = 0, off_t len = 0) : _fd(fd), _start(start), _len(len) { write_lock(fd, start, len); }
            ~ScopedWriteLock() { unlock(_fd, _start, _len); }

           private:
            int _fd;
            off_t _start;
            off_t _len;
        };

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
                pop_back();                         // Delete final value from end
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