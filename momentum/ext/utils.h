#ifndef MOMENTUM_UTILS_H
#define MOMENTUM_UTILS_H

#include <string>
#include <pthread.h>
#include <signal.h>
#include <unistd.h>
#include <functional>
#include <mutex>
#include <set>
#include <initializer_list>
#include <sys/stat.h>
#include <fcntl.h>

namespace Momentum { 

    class Context; // forward declare context

    namespace Utils {

        static const uint64_t NANOS_PER_SECOND = 1000000000;

        static void validate_stream(std::string& stream) {
            // convert to lowercase
            for (char &ch : stream) {
                ch = tolower(ch);
            }

            // remove the protocol string prefix (if present)
            std::string protocol("momentum://");
            if (stream.find(protocol) == 0) {
                stream.erase(0, protocol.size());
            } 
            
            if (stream.size() > 32) {
                throw std::string("Stream length must not exceed 32 characters");
            }

            for (size_t i = 0; i < stream.size(); i++) {
                if (stream[i] == '.') {
                    throw std::string("Stream must not contain '.' character");
                }
                else if (stream[i] == ' ' || stream[i] == '\n' || stream[i] == '\r') {
                    throw std::string("Stream must not contain whitespace characters");
                }
            }
        }

        static uint64_t now() {
            return std::chrono::duration_cast<std::chrono::nanoseconds>(
                std::chrono::high_resolution_clock::now().time_since_epoch()
            ).count();
        }

        static void get_timestamps(int fd, uint64_t* read_ts, uint64_t* write_ts) {
            struct stat fileinfo;
            
            if (fstat(fd, &fileinfo) < 0) {
                // if file was deleted, then set read/write timestamps to null
                if (errno == ENOENT) {
                    if (read_ts != NULL) *read_ts = 0;
                    if (write_ts != NULL) *write_ts = 0;
                } else {
                    throw std::string("Failed to stat file [errno: " + std::to_string(errno) + "]");
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
                throw std::string("Failed to update file timestamp [errno: " + std::to_string(errno) + "]");
            }
        }

        static size_t page_aligned_size(size_t size) {
            return ceil(size / (double) getpagesize()) * getpagesize();
        }

        static void read_lock(int fd, off_t from=0, off_t size=0) {
            struct flock lock {
                F_RDLCK,
                SEEK_SET,
                from,
                size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_read_lock(int fd, off_t from=0, off_t size=0) {
            struct flock lock {
                F_RDLCK,
                SEEK_SET,
                from, 
                size
            };
            return fcntl(fd, F_SETLK, &lock) > -1;
        }

        static void write_lock(int fd, off_t from=0, off_t size=0) {
            struct flock lock {
                F_WRLCK,
                SEEK_SET,
                from,
                size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_write_lock(int fd, off_t from=0, off_t size=0) {
            struct flock lock {
                F_WRLCK,
                SEEK_SET,
                from,
                size
            };
            return fcntl(fd, F_SETLK, &lock) > -1;
        }

        static void unlock(int fd, off_t from=0, off_t size=0) {
            struct flock lock {
                F_UNLCK,
                SEEK_SET,
                from,
                size
            };
            fcntl(fd, F_SETLK, &lock);
        }

        class FileReadLock {
            public:
                FileReadLock(int fd, off_t from=0, off_t size=0) :
                    _fd(fd),
                    _from(from),
                    _size(size)
                { }; 

                ~FileReadLock() {
                    unlock();
                }
                
                void lock() {
                    Utils::read_lock(_fd, _from, _size);
                }

                bool try_lock() {
                    return Utils::try_read_lock(_fd, _from, _size);
                }

                void unlock() {
                    Utils::unlock(_fd, _from, _size);
                }

            private:
                int _fd;
                off_t _from, _size;
        };

        class FileWriteLock {
            
            public:
                FileWriteLock(int fd, off_t from=0, off_t size=0) :
                    _fd(fd),
                    _from(from),
                    _size(size)
                { }; 

                ~FileWriteLock() {
                    unlock();
                }
                
                void lock() {
                    Utils::write_lock(_fd, _from, _size);
                }

                bool try_lock() {
                    return Utils::try_write_lock(_fd, _from, _size);
                }

                void unlock() {
                    Utils::unlock(_fd, _from, _size);
                }

            private:
                int _fd;
                off_t _from, _size;
        };

        class Logger {

            public:
                enum Level { DEBUG = 0, INFO = 1, WARNING = 2, ERROR = 3 };
                
                static Logger& get_logger() {
                    static Logger logger;
                    return logger;
                }

                void set_level(Level level) {
                    _level = level;
                }

                void debug(std::string message) {
                    print(Level::DEBUG, message);
                }

                void info(std::string message) {
                    print(Level::INFO, message);
                }

                void warning(std::string message) {
                    print(Level::WARNING, message);
                }

                void error(std::string message) {
                    print(Level::ERROR, message);
                }

                Logger(Logger const&) = delete;
                void operator=(Logger const&) = delete;

            private:
                Level _level;

                Logger() :
                    _level(Level::INFO)
                { };

                void print(Level level, std::string message) {
                    if (level < _level) return;

                    std::lock_guard<std::mutex> lock(_mutex);

                    std::cout << "MOMENTUM - ";
                    switch(level) {
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
                    std::cout << " - " << message << std::endl;
                }

                std::mutex _mutex;
        };
  
    }    


}

#endif