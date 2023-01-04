#ifndef MOMENTUMX_UTILS_H
#define MOMENTUMX_UTILS_H

#include <string>
#include <chrono>
#include <cmath>
#include <ctime>
#include <signal.h>
#include <unistd.h>
#include <functional>
#include <mutex>
#include <set>
#include <initializer_list>
#include <sys/stat.h>
#include <sys/time.h>
#include <fcntl.h>
#include <iostream>

namespace MomentumX { 

    class Context; // forward declare context

    namespace Utils {

        static const uint64_t NANOS_PER_SECOND = 1000000000;

        static void validate_stream(std::string& stream) {
            // convert to lowercase
            for (char &ch : stream) {
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
                }
                else if (stream[i] == ' ' || stream[i] == '\n' || stream[i] == '\r') {
                    throw std::runtime_error("Stream must not contain whitespace characters");
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
            return ceil(size / (double) getpagesize()) * getpagesize();
        }

        static std::mutex& _file_lock_mutex() {
            static std::mutex _mutex;
            return _mutex;
        };

        static void read_lock(int fd, off_t from=0, off_t size=0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_RDLCK,
                SEEK_SET,
                from,
                size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_read_lock(int fd, off_t from=0, off_t size=0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_RDLCK,
                SEEK_SET,
                from, 
                size
            };
            return fcntl(fd, F_SETLK, &lock) > -1;
        }

        static void write_lock(int fd, off_t from=0, off_t size=0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
            struct flock lock {
                F_WRLCK,
                SEEK_SET,
                from,
                size
            };
            fcntl(fd, F_SETLKW, &lock);
        }

        static bool try_write_lock(int fd, off_t from=0, off_t size=0) {
            std::lock_guard<std::mutex> mlock(_file_lock_mutex());
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

        template <typename T>
        static void with_write_lock(int fd, const T& func) {
            try {
                write_lock(fd);
                func();
                unlock(fd);
            } catch(...) {
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
            } catch(...) {
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

                Level get_level() const { 
                    return _level;
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

                    timeval tv;
                    gettimeofday(&tv, NULL);
                    char dt[sizeof("YYYY-MM-DDTHH:MM:SS.MMMZ")];
                    strftime(dt, sizeof(dt), "%FT%T", gmtime(&tv.tv_sec));
                    sprintf(dt + strlen(dt), ".%03ldZ", tv.tv_usec / 1000);

                    std::cout << dt << " [";
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
                    std::cout << "] MomentumX - " << message << std::endl;
                }

                std::mutex _mutex;
        };
  
    }    


}

#endif