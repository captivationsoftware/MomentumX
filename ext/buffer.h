#ifndef MOMENTUMX_BUFFER_H
#define MOMENTUMX_BUFFER_H

#include <mutex>
#include <errno.h>
#include <map>
#include <cstring>
#include <list>
#include <iostream>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/file.h>

#include "utils.h"

namespace MomentumX {

    class Buffer {

        static const uint16_t MAX_UINT16_T = -1; // intentionally wrap

        public:
                        
            Buffer(std::string stream, uint16_t id, size_t size=0, bool create=false) :
                _stream(stream),
                _id(id),
                _size(0),
                _is_create(create),
                _fd(shm_allocate(id, O_RDWR | (create ? O_CREAT : 0)))
            {
                if (_fd < 0) {
                    if (_is_create) {
                        throw std::runtime_error("Failed to create shared memory buffer for stream '" + stream + "' [errno: " + std::to_string(errno) + "]");
                    } else {
                        throw std::runtime_error("Failed to open shared memory buffer for stream '" + stream + "' [errno: " + std::to_string(errno) + "]");
                    }
                } 

                // do the ftruncate to resize and (re)mmap
                resize_remap(size);          

                if (_is_create) {
                    Utils::Logger::get_logger().info(
                        std::string("Created Buffer (" + std::to_string((uint64_t) this) + ")")          
                    );
                } else {
                    Utils::Logger::get_logger().info(
                        std::string("Opened Buffer (" + std::to_string((uint64_t) this) + ")")          
                    );
                }

            };

            ~Buffer() {
                if (_fd > -1) {
                    close(_fd);

                    if (_is_create) {
                        shm_unlink(path(_id).c_str());
                    }
                }

                if (_is_create) {
                    Utils::Logger::get_logger().info(
                        std::string("Deleted Buffer (" + std::to_string((uint64_t) this) + ")")          
                    );
                } else {
                    Utils::Logger::get_logger().info(
                        std::string("Closed Buffer (" + std::to_string((uint64_t) this) + ")")          
                    );
                }
            }

            const uint16_t id() {
                return _id;
            }

            const std::string& stream() {
                return _stream;
            }

            size_t size() {
                return _size;
            }

            int fd() {
                return _fd;
            }

            const uint64_t read_ts() {
                uint64_t ts;
                Utils::get_timestamps(_fd, &ts, 0);
                return ts;
            }

            const void read_ts(uint64_t ts) {
                Utils::set_timestamps(_fd, ts, 0);
            }

            const uint64_t write_ts() {
                uint64_t ts;
                Utils::get_timestamps(_fd, 0, &ts);
                return ts;
            }

            const void write_ts(uint64_t ts) {
                return Utils::set_timestamps(_fd, 0, ts);
            }

            uint8_t* address() {
                return _address;
            }
            
        private:
            friend class BufferManager;

            std::string _stream;
            uint16_t _id;
            size_t _size;
            bool _is_create;
            int _fd;
            uint8_t* _address;

            int shm_allocate(uint16_t id, int flags) {
                return shm_open(path(id).c_str(), flags, S_IRWXU);
            } 

            void resize_remap(size_t size) {
                size_t size_required = Utils::page_aligned_size(size);
                if (_size < size_required) {

                    // If we created this buffer (i.e. passed O_CREAT flag), 
                    // then also perform a truncate to ensure that we are sized
                    // appropriately for a call to mmap
                    if (_is_create) {
                        ftruncate(_fd, size_required);
                    }

                    // Mmap the file, or remap if previously mapped
                    uint8_t* address;
                    if (_size == 0) {
                        address = (uint8_t* ) mmap(NULL, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
                    } else {
                        address = (uint8_t* ) mremap(_address, _size, size_required, MREMAP_MAYMOVE);
                    }

                    if (_is_create) {
                        std::memset(address, 0, size_required);
                    }

                    if (address == MAP_FAILED) {
                        throw std::runtime_error("Failed to mmap shared memory file [errno: " + std::to_string(errno) + "]");
                    } else {
                        _address = address;
                        _size = size_required;
                    }
                }
            }

            std::string path(uint16_t id=0) {
                // Build the path to the underlying shm file s
                return std::string("mx." + _stream + ".buffer." + std::to_string(id));
            }

    };

    class BufferManager {
        public:
            BufferManager() { };

            ~BufferManager() {
                std::map<std::string, std::list<Buffer*>> buffers_by_stream;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    buffers_by_stream = _buffers_by_stream;
                }

                for (auto const& tuple : buffers_by_stream) {
                    for (auto const& buffer : tuple.second) {
                        deallocate(buffer);
                    }
                }
            };

            Buffer* allocate(std::string stream, uint16_t id, size_t size=0, bool create=false) {
                std::lock_guard<std::mutex> lock(_mutex); 
                Buffer* buffer = new Buffer(stream, id, size, create);
                _buffers_by_stream[stream].push_back(buffer);
                
                if (_head_buffer_by_stream.count(stream) == 0) {
                    _head_buffer_by_stream[stream] = buffer;
                }
                return buffer;
            }

            void deallocate(Buffer* buffer) {
                std::lock_guard<std::mutex> lock(_mutex);
                _buffers_by_stream[buffer->_stream].remove(buffer);
                delete buffer;
            }

            void deallocate_stream(std::string stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                for (auto const& buffer : _buffers_by_stream[stream]) {
                    delete buffer;
                }
                _buffers_by_stream.erase(stream);
            }

            Buffer* find(std::string stream, uint16_t id) {
                if (id <= 0) {
                    throw std::runtime_error("Buffer id must be greater than 0");
                }

                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    for (auto const& buffer : _buffers_by_stream[stream]) {
                        if (buffer->_id == id) {
                            return buffer;
                        }
                    }
                }

                return NULL;
            }

            bool next_is_head(std::string stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                Buffer* next_buffer = NULL;
                next_buffer = _buffers_by_stream[stream].front();
                return next_buffer == _head_buffer_by_stream[stream];             
            }

            Buffer* next(std::string stream) {
                std::lock_guard<std::mutex> lock(_mutex);

                Buffer* next_buffer = NULL;

                // rotate the buffers
                next_buffer = _buffers_by_stream[stream].front();
                _buffers_by_stream[stream].pop_front();
                _buffers_by_stream[stream].push_back(next_buffer);

                return next_buffer;
            
            }

            Buffer* head_by_stream(std::string stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                return _head_buffer_by_stream[stream];
            }

        private:
            std::map<std::string, std::list<Buffer*>> _buffers_by_stream;
            std::map<std::string, Buffer*> _head_buffer_by_stream;
            std::mutex _mutex;
    };
};

#endif