#include <mutex>
#include <errno.h>
#include <map>
#include <cstring>
#include <list>
#include <iostream>
#include <sys/mman.h>
#include <sys/file.h>

#include "utils.h"
#include "buffer.h"

namespace MomentumX {

    const uint16_t Buffer::MAX_UINT16_T = -1; // intentionally wrap

    Buffer::Buffer(const Utils::PathConfig& paths, uint16_t id, size_t size, bool create) :
        _paths(paths),
        _backing_filepath(paths.buffer_path(id)),
        _id(id),
        _size(0),
        _is_create(create),
        _fd(open(_backing_filepath.c_str(), O_RDWR | (create ? O_CREAT : 0), S_IRWXU))
    {
        if (_fd < 0) {
            if (_is_create) {
                throw std::runtime_error("Failed to create shared memory buffer for stream '" + _backing_filepath + "' [errno: " + std::to_string(errno) + "]");
            } else {
                throw std::runtime_error("Failed to open shared memory buffer for stream '" + _backing_filepath + "' [errno: " + std::to_string(errno) + "]");
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

    Buffer::~Buffer() {
        if (_fd > -1) {
            close(_fd);

            if (_is_create) {
                int return_val = std::remove(_backing_filepath.c_str()); 
                if (return_val != 0) {
                    std::stringstream ss;
                    ss << "Unable to delete buffer file \"" << _backing_filepath << "\" with error: " << return_val;
                    Utils::Logger::get_logger().error(ss.str());
                }
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

    const uint16_t Buffer::id() {
        return _id;
    }

    size_t Buffer::size() {
        return _size;
    }

    int Buffer::fd() {
        return _fd;
    }

    const uint64_t Buffer::read_ts() {
        uint64_t ts;
        Utils::get_timestamps(_fd, &ts, 0);
        return ts;
    }

    const void Buffer::read_ts(uint64_t ts) {
        Utils::set_timestamps(_fd, ts, 0);
    }

    const uint64_t Buffer::write_ts() {
        uint64_t ts;
        Utils::get_timestamps(_fd, 0, &ts);
        return ts;
    }

    const void Buffer::write_ts(uint64_t ts) {
        return Utils::set_timestamps(_fd, 0, ts);
    }

    uint8_t* Buffer::address() {
        return _address;
    }
    
    void Buffer::resize_remap(size_t size) {
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
                address = (uint8_t* ) mmap(nullptr, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
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
                _size = size;
            }
        }
    }

    BufferManager::BufferManager() {}

    BufferManager::~BufferManager() {
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

    Buffer* BufferManager::allocate(const Utils::PathConfig& paths, uint16_t id, size_t size, bool create) {
        std::lock_guard<std::mutex> lock(_mutex); 
        Buffer* buffer = new Buffer(paths, id, size, create);
        _buffers_by_stream[paths.stream_name].push_back(buffer);

        return buffer;
    }

    void BufferManager::deallocate(Buffer* buffer) {
        std::lock_guard<std::mutex> lock(_mutex);
        _buffers_by_stream[buffer->_paths.stream_name].remove(buffer);
        delete buffer;
    }

    void BufferManager::deallocate_stream(const Utils::PathConfig& paths) {
        std::lock_guard<std::mutex> lock(_mutex);
        const auto& stream = paths.stream_name;
        for (auto const& buffer : _buffers_by_stream[stream]) {
            delete buffer;
        }
        _buffers_by_stream.erase(stream);
    }

    Buffer* BufferManager::find(const Utils::PathConfig& paths, uint16_t id) {
        if (id <= 0) {
            throw std::runtime_error("Buffer id must be greater than 0");
        }

        {
            std::lock_guard<std::mutex> lock(_mutex);
            const auto& stream = paths.stream_name;
            for (auto const& buffer : _buffers_by_stream[stream]) {
                if (buffer->_id == id) {
                    return buffer;
                }
            }
        }

        return NULL;
    }

    Buffer* BufferManager::next(const Utils::PathConfig& paths) {
        std::lock_guard<std::mutex> lock(_mutex);

        Buffer* next_buffer = NULL;

        // rotate the buffers
        const auto& stream = paths.stream_name;
        next_buffer = _buffers_by_stream[stream].front();
        _buffers_by_stream[stream].pop_front();
        _buffers_by_stream[stream].push_back(next_buffer);

        return next_buffer;
    }

    Buffer* BufferManager::peek_next(const Utils::PathConfig& paths) {
        std::lock_guard<std::mutex> lock(_mutex);
        const auto& stream = paths.stream_name;
        return _buffers_by_stream[stream].front();
    }
} // namespace MomentumX