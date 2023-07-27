#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <boost/interprocess/creation_tags.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/sync/named_sharable_mutex.hpp>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <nlohmann/json.hpp>

#include "buffer.h"
#include "utils.h"

namespace MomentumX {

    const uint16_t Buffer::MAX_UINT16_T = -1;  // intentionally wrap

    Buffer::Buffer(const Utils::PathConfig& paths, uint16_t id, size_t size, bool create)
        : _paths(paths),
          _backing_filepath(paths.buffer_path(id)),
          _backing_mutex_name(paths.buffer_mutex_name(id)),
          _id(id),
          _size(0),
          _is_create(create),
          _fd(open(_backing_filepath.c_str(), O_RDWR | (create ? O_CREAT : 0), S_IRWXU)),
          _address(nullptr) {
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
            Utils::Logger::get_logger().debug(std::string("Created Producer Buffer (" + std::to_string((uint64_t)this) + ")"));
        } else {
            Utils::Logger::get_logger().debug(std::string("Created Consumer Buffer (" + std::to_string((uint64_t)this) + ")"));
        }
    };

    Buffer::~Buffer() {
        bool has_errors = false;
        int return_val;
        if (_address != MAP_FAILED) {
            return_val = munmap(_address, _size);
            if (return_val != 0) {
                has_errors |= true;
                Utils::Logger::get_logger().warning(std::string("Buffer Unmap Failed (" + std::to_string((uint64_t)this) + ")"));
            } else {
                Utils::Logger::get_logger().debug(std::string("Buffer Unmapped (" + std::to_string((uint64_t)this) + ")"));
            }
        }

        if (_fd > -1) {
            return_val = ::close(_fd);

            if (return_val != 0) {
                has_errors |= true;
                Utils::Logger::get_logger().warning(std::string("Buffer Close Failed (" + std::to_string((uint64_t)this) + ")"));
            } else {
                Utils::Logger::get_logger().debug(std::string("Buffer Closed (" + std::to_string((uint64_t)this) + ")"));
            }

            if (_is_create) {
                return_val = std::remove(_backing_filepath.c_str());
                if (return_val != 0) {
                    has_errors |= true;
                    std::stringstream ss;
                    ss << "Unable to delete buffer file \"" << _backing_filepath << "\" with error: " << return_val;
                    Utils::Logger::get_logger().warning(ss.str());
                }
            }
        }

        std::stringstream ss;
        ss << "Destroyed " << (_is_create ? "Producer" : "Consumer") << " Buffer (" << std::to_string((uint64_t)this) << ")";

        if (has_errors) {
            ss << " [POSSIBLE MEMORY LEAK DETECTED!]";
            Utils::Logger::get_logger().warning(ss.str());
        } else {
            Utils::Logger::get_logger().debug(ss.str());
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
                const int ft_rc = ftruncate(_fd, size_required);
                if (ft_rc) {
                    Utils::Logger::get_logger().warning("Failed to resize file with error code: " + std::to_string(ft_rc));
                }
            }

            // Mmap the file, or remap if previously mapped
            uint8_t* address;
            if (_size == 0) {
                address = (uint8_t*)mmap(nullptr, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
            } else {
                address = (uint8_t*)mremap(_address, _size, size_required, MREMAP_MAYMOVE);
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

    std::string BufferState::dumps(int64_t indent) const {
        return nlohmann::json(*this).dump(indent);
    }

    void to_json(nlohmann::json& j, const BufferState& bs) {
        j = nlohmann::json{{"buffer_id", bs.buffer_id}, {"buffer_size", bs.buffer_size},       {"buffer_count", bs.buffer_count},
                           {"data_size", bs.data_size}, {"data_timestamp", bs.data_timestamp}, {"iteration", bs.iteration}};
    }

    std::shared_ptr<Buffer> BufferManager::allocate(const Lock& lock, const Utils::PathConfig& paths, uint16_t id, size_t size, bool create) {
        auto buffer = std::make_shared<Buffer>(paths, id, size, create);
        _buffers_by_stream[paths.stream_name].push_back(buffer);

        return buffer;
    }

    void BufferManager::deallocate_stream(const Lock& lock, const Utils::PathConfig& paths) {
        _buffers_by_stream.erase(paths.stream_name);
    }

    std::shared_ptr<Buffer> BufferManager::find(const Lock& lock, const Utils::PathConfig& paths, uint16_t id) {
        // if (id <= 0) {
        //     throw std::runtime_error("Buffer id must be greater than 0");
        // }

        const auto& stream = paths.stream_name;
        for (auto const& buffer : _buffers_by_stream[stream]) {
            if (buffer->_id == id) {
                return buffer;
            }
        }

        return NULL;
    }

    std::shared_ptr<Buffer> BufferManager::next(const Lock& lock, const Utils::PathConfig& paths) {
        // rotate the buffers
        const auto& stream = paths.stream_name;
        std::shared_ptr<Buffer> next_buffer = _buffers_by_stream[stream].front();
        _buffers_by_stream[stream].pop_front();
        _buffers_by_stream[stream].push_back(next_buffer);

        return next_buffer;
    }

    std::shared_ptr<Buffer> BufferManager::peek_next(const Lock& lock, const Utils::PathConfig& paths) {
        return _buffers_by_stream[paths.stream_name].front();
    }

}  // namespace MomentumX
