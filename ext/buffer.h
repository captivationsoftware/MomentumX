#ifndef MOMENTUMX_BUFFER_H
#define MOMENTUMX_BUFFER_H

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <nlohmann/json_fwd.hpp>
#include <shared_mutex>
#include <sstream>

#include "utils.h"

namespace MomentumX {

    class Buffer {
        static const uint16_t MAX_UINT16_T;

       public:
        Buffer(const Utils::PathConfig& paths, uint16_t id, size_t size = 0, bool create = false);
        ~Buffer();
        const uint16_t id();
        size_t size();
        int fd();
        const uint64_t read_ts();
        const void read_ts(uint64_t ts);
        const uint64_t write_ts();
        const void write_ts(uint64_t ts);
        uint8_t* address();

        Buffer(Buffer&&) = delete;
        Buffer(const Buffer&) = delete;
        Buffer& operator=(Buffer&&) = delete;
        Buffer& operator=(const Buffer&) = delete;

       private:
        friend class BufferManager;

        const Utils::PathConfig _paths;
        const std::string _backing_filepath;
        const std::string _backing_mutex_name;
        uint16_t _id;
        size_t _size;
        bool _is_create;
        int _fd;
        uint8_t* _address;

        void resize_remap(size_t size);
    };

    struct BufferState {
        uint16_t buffer_id{0};
        size_t buffer_size{0}, buffer_count{0}, data_size{0};
        uint64_t data_timestamp{0}, iteration{0};

        BufferState() = default;

        BufferState(uint16_t id, size_t buffer_size, size_t buffer_count, size_t data_size, uint64_t timestamp, uint64_t iteration)
            : buffer_id(id), buffer_size(buffer_size), buffer_count(buffer_count), data_size(data_size), data_timestamp(timestamp), iteration(iteration){};

        inline friend bool operator==(const BufferState& lhs, const BufferState& rhs) {
            return &lhs == &rhs || (lhs.buffer_id == rhs.buffer_id && lhs.iteration == rhs.iteration);
        }
        inline friend bool operator!=(const BufferState& lhs, const BufferState& rhs) { return !(lhs == rhs); }

        std::string dumps(int64_t indent = 2) const;
        friend void to_json(nlohmann::json& j, const BufferState& bs);
        inline friend std::ostream& operator<<(std::ostream& os, const BufferState& b) {
            os << "{ buffer_id: " << b.buffer_id;
            os << ", buffer_size: " << b.buffer_size;
            os << ", buffer_count: " << b.buffer_count;
            os << ", data_size: " << b.data_size;
            os << ", data_timestamp: " << b.data_timestamp;
            os << ", iteration: " << b.iteration;
            os << "}";
            return os;
        }
    };
    static_assert(std::is_trivially_copy_assignable<BufferState>::value, "BufferState is placed in shared memory");
    static_assert(std::is_trivially_copy_constructible<BufferState>::value, "BufferState is placed in shared memory");
    static_assert(std::is_trivially_move_assignable<BufferState>::value, "BufferState is placed in shared memory");
    static_assert(std::is_trivially_move_constructible<BufferState>::value, "BufferState is placed in shared memory");

    class BufferManager {
       public:
        struct Mutex : std::mutex {};         // subclass mutex for type checking
        using Lock = std::lock_guard<Mutex>;  // alias lock to maintain ctors
        BufferManager() = default;
        ~BufferManager() = default;

        std::shared_ptr<Buffer> allocate(const Lock& lock, const Utils::PathConfig& paths, uint16_t id, size_t size = 0, bool create = false);
        void deallocate_stream(const Lock& lock, const Utils::PathConfig& paths);
        std::shared_ptr<Buffer> find(const Lock& lock, const Utils::PathConfig& paths, uint16_t id);
        std::shared_ptr<Buffer> peek_next(const Lock& lock, const Utils::PathConfig& paths);
        std::shared_ptr<Buffer> next(const Lock& lock, const Utils::PathConfig& paths);

        inline Lock get_buffer_manager_lock() { return Lock(_buffer_manager_mutex); }

       private:
        std::map<std::string, std::list<std::shared_ptr<Buffer>>> _buffers_by_stream;
        Mutex _buffer_manager_mutex;
    };
};  // namespace MomentumX

#endif