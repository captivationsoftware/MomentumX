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
#include <shared_mutex>

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

        inline Utils::OmniMutex& mutex() const { return *_mutex; }

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
        mutable std::optional<Utils::OmniMutex> _mutex;  // Optional to defer construction. Always valid post-constructor.

        void resize_remap(size_t size);
    };

    class BufferManager {
       public:
        BufferManager() = default;
        ~BufferManager() = default;

        std::shared_ptr<Buffer> allocate(const Utils::PathConfig& paths, uint16_t id, size_t size = 0, bool create = false);
        void deallocate_stream(const Utils::PathConfig& paths);
        std::shared_ptr<Buffer> find(const Utils::PathConfig& paths, uint16_t id);
        std::shared_ptr<Buffer> peek_next(const Utils::PathConfig& paths);
        std::shared_ptr<Buffer> next(const Utils::PathConfig& paths);

       private:
        std::map<std::string, std::list<std::shared_ptr<Buffer>>> _buffers_by_stream;
        std::mutex _mutex;
    };
};  // namespace MomentumX

#endif