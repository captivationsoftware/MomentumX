#ifndef MOMENTUMX_BUFFER_H
#define MOMENTUMX_BUFFER_H

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <mutex>

#include "utils.h"

namespace MomentumX {

    class Buffer {
        static const uint16_t MAX_UINT16_T;

       public:
        Buffer(std::string stream, uint16_t id, size_t size = 0, bool create = false);
        ~Buffer();
        const uint16_t id();
        const std::string& stream();
        size_t size();
        int fd();
        const uint64_t read_ts();
        const void read_ts(uint64_t ts);
        const uint64_t write_ts();
        const void write_ts(uint64_t ts);
        uint8_t* address();

       private:
        friend class BufferManager;

        std::string _stream;
        uint16_t _id;
        size_t _size;
        bool _is_create;
        int _fd;
        uint8_t* _address;

        int shm_allocate(uint16_t id, int flags);
        void resize_remap(size_t size);
        std::string path(uint16_t id = 0);
    };

    class BufferManager {
       public:
        BufferManager();
        ~BufferManager();

        Buffer* allocate(std::string stream, uint16_t id, size_t size = 0, bool create = false);
        void deallocate(Buffer* buffer);
        void deallocate_stream(std::string stream);
        Buffer* find(std::string stream, uint16_t id);
        bool next_is_head(std::string stream);
        Buffer* next(std::string stream);
        Buffer* head_by_stream(std::string stream);

       private:
        std::map<std::string, std::list<Buffer*>> _buffers_by_stream;
        std::map<std::string, Buffer*> _head_buffer_by_stream;
        std::mutex _mutex;
    };
};  // namespace MomentumX

#endif