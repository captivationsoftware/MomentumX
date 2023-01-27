#ifndef MOMENTUMX_STREAM_H
#define MOMENTUMX_STREAM_H

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <tuple>

#include "buffer.h"
#include "utils.h"

namespace MomentumX {

    class Stream {
       public:
        struct BufferState {
            uint16_t buffer_id;
            size_t buffer_size, buffer_count, data_size;
            uint64_t data_timestamp, iteration;

            BufferState()
                : buffer_id(0),
                  buffer_size(0),
                  buffer_count(0),
                  data_size(0),
                  data_timestamp(0),
                  iteration(0){};

            BufferState(const BufferState& bs)
                : buffer_id(bs.buffer_id),
                  buffer_size(bs.buffer_size),
                  buffer_count(bs.buffer_count),
                  data_size(bs.data_size),
                  data_timestamp(bs.data_timestamp),
                  iteration(bs.iteration) {}

            BufferState(uint16_t id,
                        size_t buffer_size,
                        size_t buffer_count,
                        size_t data_size,
                        uint64_t timestamp,
                        uint64_t iteration)
                : buffer_id(id),
                  buffer_size(buffer_size),
                  buffer_count(buffer_count),
                  data_size(data_size),
                  data_timestamp(timestamp),
                  iteration(iteration){};
        };

        enum Role { CONSUMER, PRODUCER };

        Stream(std::string name, size_t buffer_size = 0, size_t buffer_count = 0, bool sync = false, Role role = Role::CONSUMER);

        ~Stream();

        bool is_alive();

        const std::string& name();

        int fd();

        bool sync();

        void sync(bool _sync);

        size_t buffer_size();

        void buffer_size(size_t _buffer_size);

        size_t buffer_count();

        void buffer_count(size_t _buffer_count);

        std::list<BufferState> buffer_states(bool sort = false, uint64_t minimum_timestamp = 0);

        void update_buffer_state(BufferState* buffer_state);

        std::set<Context*> subscribers();

        void add_subscriber(Context* context);

        void remove_subscriber(Context* context);

        std::set<Context*> pending_acknowledgements();

        void set_pending_acknowledgements();

        void remove_pending_acknowledgement(Context* context);

       private:
        friend class StreamManager;

        std::string _name;
        Role _role;
        std::string _path;
        int _fd;
        size_t _stream_data_size, _buffer_state_size, _subscribers_size, _acknowledgements_size;
        size_t _stream_data_offset, _buffer_state_offset, _subscribers_offset, _acknowledgements_offset;

        char* _data;
    };

    class StreamManager {
       public:
        StreamManager(Context* context, BufferManager* buffer_manager);
        ~StreamManager();

        Stream* find(std::string name);

        Stream* create(std::string name, size_t buffer_size, size_t buffer_count = 0, bool sync = false, Stream::Role role = Stream::Role::CONSUMER);

        void destroy(Stream* stream);
        bool is_subscribed(std::string name);
        Stream* subscribe(std::string name);
        void unsubscribe(Stream* stream);
        Stream::BufferState* next_buffer_state(Stream* stream);
        bool send_buffer_state(Stream* stream, Stream::BufferState* buffer_state);
        Stream::BufferState* receive_buffer_state(Stream* stream, uint64_t minimum_timestamp = 1);
        Stream::BufferState* get_by_buffer_id(Stream* stream, uint16_t buffer_id);
        void flush_buffer_state(Stream* stream);
        void release_buffer_state(Stream* stream, Stream::BufferState* buffer_state);
        size_t subscriber_count(Stream* stream);

       private:
        Context* _context;
        BufferManager* _buffer_manager;
        std::map<std::string, Stream*> _stream_by_name;
        std::map<Stream*, std::list<Buffer*>> _buffers_by_stream;
        std::map<Stream*, Buffer*> _current_buffer_by_stream;
        std::map<Stream*, uint64_t> _iteration_by_stream;
        std::mutex _mutex;
    };
};  // namespace MomentumX

#endif