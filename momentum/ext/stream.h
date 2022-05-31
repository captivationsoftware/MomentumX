#ifndef MOMENTUM_STREAM_H
#define MOMENTUM_STREAM_H

#include <mutex>
#include <errno.h>
#include <map>
#include <set>
#include <list>
#include <tuple>
#include <iostream>
#include <sys/shm.h>
#include <sys/mman.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <cstring>
#include <condition_variable>

#include "buffer.h"
#include "utils.h"

namespace Momentum {

    class Stream {

        public:    

            struct BufferState {
                uint16_t buffer_id;
                size_t buffer_size, buffer_count, data_size;
                uint64_t data_timestamp, iteration;

                BufferState() :
                    buffer_id(0),
                    buffer_size(0),
                    buffer_count(0),
                    data_size(0),
                    data_timestamp(0),
                    iteration(0)
                { };

                BufferState(const BufferState& bs) :
                    buffer_id(bs.buffer_id),
                    buffer_size(bs.buffer_size),
                    buffer_count(bs.buffer_count),
                    data_size(bs.data_size),
                    data_timestamp(bs.data_timestamp),
                    iteration(bs.iteration)
                { }

                BufferState(uint16_t id, size_t buffer_size, size_t buffer_count, size_t data_size, uint64_t timestamp, uint64_t iteration) :
                    buffer_id(id),
                    buffer_size(buffer_size),
                    buffer_count(buffer_count),
                    data_size(data_size),
                    data_timestamp(timestamp),
                    iteration(iteration)
                {};
            };

            enum Role { CONSUMER, PRODUCER };

            Stream(std::string name, size_t buffer_size=0, size_t buffer_count=0, bool sync=false, Role role=Role::CONSUMER) :
                _name(name),
                _role(role),
                _path("momentum." + name),
                _fd(shm_open(_path.c_str(), O_RDWR | (_role == Role::PRODUCER ? (O_CREAT | O_EXCL) : 0), S_IRWXU)),
                _stream_data_read_lock(_fd, 0, getpagesize()),
                _buffer_state_read_lock(_fd, getpagesize(), getpagesize()),
                _subscriptions_read_lock(_fd, getpagesize() * 2, getpagesize()),
                _acknowledgements_read_lock(_fd, getpagesize() * 3, getpagesize()),
                _stream_data_write_lock(_fd, 0, getpagesize()),
                _buffer_state_write_lock(_fd, getpagesize(), getpagesize()),
                _subscriptions_write_lock(_fd, getpagesize() * 2, getpagesize()),
                _acknowledgements_write_lock(_fd, getpagesize() * 3, getpagesize())
            {
                if (_fd < 0) {
                    throw std::string("Failed to create shared memory stream file [errno: " + std::to_string(errno) + "]");
                } 

                // 1 page for stream data, 
                // 1 page for buffer state 
                // 1 page for stream subscriptions 
                // 1 page for acknowledgements
                size_t size_required = getpagesize() * 4; 

                if (role == Role::PRODUCER) {
                    ftruncate(_fd, size_required);
                }

                _data = (char* ) mmap(NULL, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
                if (_data == MAP_FAILED) {
                    throw std::string("Failed to mmap shared memory stream file [errno: " + std::to_string(errno) + "]");
                }

                if (_role == PRODUCER) {
                    // initialize to 0s
                    memset(_data, 0, size_required);
                    
                    this->buffer_size(buffer_size);
                    this->buffer_count(buffer_count);
                    this->sync(sync);
                }

                std::cout << "Created Stream (" << (uint64_t) this << ")" << std::endl;
            };

            ~Stream() {
                if (_fd > -1) {
                    close(_fd);

                    if (_role == Role::PRODUCER) {
                        shm_unlink(_path.c_str());
                    }
                }

                std::cout << "Deleted Stream (" << (uint64_t) this << ")" << std::endl;
            }

            bool is_alive() {
                struct stat stat;
                fstat(_fd, &stat);
                if (stat.st_nlink > 0) {
                    return true;
                }
                return false;
            }

            const std::string& name() {
                return _name;
            }

            bool sync() {
                std::lock_guard<Utils::FileReadLock> lock(_stream_data_read_lock);
                if (!is_alive()) {
                    return false;
                }
                bool _sync;
                std::memcpy(&_sync, _data, sizeof(bool));
                return _sync;
            }

            void sync(bool _sync) {
                if (_role == Role::CONSUMER) {
                    throw std::string("Consumer stream can not set stream 'sync' parameter");
                }
                if (!is_alive()) {
                    return;
                }
                std::lock_guard<Utils::FileWriteLock> lock(_stream_data_write_lock);
                std::memcpy(_data, &_sync, sizeof(bool));
            }

            size_t buffer_size() {
                std::lock_guard<Utils::FileReadLock> lock(_stream_data_read_lock);
                if (!is_alive()) {
                    return 0;
                }
                off_t offset = sizeof(bool);
                size_t _buffer_size;
                std::memcpy(&_buffer_size, _data + offset, sizeof(size_t));
                return _buffer_size;
            }

            void buffer_size(size_t _buffer_size) {
                if (_role == Role::CONSUMER) {
                    throw std::string("Consumer stream can not set stream 'buffer_size' parameter");
                }
                std::lock_guard<Utils::FileWriteLock> lock(_stream_data_write_lock);
                if (!is_alive()) {
                    return;
                }
                off_t offset = sizeof(bool);
                std::memcpy(_data + offset, &_buffer_size, sizeof(size_t));
            }

            size_t buffer_count() {
                std::lock_guard<Utils::FileReadLock> lock(_stream_data_read_lock);
                if (!is_alive()) {
                    return 0;
                }
                off_t offset = sizeof(bool) + sizeof(size_t);
                size_t _buffer_count;
                std::memcpy(&_buffer_count, _data + offset, sizeof(size_t));
                return _buffer_count;
            }

            void buffer_count(size_t _buffer_count) {
                if (_role == Role::CONSUMER) {
                    throw std::string("Consumer stream can not set stream 'buffer_count' parameter");
                }
                std::lock_guard<Utils::FileWriteLock> lock(_stream_data_write_lock);
                if (!is_alive()) {
                    return;
                }
                off_t offset = sizeof(bool) + sizeof(size_t);
                std::memcpy(_data + offset, &_buffer_count, sizeof(size_t));
            }

            std::list<BufferState> buffer_states(bool sort=false, uint64_t minimum_timestamp=0) {
                size_t _buffer_count = buffer_count();

                std::list<BufferState> _buffer_states;
                if (!is_alive()) {
                    return _buffer_states;
                }

                {
                    std::lock_guard<Utils::FileReadLock> lock(_buffer_state_read_lock);

                    off_t base_offset = getpagesize();
                    for (size_t i = 0; i < _buffer_count; i++) {
                        off_t iter_offset = i * sizeof(BufferState);
                        BufferState buffer_state;
                        std::memcpy(&buffer_state, _data + base_offset + iter_offset, sizeof(BufferState));

                        if (
                            minimum_timestamp == 0 || buffer_state.data_timestamp >= minimum_timestamp
                        ) {
                            _buffer_states.emplace_back(
                                buffer_state.buffer_id,
                                buffer_state.buffer_size,
                                buffer_state.buffer_count,
                                buffer_state.data_size,
                                buffer_state.data_timestamp,
                                buffer_state.iteration
                            );
                        }

                    }
                }

                if (sort && _buffer_states.size() > 0) {
                    _buffer_states.sort(
                        [] (const BufferState &x, const BufferState &y) { 
                            return (x.data_timestamp < y.data_timestamp);
                        }
                    );
                }

                return _buffer_states;
            }

            void buffer_states(const std::list<BufferState>& _buffer_states) {
                if (_role == Role::CONSUMER) {
                    throw std::string("Consumer stream can not set stream 'buffer_states' parameter");
                }
                std::lock_guard<Utils::FileWriteLock> lock(_buffer_state_write_lock);
                off_t base_offset = getpagesize();
                off_t iter_offset = 0;
                for (auto const& buffer_state : _buffer_states) {
                    std::memcpy(_data + base_offset + iter_offset, &buffer_state, sizeof(BufferState));
                    iter_offset += sizeof(BufferState);
                }
            }

            std::set<Context*> subscribers() {
                std::lock_guard<Utils::FileReadLock> lock(_subscriptions_read_lock);
                std::set<Context*> subscribers;

                if (is_alive()) {
                    off_t base_offset = getpagesize() * 2;
                    for (off_t offset = base_offset; offset < base_offset + getpagesize(); offset += sizeof(Context*)) {
                        Context* context;

                        std::memcpy(&context, _data + offset, sizeof(Context*));
                        if (context != NULL) {
                            subscribers.insert(
                                context
                            );
                        }
                    }
                }

                return subscribers;
            }

            void subscribers(std::set<Context*> subscribers) {
                if (_role == Role::PRODUCER) {
                    throw std::string("Producer stream can not set stream 'subscribers' parameter");
                }

                std::lock_guard<Utils::FileWriteLock> lock(_subscriptions_write_lock);
                if (!is_alive()) {
                    return;
                }

                off_t base_offset = getpagesize() * 2;
                std::memset(_data + base_offset, 0, getpagesize());

                off_t offset = base_offset;
                for (auto const& context : subscribers) {
                    std::memcpy(_data + offset, &context, sizeof(Context*));
                    offset += sizeof(Context*);
                }
            }

            std::set<Context*> pending_acknowledgements() {
                std::lock_guard<Utils::FileReadLock> lock(_acknowledgements_read_lock);

                std::set<Context*> pending;
                
                if (is_alive()) {
                    off_t base_offset = getpagesize() * 3;
                    for (off_t offset = base_offset; offset < base_offset + getpagesize(); offset += sizeof(Context*)) {
                        Context* context;

                        std::memcpy(&context, _data + offset, sizeof(Context*));
                        if (context != NULL) {
                            pending.insert(
                                context
                            );
                        }
                    }
                }

                return pending;
            }

            void pending_acknowledgements(std::set<Context*> pending) {
                std::lock_guard<Utils::FileWriteLock> lock(_acknowledgements_write_lock);
                if (!is_alive()) {
                    return;
                }

                off_t base_offset = getpagesize() * 3;
                std::memset(_data + base_offset, 0, getpagesize());

                off_t offset = base_offset;
                for (auto const& context : pending) {
                    std::memcpy(_data + offset, &context, sizeof(Context*));
                    offset += sizeof(Context*);
                }
            }

        private:
            friend class StreamManager;

            std::string _name;
            Role _role;
            std::string _path;
            int _fd;
            Utils::FileReadLock 
                _stream_data_read_lock, 
                _buffer_state_read_lock, 
                _subscriptions_read_lock, 
                _acknowledgements_read_lock;

            Utils::FileWriteLock 
                _stream_data_write_lock, 
                _buffer_state_write_lock, 
                _subscriptions_write_lock, 
                _acknowledgements_write_lock;

            char* _data;
    };

    class StreamManager {
        public:
            StreamManager(Context* context, BufferManager* buffer_manager) :
                _context(context),
                _buffer_manager(buffer_manager)
            { };

            ~StreamManager() {
                std::map<std::string, Stream*> stream_by_name;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    stream_by_name = _stream_by_name;
                }

                for (auto const& tuple : stream_by_name) {
                    destroy(tuple.second);
                }
            };

            Stream* find(std::string name) {
                std::lock_guard<std::mutex> lock(_mutex); 

                Stream* stream;
                if (_stream_by_name.count(name) > 0) {
                    // if we already know about this stream, return it
                    stream = _stream_by_name[name];
                } else {
                    stream = new Stream(name);
                    _stream_by_name[name] = stream;
                }

                return stream;
            }

            Stream* create(
                std::string name, 
                size_t buffer_size, 
                size_t buffer_count=0, 
                bool sync=false, 
                Stream::Role role=Stream::Role::CONSUMER
            ) {
                std::lock_guard<std::mutex> lock(_mutex); 
                Stream* stream = new Stream(name, buffer_size, buffer_count, sync, role);
                _stream_by_name[name] = stream;

                std::list<Stream::BufferState> buffer_states;
                for (size_t i = 0; i < buffer_count; i++) {
                    Buffer* buffer = _buffer_manager->allocate(
                        name, 
                        Buffer::CREATE_ID, 
                        buffer_size
                    );

                    buffer_states.emplace_back(
                        buffer->id(),
                        buffer_size,
                        buffer_count,
                        0,
                        0,
                        0
                    );
                }

                stream->buffer_states(buffer_states);

                return stream;
            }

            void destroy(Stream* stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                _stream_by_name.erase(stream->name());
                _iteration_by_stream.erase(stream);
                _current_buffer_by_stream.erase(stream);
                delete stream;
            }


            bool is_subscribed(std::string name) {
                Stream* stream = find(name);

                // if we don't know about this stream, then clearly we are
                // not subscribed
                if (stream == NULL) {
                    return false;
                }

                // likewise, if this stream is no longer available, we can't
                // be subscribed.
                // NOTE: do a silent unsubscribe for somehousekeeping
                if (!stream->is_alive()) {
                    unsubscribe(stream);
                    return false;
                }

                for (auto const* subscriber : stream->subscribers()) {
                    if (subscriber == _context) {
                        return true;
                    }
                }

                return false;
            }

            Stream* subscribe(std::string name) {
                Stream* stream = find(name);

                std::lock_guard<std::mutex> lock(_mutex);
                std::set<Context*> subscribers = stream->subscribers();

                for (auto const& subscriber : subscribers) {
                    // check if already subscribed
                    if (subscriber == _context) {
                        return stream;
                    }
                }

                subscribers.insert(_context);

                for (auto const& buffer_state : stream->buffer_states()) {
                    _buffer_manager->allocate(
                        name, 
                        buffer_state.buffer_id, 
                        stream->buffer_size()
                    );
                }

                stream->subscribers(subscribers);

                return stream;
            }

            void unsubscribe(Stream* stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                std::set<Context*> subscribers = stream->subscribers();
                subscribers.erase(_context);
                stream->subscribers(subscribers);

                std::set<Context*> pending = stream->pending_acknowledgements();
                pending.erase(_context);
                stream->pending_acknowledgements(pending);

                _buffer_manager->deallocate_stream(stream->name());
            }

            Stream::BufferState* next_buffer_state(Stream* stream) {
                Buffer* current_buffer;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    current_buffer = _current_buffer_by_stream[stream];
                }

                Stream::BufferState* buffer_state = NULL;
                Buffer* next_buffer;

                if (stream->sync() && stream->pending_acknowledgements().size() > 0) {
                    return NULL;
                }

                // attempt to get a lock (blocking if in sync mode)
                for (size_t _ = 0; _ < stream->buffer_count(); _++) {
                    
                    next_buffer = _buffer_manager->next(stream->name());

                    if (next_buffer != current_buffer) {
                        if (stream->sync()) {
                            // When synchronous, block until we can obtain this buffer's lock...
                            Utils::write_lock(next_buffer->fd());
                        } else {
                            // Otherwise make a nonblocking attempt to get the write lock 
                            if (!Utils::try_write_lock(next_buffer->fd())) {
                                next_buffer = NULL;
                            }
                        }

                        if (next_buffer != NULL) {
                            std::lock_guard<std::mutex> lock(_mutex);
                            _current_buffer_by_stream[stream] = next_buffer;
                            buffer_state = new Stream::BufferState(
                                next_buffer->id(),
                                next_buffer->size(),
                                stream->buffer_count(),
                                0,
                                0,
                                ++_iteration_by_stream[stream]
                            );
                            break;
                        }
                    }
                }

                return buffer_state;
            }

            bool send_buffer_state(Stream* stream, Stream::BufferState* buffer_state) {
                std::lock_guard<std::mutex> lock(_mutex);

                if (stream->sync()) {
                    if (stream->subscribers().size() == 0) {
                        return false;
                    }
                }

                Buffer* buffer = _buffer_manager->find(stream->name(), buffer_state->buffer_id);

                // update the buffer write timestamp
                if (buffer_state->data_timestamp == 0) {
                    buffer_state->data_timestamp = Utils::now();
                } 
                buffer->write_ts(buffer_state->data_timestamp);

                Utils::unlock(buffer->fd());

                if (stream->sync()) {
                    stream->pending_acknowledgements(stream->subscribers());
                }

                std::list<Stream::BufferState> buffer_states = stream->buffer_states();
                for (auto& bs : buffer_states) {
                    if (bs.buffer_id == buffer->id()) {
                        bs.data_size = buffer_state->data_size;
                        bs.data_timestamp = buffer_state->data_timestamp;
                        bs.iteration = buffer_state->iteration;
                    }
                }
                stream->buffer_states(buffer_states);

                delete buffer_state;

                return true;
            }

            Stream::BufferState* receive_buffer_state(Stream* stream, uint64_t minimum_timestamp=1) {
                if (!stream->is_alive()) {
                    throw std::string(
                        "Attempted to receive buffer state on stream that has been terminated"
                    );
                }

                std::list<Stream::BufferState> buffer_states;
                {
                    std::lock_guard<std::mutex> lock(_mutex);
                    buffer_states = stream->buffer_states(true, stream->sync() ? 0 : minimum_timestamp);
                }

                if (buffer_states.size() == 0) {
                    return NULL;
                }

                for (auto const& buffer_state : buffer_states) {
                    {
                        // skip duplicates
                        std::lock_guard<std::mutex> lock(_mutex);
                        if (buffer_state.iteration > _iteration_by_stream[stream]) {
                            _iteration_by_stream[stream] = buffer_state.iteration;
                        } else {
                            continue;
                        }
                    }

                    Buffer* buffer = _buffer_manager->find(stream->name(), buffer_state.buffer_id);
                    if (buffer == NULL) {
                        throw std::string(
                            "Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'"
                        );
                    }
                    
                    if (Utils::try_read_lock(buffer->fd())) {
                        if (stream->sync()) {
                            std::set<Context*> pending = stream->pending_acknowledgements();
                            pending.erase(_context);
                            stream->pending_acknowledgements(pending);
                        }

                        // we got the read lock, so return a copy of buffer state to the caller
                        return new Stream::BufferState(buffer_state);
                    } else {
                        // failed to get the lock
                    }
                }

                // if we made it here, we were unable to read any of the last returned
                // buffer states, so return null
                return NULL;
            }

            void release_buffer_state(Stream* stream, Stream::BufferState* buffer_state) {
                std::lock_guard<std::mutex> lock(_mutex);
             
                Buffer* buffer = _buffer_manager->find(stream->name(), buffer_state->buffer_id);
                Utils::unlock(buffer->fd());

                delete buffer_state;

            }

            size_t subscriber_count(Stream* stream) {
                std::lock_guard<std::mutex> lock(_mutex); 
                return stream->subscribers().size();
            }

        private:
            Context* _context;
            BufferManager* _buffer_manager;
            std::map<std::string, Stream*> _stream_by_name;
            std::map<Stream*, std::list<Buffer*>> _buffers_by_stream;
            std::map<Stream*, Buffer*> _current_buffer_by_stream;
            std::map<Stream*, uint64_t> _iteration_by_stream;
            std::mutex _mutex;

    };
};

#endif