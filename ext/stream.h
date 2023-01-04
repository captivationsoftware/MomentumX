#ifndef MOMENTUMX_STREAM_H
#define MOMENTUMX_STREAM_H

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

namespace MomentumX {

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

            Stream(
                std::string name, 
                size_t buffer_size=0, 
                size_t buffer_count=0, 
                bool sync=false, 
                Role role=Role::CONSUMER
            ) :
                _name(name),
                _role(role),
                _path("mx." + name),
                _fd(shm_open(_path.c_str(), O_RDWR | (_role == Role::PRODUCER ? O_CREAT : 0), S_IRWXU)),
                
                _stream_data_size(sizeof(bool) + sizeof(size_t) + sizeof(size_t)),
                _buffer_state_size(512 * sizeof(BufferState)),
                _subscribers_size(256 * sizeof(Context*)),
                _acknowledgements_size(_subscribers_size),

                _stream_data_offset(0),
                _buffer_state_offset(_stream_data_offset + _stream_data_size),
                _subscribers_offset(_buffer_state_offset + _buffer_state_size),
                _acknowledgements_offset(_subscribers_offset + _subscribers_size)
            {

                if (_fd < 0) {
                    if (role == Role::CONSUMER) {
                        throw std::runtime_error("Failed to open shared memory stream file '"+name+"' [errno: " + std::to_string(errno) + "]");
                    } else {
                        throw std::runtime_error("Failed to create shared memory stream file '"+name+"' [errno: " + std::to_string(errno) + "]");
                    }
                } 

                size_t size_required = Utils::page_aligned_size(
                    _stream_data_size + 
                    _buffer_state_size + 
                    _subscribers_size + 
                    _acknowledgements_size
                );

                if (role == Role::PRODUCER) {
                    ftruncate(_fd, size_required);
                }

                _data = (char* ) mmap(NULL, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
                if (_data == MAP_FAILED) {
                    throw std::runtime_error("Failed to mmap shared memory stream file [errno: " + std::to_string(errno) + "]");
                }

                if (_role == PRODUCER) {
                    // initialize to 0s
                    memset(_data, 0, size_required);
                    
                    this->buffer_size(buffer_size);
                    this->buffer_count(buffer_count);
                    this->sync(sync);
                }

                if (_role == PRODUCER) {
                    Utils::Logger::get_logger().info(
                        std::string("Created Stream (" + std::to_string((uint64_t) this) + ")")          
                    );
                } else {
                    Utils::Logger::get_logger().info(
                        std::string("Opened Stream (" + std::to_string((uint64_t) this) + ")")          
                    );
                }
            };

            ~Stream() {
                if (_fd > -1) {
                    close(_fd);

                    if (_role == Role::PRODUCER) {
                        shm_unlink(_path.c_str());
                    }
                }

                if (_role == PRODUCER) {
                    Utils::Logger::get_logger().info(
                        std::string("Deleted Stream (" + std::to_string((uint64_t) this) + ")")          
                    );
                } else {
                    Utils::Logger::get_logger().info(
                        std::string("Closed Stream (" + std::to_string((uint64_t) this) + ")")          
                    );
                }
            
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

            int fd() { 
                return _fd;
            }

            bool sync() {
                if (!is_alive()) {
                    return false;
                }
               
                bool _sync;

                Utils::with_read_lock(_fd, [&] {
                    std::memcpy(&_sync, _data, sizeof(bool));
                });

                return _sync;
            }

            void sync(bool _sync) {
                if (_role == Role::CONSUMER) {
                    throw std::runtime_error("Consumer stream can not set stream 'sync' parameter");
                }

                if (!is_alive()) {
                    return;
                }
                
                Utils::with_write_lock(_fd, [&] {
                    std::memcpy(_data, &_sync, sizeof(bool));
                });
            }

            size_t buffer_size() {
                if (!is_alive()) {
                    return 0;
                }

                off_t offset = sizeof(bool);
                size_t _buffer_size;

                Utils::with_read_lock(_fd, [&] {
                    std::memcpy(&_buffer_size, _data + offset, sizeof(size_t));
                });

                return _buffer_size;
            }

            void buffer_size(size_t _buffer_size) {
                if (_role == Role::CONSUMER) {
                    throw std::runtime_error("Consumer stream can not set stream 'buffer_size' parameter");
                }

                if (!is_alive()) {
                    return;
                }

                off_t offset = sizeof(bool);
                Utils::with_write_lock(_fd, [&] {
                    std::memcpy(_data + offset, &_buffer_size, sizeof(size_t));
                });
            }

            size_t buffer_count() {
                if (!is_alive()) {
                    return 0;
                }
                
                off_t offset = sizeof(bool) + sizeof(size_t);
                size_t _buffer_count;
                Utils::with_read_lock(_fd, [&] {
                    std::memcpy(&_buffer_count, _data + offset, sizeof(size_t));
                });

                return _buffer_count;
            }

            void buffer_count(size_t _buffer_count) {
                if (_role == Role::CONSUMER) {
                    throw std::runtime_error("Consumer stream can not set stream 'buffer_count' parameter");
                }
                
                if (!is_alive()) {
                    return;
                }

                off_t offset = sizeof(bool) + sizeof(size_t);
                Utils::with_write_lock(_fd, [&] {
                    std::memcpy(_data + offset, &_buffer_count, sizeof(size_t));
                });
            }

            std::list<BufferState> buffer_states(bool sort=false, uint64_t minimum_timestamp=0) {
                size_t _buffer_count = buffer_count();
                std::list<BufferState> _buffer_states;
                
                if (!is_alive()) {
                    return _buffer_states;
                }

                Utils::with_read_lock(_fd, [&] {

                    for (size_t i = 0; i < _buffer_count; i++) {
                        off_t iter_offset = i * sizeof(BufferState);
                        BufferState buffer_state;
                        std::memcpy(&buffer_state, _data + _buffer_state_offset + iter_offset, sizeof(BufferState));

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
                });
                
                if (sort && _buffer_states.size() > 0) {
                    _buffer_states.sort(
                        [] (const BufferState &x, const BufferState &y) { 
                            return (x.data_timestamp < y.data_timestamp);
                        }
                    );
                }

                return _buffer_states;
            }

            void update_buffer_state(BufferState* buffer_state) {
                if (_role == Role::CONSUMER) {
                    throw std::runtime_error("Consumer stream can not update stream buffer states");
                }

                if (!is_alive()) {
                    return;
                }

                Utils::with_write_lock(_fd, [&] {
                    size_t offset;
                    for (offset = _buffer_state_offset; offset < _buffer_state_offset + _buffer_state_size; offset += sizeof(BufferState)) {
                        BufferState bs;
                        std::memcpy(&bs, _data + offset, sizeof(BufferState));

                        if (bs.buffer_id == 0) break;

                        if (bs.buffer_id == buffer_state->buffer_id) {
                            std::memcpy(_data + offset, buffer_state, sizeof(BufferState));
                            return;
                        }
                    }

                    // if we made it here, its a new buffer so we need to add it
                    std::memcpy(_data + offset, buffer_state, sizeof(BufferState));
                });
            }

            std::set<Context*> subscribers() {
                std::set<Context*> _subscribers;

                if (is_alive()) {
                    Utils::with_read_lock(_fd, [&] {
                        for (
                            size_t offset = _subscribers_offset; 
                            offset < _subscribers_offset + _subscribers_size; 
                            offset += sizeof(Context*)
                        ) {
                            Context* context;

                            std::memcpy(&context, _data + offset, sizeof(Context*));
                            if (context != NULL) {
                                _subscribers.insert(
                                    context
                                );
                            } else {
                                break;
                            }
                        }
                    });
                }

                return _subscribers;
            }

            void add_subscriber(Context* context) {
                if (_role == Role::PRODUCER) {
                    throw std::runtime_error("Producer stream cannot add subscribers");
                }

                if (!is_alive()) {
                    return;
                }

                bool added = false;
                Utils::with_write_lock(_fd, [&] {
                    for (size_t offset = _subscribers_offset; offset < _subscribers_offset + _subscribers_size; offset += sizeof(Context*)) {
                        Context* subscriber;
                        std::memcpy(&subscriber, _data + offset, sizeof(Context*));

                        if (subscriber == NULL) {
                            std::memcpy(_data + offset, &context, sizeof(Context*));
                            Utils::Logger::get_logger().debug(std::string("Added subscriber: " + std::to_string((uint64_t) context)));
                            added = true;
                            break;
                        }
                    }
                });

                if (!added) {
                    // if we made it here, there was no slot found for this subscriber
                    throw std::runtime_error("Subscriber count exceeds the allowable amount for this stream");
                }
            }


            void remove_subscriber(Context* context) {
                if (_role == Role::PRODUCER) {
                    throw std::runtime_error("Producer stream cannot remove subscribers");
                }

                if (!is_alive()) {
                    return;
                }

                Utils::with_write_lock(_fd, [&] {
                    bool shift = false;
                    for (size_t offset = _subscribers_offset; offset < _subscribers_offset + _subscribers_size; offset += sizeof(Context*)) {
                        Context* subscriber;
                        std::memcpy(&subscriber, _data + offset, sizeof(Context*));
                        
                        if (subscriber == context) {
                            std::memset(_data + offset, 0, sizeof(Context*));
                            Utils::Logger::get_logger().debug(std::string("Removed subscriber: " + std::to_string((uint64_t) context)));
                            shift = true;
                        }

                        if (shift && _data + offset + sizeof(Context*) < _data + _subscribers_offset + _subscribers_size) {
                            std::memmove(_data + offset, _data + offset + sizeof(Context*), sizeof(Context*));
                        }
                    }
                });

            }

            std::set<Context*> pending_acknowledgements() {
                std::set<Context*> pending;
                
                if (is_alive()) {
                    Utils::with_read_lock(_fd, [&] {
                        for (
                            size_t offset = _acknowledgements_offset; 
                            offset < _acknowledgements_offset + _acknowledgements_size; 
                            offset += sizeof(Context*)
                        ) {
                            Context* context;

                            std::memcpy(&context, _data + offset, sizeof(Context*));
                            if (context != NULL) {
                                pending.insert(
                                    context
                                );
                            }
                        }
                    });
                }

                return pending;
            }

            void set_pending_acknowledgements() {
                if (!is_alive()) {
                    return;
                }

                Utils::with_write_lock(_fd, [&] {
                    std::memcpy(_data + _acknowledgements_offset, _data + _subscribers_offset, _subscribers_size);
                });

                Utils::Logger::get_logger().debug(std::string("Reset pending acknowledgements to mirror current subscribers"));
            }

            void remove_pending_acknowledgement(Context* context) {
                if (!is_alive()) {
                    return;
                }

                Utils::with_write_lock(_fd, [&] {
                    bool shift = true;
                    for (size_t offset = _acknowledgements_offset; offset < _acknowledgements_offset + _acknowledgements_size; offset += sizeof(Context*)) {
                        Context* subscriber;
                        std::memcpy(&subscriber, _data + offset, sizeof(Context*));

                        if (subscriber == context) {
                            std::memset(_data + offset, 0, sizeof(Context*));
                            Utils::Logger::get_logger().debug(std::string("Removed pending acknowledgement: " + std::to_string((uint64_t) context)));
                            shift = true;
                        }

                        if (shift && _data + offset + sizeof(Context*) < _data + _acknowledgements_offset + _acknowledgements_size) {
                            std::memmove(_data + offset, _data + offset + sizeof(Context*), sizeof(Context*));
                        }
                    }
                });
            }


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

                for (size_t i = 1; i <= buffer_count; i++) {
                    Buffer* buffer = _buffer_manager->allocate(
                        name, 
                        i, 
                        buffer_size,
                        true
                    );

                    Stream::BufferState* buffer_state = new Stream::BufferState(
                        buffer->id(),
                        buffer_size,
                        buffer_count,
                        0,
                        0,
                        0
                    );

                    stream->update_buffer_state(buffer_state);
                    delete buffer_state;
                }


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

                for (auto const& subscriber : stream->subscribers()) {
                    if (subscriber == _context) {
                        return true;
                    }
                }
                
                Utils::Logger::get_logger().debug(std::string("Not subscribed to stream: " + std::to_string((uint64_t) _context)));
                return false;
            }

            Stream* subscribe(std::string name) {
                Stream* stream = find(name);

                // short circuit if we're already subscribed                    
                if (is_subscribed(name)) {
                    return stream;
                }

                std::lock_guard<std::mutex> lock(_mutex);

                stream->add_subscriber(_context);

                for (auto const& buffer_state : stream->buffer_states()) {
                    _buffer_manager->allocate(
                        name, 
                        buffer_state.buffer_id, 
                        stream->buffer_size()
                    );
                }


                return stream;
            }

            void unsubscribe(Stream* stream) {
                std::lock_guard<std::mutex> lock(_mutex);
                stream->remove_pending_acknowledgement(_context);
                stream->remove_subscriber(_context);

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

                if (buffer_state->data_size == 0) {
                    Utils::Logger::get_logger().warning("Sending buffer state without having set data_size property");
                }

                Buffer* buffer = _buffer_manager->find(stream->name(), buffer_state->buffer_id);

                // update the buffer write timestamp
                if (buffer_state->data_timestamp == 0) {
                    buffer_state->data_timestamp = Utils::now();
                } 
                buffer->write_ts(buffer_state->data_timestamp);

                Utils::unlock(buffer->fd());

                if (stream->sync()) {
                    stream->set_pending_acknowledgements();
                }

                stream->update_buffer_state(buffer_state);

                delete buffer_state;

                return true;
            }

            Stream::BufferState* receive_buffer_state(Stream* stream, uint64_t minimum_timestamp=1) {
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
                        throw std::runtime_error(
                            "Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'"
                        );
                    }
                    
                    if (Utils::try_read_lock(buffer->fd())) {
                        uint64_t last_data_timestamp;
                        Utils::get_timestamps(buffer->fd(), NULL, &last_data_timestamp);
                        
                        if (buffer_state.data_timestamp == last_data_timestamp) {
                            // we got the read lock and data matches, so return a copy of buffer state to the caller
                            return new Stream::BufferState(buffer_state);
                        }
                    } else {
                        // failed to get the lock
                    }
                }

                // weren't able to read any messages - is it because the stream is terminated?
                if (!stream->is_alive()) {
                    // yes it was, so alert the caller
                    throw std::runtime_error(
                        "Attempted to receive buffer state on stream that has been terminated"
                    );
                }

                // if we made it here, we were unable to read any of the last returned
                // buffer states, but the stream IS alive, so return null
                return NULL;
            }

            Stream::BufferState* get_by_buffer_id(Stream* stream, uint16_t buffer_id) {

                Buffer* buffer = _buffer_manager->find(stream->name(), buffer_id);
                if (buffer == NULL) {
                    throw std::runtime_error(
                        "Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'"
                    );
                }

                if (Utils::try_read_lock(buffer->fd())) {
                    uint64_t last_data_timestamp;
                    Utils::get_timestamps(buffer->fd(), NULL, &last_data_timestamp);

                    std::list<Stream::BufferState> buffer_states;
                    {
                        std::lock_guard<std::mutex> lock(_mutex);
                        buffer_states = stream->buffer_states();
                    }

                    for (auto const& buffer_state : buffer_states) {
                        if (buffer_state.buffer_id == buffer_id) {
                            if (last_data_timestamp == buffer_state.data_timestamp) {
                                // we verified that buffer state mirrors the underlying file, so return
                                return new Stream::BufferState(buffer_state);
                            } else {
                                // data was expired, so return null
                                break;
                            }
                        }
                    }

                }

                return NULL;

            }

            void flush_buffer_state(Stream* stream) {
                if (stream->sync()) {
                    Utils::Logger::get_logger().warning("Calling flush on a stream in sync mode is a no-op");
                } else {
                    std::lock_guard<std::mutex> lock(_mutex);
                    _iteration_by_stream[stream] = stream->buffer_states(true).back().iteration; 
                }

            }

            void release_buffer_state(Stream* stream, Stream::BufferState* buffer_state) {
                std::lock_guard<std::mutex> lock(_mutex);
             
                if (stream->sync()) {
                    stream->remove_pending_acknowledgement(_context);
                }

                Buffer* buffer = _buffer_manager->find(stream->name(), buffer_state->buffer_id);
                Utils::unlock(buffer->fd());


                delete buffer_state;

            }

            size_t subscriber_count(Stream* stream) {
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