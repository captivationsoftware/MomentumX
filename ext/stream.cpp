#include "stream.h"

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <list>
#include <map>
#include <mutex>
#include <set>
#include <tuple>
#include <type_traits>

#include "buffer.h"
#include "context.h"
#include "utils.h"

namespace MomentumX {

    struct PendingAcknowledgement { 
        size_t buffer_id = 0;
        Context* context = nullptr;

        PendingAcknowledgement() = default;
        
        PendingAcknowledgement(size_t _buffer_id, Context* _context) :
            buffer_id(_buffer_id),
            context(_context)
        { }
    };

    struct ControlBlock {
        static constexpr size_t MAX_BUFFERS = 512;
        static constexpr size_t MAX_SUBSCRIPTIONS = 256;

        bool sync{};
        size_t buffer_size{};
        size_t buffer_count{};
        Utils::StaticVector<Stream::BufferState, MAX_BUFFERS> buffers{};
        Utils::StaticVector<Context*, MAX_SUBSCRIPTIONS> subscribers{};
        Utils::StaticVector<PendingAcknowledgement, MAX_SUBSCRIPTIONS * MAX_BUFFERS> pending_acknowledgements{};
    };

    static_assert(std::is_trivially_copy_constructible<ControlBlock>::value, "needed for std::memcpy");

    Stream::Stream(const Utils::PathConfig& paths, size_t buffer_size, size_t buffer_count, bool sync, Stream::Role role)
        : _paths(paths), _role(role), _fd(open(_paths.stream_path.c_str(), O_RDWR | (_role == Role::PRODUCER ? O_CREAT | O_EXCL : 0), S_IRWXU)) {
        if (_fd < 0) {
            if (role == Role::CONSUMER) {
                throw std::runtime_error("Failed to open shared memory stream file '" + _paths.stream_name + "' [errno: " + std::to_string(errno) + "]");
            } else {
                throw std::runtime_error("Failed to create shared memory stream file '" + _paths.stream_name + "' [errno: " + std::to_string(errno) + "]");
            }
        }

        const size_t size_required = Utils::page_aligned_size(sizeof(ControlBlock));

        if (role == Role::PRODUCER) {
            ftruncate(_fd, size_required);
        }

        _data = (char*)mmap(NULL, size_required, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
        if (_data == MAP_FAILED) {
            throw std::runtime_error("Failed to mmap shared memory stream file [errno: " + std::to_string(errno) + "]");
        }

        _control = reinterpret_cast<ControlBlock*>(_data);
        if (_role == PRODUCER) {
            *_control = ControlBlock{};
            _control->sync = sync;
            _control->buffer_size = buffer_size;
            _control->buffer_count = buffer_count;
        }

        if (_role == PRODUCER) {
            Utils::Logger::get_logger().info(std::string("Created Stream (" + std::to_string((uint64_t)this) + ")"));
        } else {
            Utils::Logger::get_logger().info(std::string("Opened Stream (" + std::to_string((uint64_t)this) + ")"));
        }
    };

    Stream::~Stream() {
        if (_fd > -1) {
            close(_fd);

            if (_role == Role::PRODUCER) {
                int return_val = std::remove(_paths.stream_path.c_str());
                if (return_val != 0) {
                    std::stringstream ss;
                    ss << "Unable to delete stream file \"" << _paths.stream_path << "\" with error: " << return_val;
                    Utils::Logger::get_logger().error(ss.str());
                }
            }
        }

        if (_role == PRODUCER) {
            Utils::Logger::get_logger().info(std::string("Deleted Stream (" + std::to_string((uint64_t)this) + ")"));
        } else {
            Utils::Logger::get_logger().info(std::string("Closed Stream (" + std::to_string((uint64_t)this) + ")"));
        }
    }

    bool Stream::is_alive() {
        struct stat stat;
        fstat(_fd, &stat);
        if (stat.st_nlink > 0) {
            return true;
        }
        return false;
    }

    const std::string& Stream::name() {
        return _paths.stream_name;
    }

    Utils::PathConfig Stream::paths() {
        return _paths;
    }

    int Stream::fd() {
        return _fd;
    }

    bool Stream::sync() {
        if (!is_alive()) {
            return false;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        return _control->sync;
    }

    size_t Stream::buffer_size() {
        if (!is_alive()) {
            return 0;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        return _control->buffer_size;
    }

    size_t Stream::buffer_count() {
        if (!is_alive()) {
            return 0;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        return _control->buffer_count;
    }

    std::list<Stream::BufferState> Stream::buffer_states(bool sort, uint64_t minimum_timestamp) {
        if (!is_alive()) {
            return {};
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        std::list<Stream::BufferState> buffer_states(_control->buffers.begin(), _control->buffers.end());
        if (sort) {
            const auto comparator = [](const BufferState& x, const BufferState& y) { return (x.data_timestamp < y.data_timestamp); };
            buffer_states.sort(comparator);
        }

        return buffer_states;
    }

    void Stream::update_buffer_state(Stream::BufferState* buffer_state) {
        if (_role == Role::CONSUMER) {
            throw std::runtime_error("Consumer stream can not update stream buffer states");
        }

        if (!is_alive()) {
            return;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedWriteLock lock(_fd);
        const auto beg = _control->buffers.begin();
        const auto end = _control->buffers.end();
        const auto pred = [&](const BufferState& bs) { return bs.buffer_id == buffer_state->buffer_id; };
        const auto loc = std::find_if(beg, end, pred);

        if (loc == end) {
            _control->buffers.push_back(*buffer_state);
        } else {
            *loc = *buffer_state;
        }
    }

    std::set<Context*> Stream::subscribers() {
        if (!is_alive()) {
            return {};
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        const auto beg = _control->subscribers.begin();
        const auto end = _control->subscribers.end();
        return std::set<Context*>(beg, end);
    }

    void Stream::add_subscriber(Context* context) {
        if (_role == Role::PRODUCER) {
            throw std::runtime_error("Producer stream cannot add subscribers");
        }

        if (!is_alive()) {
            return;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedWriteLock lock(_fd);
        try {
            _control->subscribers.push_back(context);
        } catch (std::exception& e) {
            // re-throw for a more user-friendly error message
            throw std::runtime_error(
                "Subscriber count exceeds the allowable "
                "amount for this stream");
        }
    }

    void Stream::remove_subscriber(Context* context) {
        if (_role == Role::PRODUCER) {
            throw std::runtime_error("Producer stream cannot remove subscribers");
        }

        if (!is_alive()) {
            return;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedWriteLock lock(_fd);
        const auto beg = _control->subscribers.begin();
        const auto end = _control->subscribers.end();
        const auto loc = std::find(beg, end, context);

        if (loc != end) {
            _control->subscribers.erase(loc);
        }
    }

    bool Stream::has_pending_acknowledgements(size_t buffer_id) {
        if (!is_alive()) {
            return {};
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedReadLock lock(_fd);
        const auto beg = _control->pending_acknowledgements.begin();
        const auto end = _control->pending_acknowledgements.end();
        const auto loc = std::find_if(beg, end, [&](const PendingAcknowledgement& pa) { return pa.buffer_id == buffer_id; });
        
        return loc != end;
    }

    void Stream::set_pending_acknowledgements(size_t buffer_id) {
        if (!is_alive()) {
            return;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedWriteLock lock(_fd);
        for (auto const subscriber : _control->subscribers) {
            _control->pending_acknowledgements.push_back(MomentumX::PendingAcknowledgement(buffer_id, subscriber));
        } 

        Utils::Logger::get_logger().debug(std::string("Reset pending acknowledgements to mirror current subscribers for buffer id " + buffer_id));
    }

    void Stream::remove_all_pending_acknowledgements(Context* context) {
        while(is_alive()) {
            std::lock_guard<std::mutex> thread_lock(_mutex);
            Utils::ScopedWriteLock lock(_fd);
            auto const beg = _control->pending_acknowledgements.begin();
            auto const end = _control->pending_acknowledgements.end();
            auto const loc = std::find_if(
                beg, 
                end,
                [&](const PendingAcknowledgement& pa) -> bool { return pa.context == context; }
            );

            if (loc != end) {
                _control->pending_acknowledgements.erase(loc);
            } else {
                return;
            }
        }
    }

    void Stream::remove_pending_acknowledgement(size_t buffer_id, Context* context) {
        if (!is_alive()) {
            return;
        }

        std::lock_guard<std::mutex> thread_lock(_mutex);
        Utils::ScopedWriteLock lock(_fd);
        const auto beg = _control->pending_acknowledgements.begin();
        const auto end = _control->pending_acknowledgements.end();
        const auto loc = std::find_if(
            beg, 
            end, 
            [&](const PendingAcknowledgement& pa) -> bool { return pa.buffer_id == buffer_id && pa.context == context; }
        ); 

        if (loc != end) {
            _control->pending_acknowledgements.erase(loc);
        } else {
            Utils::Logger::get_logger().warning(
                std::string("Attempted to remove pending acknowledgement that does not exist for buffer: ") + std::to_string(buffer_id)
            );
        }
    }

    StreamManager::StreamManager(Context* context, BufferManager* buffer_manager) : _context(context), _buffer_manager(buffer_manager){};

    StreamManager::~StreamManager() {
        std::map<std::string, Stream*> stream_by_name;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            stream_by_name = _stream_by_name;
        }

        for (auto const& tuple : stream_by_name) {
            destroy(tuple.second);
        }
    };

    Stream* StreamManager::find(std::string name) {
        std::lock_guard<std::mutex> lock(_mutex);

        Stream* stream;
        if (_stream_by_name.count(name) > 0) {
            // if we already know about this stream, return it
            stream = _stream_by_name[name];
        } else {
            Utils::PathConfig paths(_context->context_path(), name);
            stream = new Stream(paths);
            _stream_by_name[name] = stream;
        }

        return stream;
    }

    Stream* StreamManager::create(std::string name, size_t buffer_size, size_t buffer_count, bool sync, Stream::Role role) {
        std::lock_guard<std::mutex> lock(_mutex);
        const Utils::PathConfig paths(_context->context_path(), name);

        Stream* stream = new Stream(paths, buffer_size, buffer_count, sync, role);
        _stream_by_name[name] = stream;

        for (size_t i = 1; i <= buffer_count; i++) {
            Buffer* buffer = _buffer_manager->allocate(paths, i, buffer_size, true);

            Stream::BufferState* buffer_state = new Stream::BufferState(buffer->id(), buffer_size, buffer_count, 0, 0, 0);

            stream->update_buffer_state(buffer_state);
            delete buffer_state;
        }

        return stream;
    }

    void StreamManager::destroy(Stream* stream) {
        std::lock_guard<std::mutex> lock(_mutex);
        _stream_by_name.erase(stream->name());
        _iteration_by_stream.erase(stream);
        _current_buffer_by_stream.erase(stream);
        delete stream;
    }

    bool StreamManager::is_subscribed(std::string name) {
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

        Utils::Logger::get_logger().debug(std::string("Not subscribed to stream: " + std::to_string((uint64_t)_context)));
        return false;
    }

    Stream* StreamManager::subscribe(std::string name) {
        Stream* stream = find(name);

        // short circuit if we're already subscribed
        if (is_subscribed(name)) {
            return stream;
        }

        std::lock_guard<std::mutex> lock(_mutex);

        stream->add_subscriber(_context);

        for (auto const& buffer_state : stream->buffer_states()) {
            _buffer_manager->allocate(stream->paths(), buffer_state.buffer_id, stream->buffer_size());
        }

        return stream;
    }

    void StreamManager::unsubscribe(Stream* stream) {
        std::lock_guard<std::mutex> lock(_mutex);
        stream->remove_all_pending_acknowledgements(_context);
        stream->remove_subscriber(_context);

        _buffer_manager->deallocate_stream(stream->paths());
    }

    Stream::BufferState* StreamManager::next_buffer_state(Stream* stream) {
        Buffer* current_buffer;
        {
            std::lock_guard<std::mutex> lock(_mutex);
            current_buffer = _current_buffer_by_stream[stream];
        }

        Stream::BufferState* buffer_state = nullptr;
        Buffer* next_buffer;

        if (stream->sync()) {
            next_buffer = _buffer_manager->peek_next(stream->paths()); // peek

            if (stream->has_pending_acknowledgements(next_buffer->id())) {
                return nullptr;
            }
            
            next_buffer = _buffer_manager->next(stream->paths()); // actually rotate the buffers

            // When synchronous, block until we can obtain this buffer's
            // lock...
            Utils::write_lock(next_buffer->fd());

            for (auto const& x : stream->_control->pending_acknowledgements) {
                std::cout << "Still waiting for " << x.buffer_id << " and context " << x.context << " to clear" <<std::endl;
            }

        } else {
            for (size_t _ = 0; _ < stream->buffer_count(); _++) {
                next_buffer = _buffer_manager->next(stream->paths());

                // ensure that we don't write to the same buffer repeatedly
                if (next_buffer == current_buffer) {
                    continue;
                }

                if (!Utils::try_write_lock(next_buffer->fd())) {
                    next_buffer = nullptr;
                }

                if (next_buffer != nullptr) {
                    break;
                }
            }
        }
        
        std::lock_guard<std::mutex> lock(_mutex);
        _current_buffer_by_stream[stream] = next_buffer;

        // create a new buffer state pointer and break out to the
        // caller
        buffer_state =
            new Stream::BufferState(next_buffer->id(), next_buffer->size(), stream->buffer_count(), 0, 0, ++_iteration_by_stream[stream]);


        return buffer_state;
    }

    bool StreamManager::send_buffer_state(Stream* stream, Stream::BufferState* buffer_state) {
        std::lock_guard<std::mutex> lock(_mutex);

        if (stream->sync()) {
            if (stream->subscribers().size() == 0) {
                return false;
            }
        }

        if (buffer_state->data_size == 0) {
            Utils::Logger::get_logger().warning("Sending buffer state without having set data_size property");
        }

        Buffer* buffer = _buffer_manager->find(stream->paths(), buffer_state->buffer_id);

        // update the buffer write timestamp
        if (buffer_state->data_timestamp == 0) {
            buffer_state->data_timestamp = Utils::now();
        }
        buffer->write_ts(buffer_state->data_timestamp);

        if (stream->sync()) {
            stream->set_pending_acknowledgements(buffer->id());
        }

        stream->update_buffer_state(buffer_state);

        Utils::unlock(buffer->fd());

        delete buffer_state;

        return true;
    }

    Stream::BufferState* StreamManager::receive_buffer_state(Stream* stream, uint64_t minimum_timestamp) {
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

            Buffer* buffer = _buffer_manager->find(stream->paths(), buffer_state.buffer_id);
            if (buffer == NULL) {
                throw std::runtime_error("Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'");
            }

            if (Utils::try_read_lock(buffer->fd())) {
                uint64_t last_data_timestamp;
                Utils::get_timestamps(buffer->fd(), NULL, &last_data_timestamp);

                if (buffer_state.data_timestamp == last_data_timestamp) {
                    // we got the read lock and data matches, so return a copy
                    // of buffer state to the caller
                    return new Stream::BufferState(buffer_state);
                }
            } else {
                // failed to get the lock
            }
        }

        // weren't able to read any messages - is it because the stream is
        // terminated?
        if (!stream->is_alive()) {
            // yes it was, so alert the caller
            throw std::runtime_error(
                "Attempted to receive buffer state on stream "
                "that has been terminated");
        }

        // if we made it here, we were unable to read any of the last returned
        // buffer states, but the stream IS alive, so return null
        return NULL;
    }

    Stream::BufferState* StreamManager::get_by_buffer_id(Stream* stream, uint16_t buffer_id) {
        Buffer* buffer = _buffer_manager->find(stream->paths(), buffer_id);
        if (buffer == NULL) {
            throw std::runtime_error("Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'");
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
                        // we verified that buffer state mirrors the underlying
                        // file, so return
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

    void StreamManager::flush_buffer_state(Stream* stream) {
        if (stream->sync()) {
        Utils::Logger::get_logger().warning("Calling flush on a stream in sync mode is a no-op");
        } else {
            std::lock_guard<std::mutex> lock(_mutex);
            _iteration_by_stream[stream] = stream->buffer_states(true).back().iteration;
        }
    }

    void StreamManager::release_buffer_state(Stream* stream, Stream::BufferState* buffer_state) {
        std::lock_guard<std::mutex> lock(_mutex);

        if (stream->sync()) {
            stream->remove_pending_acknowledgement(buffer_state->buffer_id, _context);
        }

        Buffer* buffer = _buffer_manager->find(stream->paths(), buffer_state->buffer_id);
        Utils::unlock(buffer->fd());

        delete buffer_state;
    }

    size_t subscriber_count(Stream* stream) {
        return stream->subscribers().size();
    }

}  // namespace MomentumX