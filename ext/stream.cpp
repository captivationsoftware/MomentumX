#include "stream.h"

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <boost/interprocess/creation_tags.hpp>
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
#include "control.h"
#include "utils.h"

namespace MomentumX {

    static_assert(std::is_trivially_copy_constructible<ControlBlock>::value, "needed for std::memcpy");

    Stream::Stream(const Utils::PathConfig& paths, size_t buffer_size, size_t buffer_count, bool sync, Stream::Role role)
        : _paths(paths),
          _role(role),
          _fd(open(_paths.stream_path.c_str(), O_RDWR | (_role == Role::PRODUCER ? O_CREAT | O_EXCL : 0), S_IRWXU)),
          _data(nullptr),
          _control(nullptr),
          _control_mutex() {
        if (_fd < 0) {
            if (role == Role::CONSUMER) {
                throw std::runtime_error("Failed to open shared memory stream file '" + _paths.stream_name + "' [errno: " + std::to_string(errno) + "]");
            } else {
                throw std::runtime_error("Failed to create shared memory stream file '" + _paths.stream_name + "' [errno: " + std::to_string(errno) + "]");
            }
        }

        const size_t size_required = Utils::page_aligned_size(sizeof(ControlBlock));

        if (role == Role::PRODUCER) {
            const int ft_rc = ftruncate(_fd, size_required);
            if (ft_rc) {
                Utils::Logger::get_logger().warning("Failed to resize file with error code: " + std::to_string(ft_rc));
            }
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
            _control_mutex.emplace(bip::create_only, paths.stream_mutex.c_str());
            Utils::Logger::get_logger().info(std::string("Created Producer Stream (" + std::to_string((uint64_t)this) + ")"));
        } else {
            _control_mutex.emplace(bip::open_only, paths.stream_mutex.c_str());
            Utils::Logger::get_logger().info(std::string("Created Consumer Stream (" + std::to_string((uint64_t)this) + ")"));
        }

        // Initialize any cached variables that will remain for the duration of the stream to prevent extraneous locking...
        _sync = _control->sync;
    };

    Stream::~Stream() {
        const auto control_lock = get_control_lock();
        if (_role == Role::PRODUCER) {
            Utils::Logger::get_logger().info("Producer stream signalling end of stream");
            end(control_lock);
        }

        if (_fd > -1) {
            ::close(_fd);

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
            _control_mutex->remove(_paths.stream_mutex.c_str());
            Utils::Logger::get_logger().info(std::string("Destroyed Producer Stream (" + std::to_string((uint64_t)this) + ")"));
        } else {
            Utils::Logger::get_logger().info(std::string("Destroyed Consumer Stream (" + std::to_string((uint64_t)this) + ")"));
        }
    }

    const std::string& Stream::name(const Utils::OmniWriteLock& control_lock) {
        return _paths.stream_name;
    }

    Utils::PathConfig Stream::paths(const Utils::OmniWriteLock& control_lock) {
        return _paths;
    }

    int Stream::fd(const Utils::OmniWriteLock& control_lock) {
        return _fd;
    }

    bool Stream::sync(const Utils::OmniWriteLock& control_lock) {
        return _control->sync;
    }

    size_t Stream::buffer_size(const Utils::OmniWriteLock& control_lock) {
        return _control->buffer_size;
    }

    size_t Stream::buffer_count(const Utils::OmniWriteLock& control_lock) {
        return _control->buffer_count;
    }

    bool Stream::is_ended(const Utils::OmniWriteLock& control_lock) {
        // First, check to see if the producer is still producing (i.e. is_ended != true)
        return _control->is_ended;
    }

    void Stream::end(const Utils::OmniWriteLock& control_lock) {
        _control->is_ended = true;
    }

    std::list<Stream::BufferState> Stream::buffer_states(const Utils::OmniWriteLock& control_lock, bool sort, uint64_t minimum_timestamp) {
        std::list<Stream::BufferState> buffer_states(_control->buffers.begin(), _control->buffers.end());
        if (sort) {
            const auto comparator = [](const BufferState& x, const BufferState& y) { return (x.data_timestamp < y.data_timestamp); };
            buffer_states.sort(comparator);
        }

        return buffer_states;
    }

    void Stream::update_buffer_state(const Utils::OmniWriteLock& control_lock, const Stream::BufferState& buffer_state) {
        if (_role == Role::CONSUMER) {
            throw std::runtime_error("Consumer stream can not update stream buffer states");
        }

        const auto beg = _control->buffers.begin();
        const auto end = _control->buffers.end();
        const auto pred = [&](const BufferState& bs) { return bs.buffer_id == buffer_state.buffer_id; };
        const auto loc = std::find_if(beg, end, pred);

        if (loc == end) {
            _control->buffers.push_back(buffer_state);
        } else {
            *loc = buffer_state;
        }
    }

    std::set<Context*> Stream::subscribers(const Utils::OmniWriteLock& control_lock) {
        const auto beg = _control->subscribers.begin();
        const auto end = _control->subscribers.end();
        return std::set<Context*>(beg, end);
    }

    void Stream::add_subscriber(const Utils::OmniWriteLock& control_lock, Context* context) {
        if (_role == Role::PRODUCER) {
            throw std::runtime_error("Producer stream cannot add subscribers");
        }

        try {
            _control->subscribers.push_back(context);
        } catch (std::exception& e) {
            // re-throw for a more user-friendly error message
            throw std::runtime_error(
                "Subscriber count exceeds the allowable "
                "amount for this stream");
        }
    }

    void Stream::remove_subscriber(const Utils::OmniWriteLock& control_lock, Context* context) {
        if (_role == Role::PRODUCER) {
            throw std::runtime_error("Producer stream cannot remove subscribers");
        }

        const auto beg = _control->subscribers.begin();
        const auto end = _control->subscribers.end();
        const auto loc = std::find(beg, end, context);

        if (loc != end) {
            _control->subscribers.erase(loc);
        }
    }

    bool Stream::has_pending_acknowledgements(const Utils::OmniWriteLock& control_lock, size_t buffer_id) {
        const auto beg = _control->pending_acknowledgements.begin();
        const auto end = _control->pending_acknowledgements.end();
        const auto loc = std::find_if(beg, end, [&](const PendingAcknowledgement& pa) { return pa.buffer_id == buffer_id; });

        return loc != end;
    }

    void Stream::set_pending_acknowledgements(const Utils::OmniWriteLock& control_lock, size_t buffer_id) {
        for (auto const subscriber : _control->subscribers) {
            _control->pending_acknowledgements.push_back(MomentumX::PendingAcknowledgement(buffer_id, subscriber));
        }

        Utils::Logger::get_logger().debug(
            std::string("Reset pending acknowledgements to mirror current subscribers for buffer id " + std::to_string(buffer_id)));
    }

    void Stream::remove_all_pending_acknowledgements(const Utils::OmniWriteLock& control_lock, Context* context) {
        while (true) {
            auto const beg = _control->pending_acknowledgements.begin();
            auto const end = _control->pending_acknowledgements.end();
            auto const loc = std::find_if(beg, end, [&](const PendingAcknowledgement& pa) -> bool { return pa.context == context; });

            if (loc != end) {
                _control->pending_acknowledgements.erase(loc);
            } else {
                return;
            }
        }
    }

    void Stream::remove_pending_acknowledgement(const Utils::OmniWriteLock& control_lock, size_t buffer_id, Context* context) {
        const auto beg = _control->pending_acknowledgements.begin();
        const auto end = _control->pending_acknowledgements.end();
        const auto loc = std::find_if(beg, end, [&](const PendingAcknowledgement& pa) -> bool { return pa.buffer_id == buffer_id && pa.context == context; });

        if (loc != end) {
            _control->pending_acknowledgements.erase(loc);
        } else {
            Utils::Logger::get_logger().warning(std::string("Attempted to remove pending acknowledgement that does not exist for buffer: ") +
                                                std::to_string(buffer_id));
        }
    }

    StreamManager::StreamManager(Context* context, BufferManager* buffer_manager) : _context(context), _buffer_manager(buffer_manager){};

    StreamManager::~StreamManager() {
        const auto lock = get_stream_manager_lock();
        std::map<std::string, Stream*> stream_by_name = _stream_by_name;

        // cleanup
        for (auto const& tuple : stream_by_name) {
            destroy(lock, tuple.second);
        }
    };

    Stream* StreamManager::find(const Lock& lock, std::string name) {
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

    Stream* StreamManager::create(const Lock& lock,
                                  const BufferManager::Lock& buffer_manager_lock,
                                  std::string name,
                                  size_t buffer_size,
                                  size_t buffer_count,
                                  bool sync,
                                  Stream::Role role) {
        const Utils::PathConfig paths(_context->context_path(), name);

        Stream* stream = new Stream(paths, buffer_size, buffer_count, sync, role);
        const auto control_lock = stream->get_control_lock();
        _stream_by_name[name] = stream;

        for (size_t i = 1; i <= buffer_count; i++) {
            std::shared_ptr<Buffer> buffer = _buffer_manager->allocate(buffer_manager_lock, paths, i, buffer_size, true);

            Stream::BufferState buffer_state(buffer->id(), buffer_size, buffer_count, 0, 0, 0);

            stream->update_buffer_state(control_lock, buffer_state);
        }

        return stream;
    }

    void StreamManager::destroy(const Lock& lock, Stream* stream) {
        _stream_by_name.erase(stream->name(stream->get_control_lock()));
        _iteration_by_stream.erase(stream);
        _current_buffer_by_stream.erase(stream);
        delete stream;
    }

    bool StreamManager::is_subscribed(const Lock& lock, const Stream::Lock& control_lock, Stream* stream) {
        // if we don't know about this stream, then clearly we are
        // not subscribed
        if (stream == NULL) {
            return false;
        }

        for (auto const& subscriber : stream->subscribers(control_lock)) {
            if (subscriber == _context) {
                return true;
            }
        }

        Utils::Logger::get_logger().debug(std::string("Not subscribed to stream: " + std::to_string((uint64_t)_context)));
        return false;
    }

    Stream* StreamManager::subscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, std::string name) {
        Stream* stream = find(lock, name);
        const auto control_lock = stream->get_control_lock();

        // short circuit if we're already subscribed
        if (is_subscribed(lock, control_lock, stream)) {
            return stream;
        }

        stream->add_subscriber(control_lock, _context);

        for (auto const& buffer_state : stream->buffer_states(control_lock)) {
            _buffer_manager->allocate(buffer_manager_lock, stream->paths(control_lock), buffer_state.buffer_id, stream->buffer_size(control_lock));
        }

        return stream;
    }

    void StreamManager::unsubscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, Stream* stream) {
        const auto control_lock = stream->get_control_lock();
        stream->remove_all_pending_acknowledgements(control_lock, _context);
        stream->remove_subscriber(control_lock, _context);

        _buffer_manager->deallocate_stream(buffer_manager_lock, stream->paths(control_lock));
    }

    std::shared_ptr<Stream::BufferState> StreamManager::next_buffer_state(const Lock& lock,
                                                                          const BufferManager::Lock& buffer_manager_lock,
                                                                          const Stream::Lock& control_lock,
                                                                          Stream* stream) {
        std::shared_ptr<Buffer> current_buffer = _current_buffer_by_stream[stream];

        std::shared_ptr<Buffer> next_buffer;
        std::shared_ptr<Utils::OmniWriteLock> lock_ptr;

        if (stream->sync(control_lock)) {
            next_buffer = _buffer_manager->peek_next(buffer_manager_lock, stream->paths(control_lock));  // peek

            if (stream->has_pending_acknowledgements(control_lock, next_buffer->id())) {
                return nullptr;
            }

            next_buffer = _buffer_manager->next(buffer_manager_lock, stream->paths(control_lock));  // actually rotate the buffers
            lock_ptr = std::make_shared<Utils::OmniWriteLock>(next_buffer->mutex());                // lock write mutex
        } else {
            for (size_t _ = 0; _ < stream->buffer_count(control_lock); _++) {
                next_buffer = _buffer_manager->next(buffer_manager_lock, stream->paths(control_lock));

                // ensure that we don't write to the same buffer repeatedly
                if (next_buffer == current_buffer) {
                    continue;
                }

                Utils::OmniWriteLock lock(next_buffer->mutex(), bip::defer_lock);  // unlocked
                if (lock.try_lock()) {
                    lock_ptr = std::make_shared<Utils::OmniWriteLock>(std::move(lock));  // locked
                } else {
                    next_buffer = nullptr;
                }

                if (next_buffer != nullptr) {
                    break;
                }
            }
        }

        _current_buffer_by_stream[stream] = next_buffer;

        // create a new buffer state pointer and break out to the caller
        return std::shared_ptr<Stream::BufferState>(
            new Stream::BufferState(next_buffer->id(), next_buffer->size(), stream->buffer_count(control_lock), 0, 0, ++_iteration_by_stream[stream]),
            [lock_ptr](Stream::BufferState* state) {
                delete state;  // lock_ptr maintains file lock until this deleter called
            });
    }

    bool StreamManager::send_buffer_state(const Lock& lock,
                                          const BufferManager::Lock& buffer_manager_lock,
                                          const Stream::Lock& control_lock,
                                          Stream* stream,
                                          Stream::BufferState buffer_state) {
        if (stream->sync(control_lock)) {
            if (stream->subscribers(control_lock).size() == 0) {
                return false;
            }
        }

        if (buffer_state.data_size == 0) {
            Utils::Logger::get_logger().warning("Sending buffer state without having set data_size property");
        }

        std::shared_ptr<Buffer> buffer = _buffer_manager->find(buffer_manager_lock, stream->paths(control_lock), buffer_state.buffer_id);

        // update the buffer write timestamp
        if (buffer_state.data_timestamp == 0) {
            buffer_state.data_timestamp = Utils::now();
        }

        buffer->write_ts(buffer_state.data_timestamp);

        if (stream->sync(control_lock)) {
            stream->set_pending_acknowledgements(control_lock, buffer->id());
        }

        stream->update_buffer_state(control_lock, buffer_state);

        return true;
    }

    bool StreamManager::has_next_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, uint64_t minimum_timestamp) {
        std::list<Stream::BufferState> buffer_states = stream->buffer_states(control_lock, true, stream->sync(control_lock) ? 0 : minimum_timestamp);
        const auto beg = buffer_states.begin();
        const auto end = buffer_states.end();
        const auto pred = [&](const Stream::BufferState& bs) { return bs.iteration > _iteration_by_stream[stream]; };
        const auto loc = std::find_if(beg, end, pred);

        if (stream->is_ended(control_lock)) {
            // Stream has ended, so we only have more buffers if there are previously unaccessed buffers remaining
            return loc != end;
        } else {
            // Stream is not ended so we can presume more buffers are coming.
            return true;
        }
    }

    std::shared_ptr<Stream::BufferState> StreamManager::receive_buffer_state(const Lock& lock,
                                                                             const BufferManager::Lock& buffer_manager_lock,
                                                                             const Stream::Lock& control_lock,
                                                                             Stream* stream,
                                                                             uint64_t minimum_timestamp) {
        std::list<Stream::BufferState> buffer_states = stream->buffer_states(control_lock, true, stream->sync(control_lock) ? 0 : minimum_timestamp);

        for (auto const& buffer_state : buffer_states) {
            {
                if (buffer_state.iteration > _iteration_by_stream[stream]) {
                    _iteration_by_stream[stream] = buffer_state.iteration;
                } else {
                    continue;
                }
            }
            std::shared_ptr<Buffer> buffer = _buffer_manager->find(buffer_manager_lock, stream->paths(control_lock), buffer_state.buffer_id);
            if (buffer == NULL) {
                throw std::runtime_error("Attempted to reference an unallocated buffer with id '" + std::to_string(buffer->id()) + "'");
            }

            Utils::OmniReadLock lock(buffer->mutex(), bip::defer_lock);

            if (lock.try_lock()) {
                // we got the read lock, so return a copy
                // of buffer state to the caller.
                // NOTE: the lock_ptr
                auto lock_ptr = std::make_shared<Utils::OmniReadLock>(std::move(lock));
                return std::shared_ptr<Stream::BufferState>(new Stream::BufferState(buffer_state), [lock_ptr](Stream::BufferState* state) {
                    delete state;  // still delete
                });

            } else {
                // failed to get the lock
            }
        }

        if (stream->is_ended(control_lock)) {
            Utils::Logger::get_logger().info("Unable to read any more messages from ended stream. Automatically unsubscribing...");
            unsubscribe(lock, buffer_manager_lock, stream);
        }

        return NULL;
    }

    void StreamManager::flush_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream) {
        if (stream->sync(control_lock)) {
            Utils::Logger::get_logger().warning("Calling flush on a stream in sync mode is a no-op");
        } else {
            _iteration_by_stream[stream] = stream->buffer_states(control_lock, true).back().iteration;
        }
    }

    void StreamManager::release_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, const Stream::BufferState& buffer_state) {
        if (stream->sync(control_lock)) {
            stream->remove_pending_acknowledgement(control_lock, buffer_state.buffer_id, _context);
        }
    }

    size_t StreamManager::subscriber_count(const Lock& lock, const Stream::Lock& control_lock, Stream* stream) {
        return stream->subscribers(control_lock).size();
    }

}  // namespace MomentumX