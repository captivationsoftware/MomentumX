#include "stream.h"

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <algorithm>
#include <boost/interprocess/creation_tags.hpp>
#include <chrono>
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

    using init_bool = std::atomic_bool;

    Stream::Stream(const Utils::PathConfig& paths, size_t buffer_size, size_t buffer_count, bool sync, Stream::Role role)
        : _paths(paths),
          _role(role),
          _fd(open(_paths.stream_path.c_str(), O_RDWR | (_role == Role::PRODUCER ? O_CREAT | O_EXCL : 0), S_IRWXU)),
          _data(nullptr),
          _control(nullptr) {
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
            new (_control) ControlBlock();  // placement construct into shared memory

            const auto control_lock = get_control_lock();
            _control->sync = sync;
            _control->buffer_size = buffer_size;
            _control->buffer_count = buffer_count;

            Utils::Logger::get_logger().debug(std::string("Created Producer Stream (" + std::to_string((uint64_t)this) + ")"));
        } else {
            auto control_lock = get_control_lock();
            if (!_control->is_ready()) {
                throw std::runtime_error("Attempt to open stream before it is ready");
            }
            _control->subscriber_count++;
            if (_control->sync) {
                const auto index_iterations = _control->sorted_index_iterations(control_lock);
                if (index_iterations.size() != 0) {
                    // If any previous iterations, set to just before the oldest still-rechable buffer
                    const auto dec = [&](size_t idx) { return ControlBlock::wrapping_decrement(idx, _control->buffer_count); };
                    _last_index = dec(std::get<0>(index_iterations.at(0)));     // prevous index, so next increment places us at oldest
                    _last_iteration = std::get<1>(index_iterations.at(0)) - 1;  // value when the previous increment was created

                    for (auto ii : index_iterations) {
                        const auto idx = std::get<0>(ii);
                        auto& bsync = _control->buffers.at(idx).buffer_sync;
                        bsync.inc_required(control_lock);
                    }
                }
            }
            _subscribed = true;

            Utils::Logger::get_logger().debug(std::string("Created Consumer Stream (" + std::to_string((uint64_t)this) + ")"));
        }

        // Initialize any cached variables that will remain for the duration of the stream to prevent extraneous locking...
        _sync = _control->sync;
    };

    Stream::~Stream() {
        {
            auto control_lock = get_control_lock();
            if (_role == Role::PRODUCER) {
                Utils::Logger::get_logger().debug(std::string("Producer stream signaling end of stream (" + std::to_string((uint64_t)this) + ")"));
                end(control_lock);
            } else {
                if (_subscribed) {
                    --_control->subscriber_count;

                    // If we are destructing while producer is still sending, claim and release all unmanaged buffers
                    // Since this happens with the control block lock, any currently checkout-out write buffers will
                    // submitted with the updated subscriber_count value
                    if (_control->sync) {
                        const auto inc = [&](size_t idx) { return ControlBlock::wrapping_increment(idx, _control->buffer_count); };
                        while (_last_iteration != _control->last_sent_iteration()) {
                            _last_index = inc(_last_index);
                            _last_iteration++;

                            auto& b = _control->buffers.at(_last_index);
                            auto& bsync = b.buffer_sync;
                            if (bsync.checkout_read(control_lock) == BufferSync::CheckoutResult::SUCCESS) {
                                bsync.checkin_read(control_lock);
                            }
                        }
                    }
                }
            }
        }

        bool has_errors = false;
        int return_val;

        if (_data != MAP_FAILED) {
            return_val = munmap(_data, Utils::page_aligned_size(sizeof(ControlBlock)));
            if (return_val != 0) {
                has_errors |= true;
                Utils::Logger::get_logger().warning(std::string("Stream Unmap Failed (" + std::to_string((uint64_t)this) + ")"));
            } else {
                Utils::Logger::get_logger().debug(std::string("Stream Unmapped (" + std::to_string((uint64_t)this) + ")"));
            }
        }

        if (_fd > -1) {
            return_val = ::close(_fd);
            if (return_val != 0) {
                has_errors |= true;
                Utils::Logger::get_logger().warning(std::string("Stream Close Failed (" + std::to_string((uint64_t)this) + ")"));
            } else {
                Utils::Logger::get_logger().debug(std::string("Stream Closed (" + std::to_string((uint64_t)this) + ")"));
            }

            if (_role == Role::PRODUCER) {
                return_val = std::remove(_paths.stream_path.c_str());
                if (return_val != 0) {
                    has_errors |= true;
                    std::stringstream ss;
                    ss << "Unable to delete stream file \"" << _paths.stream_path << "\" with error: " << return_val;
                    Utils::Logger::get_logger().warning(ss.str());
                }
            }
        }

        std::stringstream ss;
        ss << "Destroyed " << (_role == PRODUCER ? "Producer" : "Consumer") << " Stream (" << std::to_string((uint64_t)this) << ")";

        if (has_errors) {
            ss << " [POSSIBLE MEMORY LEAK DETECTED!]";
            Utils::Logger::get_logger().warning(ss.str());
        } else {
            Utils::Logger::get_logger().debug(ss.str());
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

    void Stream::unsubscribe() {
        auto control_lock = get_control_lock();
        if (_role == Role::PRODUCER) {
            throw std::logic_error("Producer cannot unsubscribe");
        }

        if (_subscribed) {
            _subscribed = false;
            --_control->subscriber_count;

            // Remove required acknowledgement for all future buffers
            for (auto& b : _control->buffers) {
                auto& bs = b.buffer_state;
                if (_last_iteration < bs.iteration) {
                    b.buffer_sync.dec_required(control_lock);
                }
            }
        }
    }

    Stream::Lock Stream::get_control_lock() {
        return Lock(_control->control_mutex);
    }

    std::list<Stream::BufferState> Stream::buffer_states(const Utils::OmniWriteLock& control_lock, bool sort, uint64_t minimum_timestamp) {
        std::list<Stream::BufferState> buffer_states;
        for (const auto& b : _control->buffers) {
            buffer_states.push_back(b.buffer_state);
        }

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
        const auto pred = [&](const LockableBufferState& lbs) { return lbs.buffer_state.buffer_id == buffer_state.buffer_id; };
        const auto loc = std::find_if(beg, end, pred);

        if (loc == end) {
            _control->buffers.emplace_back(buffer_state);
        } else {
            loc->buffer_state = buffer_state;
        }
    }

    size_t Stream::subscriber_count(const Utils::OmniWriteLock& control_lock) {
        return _control->subscriber_count;
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

    Stream* StreamManager::find_or_create(const Lock& lock, std::string name) {
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

        for (size_t i = 0; i < buffer_count; i++) {
            std::shared_ptr<Buffer> buffer = _buffer_manager->allocate(buffer_manager_lock, paths, i, buffer_size, true);

            Stream::BufferState buffer_state(buffer->id(), buffer_size, buffer_count, 0, 0, 0);

            stream->update_buffer_state(control_lock, buffer_state);
        }

        return stream;
    }

    void StreamManager::destroy(const Lock& lock, Stream* stream) {
        _stream_by_name.erase(stream->name(stream->get_control_lock()));
        delete stream;
    }

    Stream* StreamManager::subscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, std::string name) {
        Stream* stream = find_or_create(lock, name);
        const auto control_lock = stream->get_control_lock();

        // short circuit if we're already subscribed
        const bool is_subscribed = _buffers_by_stream.count(stream) != 0;
        if (is_subscribed) {
            return stream;
        }

        for (auto const& buffer_state : stream->buffer_states(control_lock)) {
            _buffer_manager->allocate(buffer_manager_lock, stream->paths(control_lock), buffer_state.buffer_id, stream->buffer_size(control_lock));
        }

        return stream;
    }

    void StreamManager::unsubscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, Stream* stream) {
        const auto control_lock = stream->get_control_lock();

        _buffer_manager->deallocate_stream(buffer_manager_lock, stream->paths(control_lock));
    }

    std::shared_ptr<Stream::BufferState> StreamManager::next_buffer_state(const Lock& lock,
                                                                          const BufferManager::Lock& buffer_manager_lock,
                                                                          Stream::Lock& control_lock,
                                                                          Stream* stream) {
        // convenience wrapper
        const auto inc = [&](size_t idx) { return ControlBlock::wrapping_increment(idx, stream->_control->buffer_count); };

        const size_t iteration = stream->_last_iteration + 1;

        if (stream->sync(control_lock)) {
            const size_t idx = inc(stream->_control->last_sent_index);

            auto& b = stream->_control->buffers.at(idx);
            auto& bs = b.buffer_state;
            const bool has_buffer = b.buffer_sync.checkout_write(control_lock, std::chrono::milliseconds(200));

            if (has_buffer) {
                auto ptr = new Stream::BufferState(bs.buffer_id, bs.buffer_size, bs.buffer_count, 0, 0, iteration);
                auto del = [stream, idx](Stream::BufferState* state) {
                    // TODO: capture by shared_ptr, not raw pointer
                    auto control_lock = stream->get_control_lock();
                    stream->_control->buffers.at(idx).buffer_sync.checkin_write(control_lock);

                    delete state;
                };
                return std::shared_ptr<Stream::BufferState>(ptr, del);
            }
        } else {
            const size_t beg = inc(stream->_control->last_sent_index);
            size_t idx = beg;
            do {
                auto& b = stream->_control->buffers.at(idx);
                auto& bs = b.buffer_state;
                const bool has_buffer = b.buffer_sync.checkout_write(control_lock, std::chrono::seconds(0));

                if (has_buffer) {
                    auto ptr = new Stream::BufferState(bs.buffer_id, bs.buffer_size, bs.buffer_count, 0, 0, iteration);
                    auto del = [stream, idx](Stream::BufferState* state) {
                        // TODO: capture by shared_ptr, not raw pointer
                        auto control_lock = stream->get_control_lock();
                        stream->_control->buffers.at(idx).buffer_sync.checkin_write(control_lock);

                        delete state;
                    };
                    return std::shared_ptr<Stream::BufferState>(ptr, del);
                }
                idx = inc(idx);
            } while (idx != beg);  // stop if we cycle all the way around back to the start
        }
        return nullptr;
    }

    bool StreamManager::send_buffer_state(const Lock& lock,
                                          const BufferManager::Lock& buffer_manager_lock,
                                          Stream::Lock& control_lock,
                                          Stream* stream,
                                          Stream::BufferState buffer_state) {
        if (buffer_state.data_size == 0) {
            Utils::Logger::get_logger().warning("Sending buffer state without having set data_size property");
        }

        const size_t iteration = buffer_state.iteration;
        const size_t idx = buffer_state.buffer_id;

        auto& b = stream->_control->buffers.at(idx);
        const size_t required_readers = stream->_control->sync ? stream->_control->subscriber_count : 0;
        b.buffer_sync.mark_sent(control_lock, required_readers);
        stream->_last_iteration = iteration;      // update buffer iteration
        stream->_control->last_sent_index = idx;  // update buffer index
        b.buffer_state = buffer_state;
        return true;
    }

    bool StreamManager::has_next_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, uint64_t minimum_timestamp) {
        if (!stream->is_ended(control_lock)) {
            // Stream is not ended so we can presume more buffers are coming.
            return true;
        } else {
            // Stream has ended, so we only have more buffers if there are previously unaccessed buffers remaining
            return stream->_last_iteration < stream->_control->buffers.at(stream->_control->last_sent_index).buffer_state.iteration;
        }
    }

    std::shared_ptr<Stream::BufferState> StreamManager::receive_buffer_state(const Lock& lock,
                                                                             const BufferManager::Lock& buffer_manager_lock,
                                                                             Stream::Lock& control_lock,
                                                                             Stream* stream,
                                                                             uint64_t minimum_timestamp) {
        // convenience wrapper
        const auto inc = [&](size_t idx) { return ControlBlock::wrapping_increment(idx, stream->_control->buffer_count); };
        const auto dec = [&](size_t idx) { return ControlBlock::wrapping_decrement(idx, stream->_control->buffer_count); };

        const size_t last_read_idx = stream->_last_index;
        const size_t last_sent_idx = stream->_control->last_sent_index;
        const size_t last_read_iteration = stream->_last_iteration;
        const size_t last_sent_iteration = stream->_control->buffers.at(last_sent_idx).buffer_state.iteration;
        if (last_sent_iteration == last_read_iteration) {
            return nullptr;  // TODO: block once we've caught up, instead of just bailing like we are now
        }

        size_t next_idx = inc(last_read_idx);
        size_t next_expected_iteration = last_read_iteration + 1;

        auto b = std::ref(stream->_control->buffers.at(next_idx));

        // In the simple case, the next buffer (sequentially) is used.
        // If that's not the case, we'll have to search for the next available buffer.
        // NOTE: `sync` mode requires each buffer to be used sequentially, so this is effectively a no-op in `sync` mode.
        const bool next_idx_matches_next_iteration = b.get().buffer_state.iteration == next_expected_iteration;
        if (next_idx_matches_next_iteration) {
            // nothing to do, next is correct
        } else {
            if (stream->_sync) {
                throw std::runtime_error("Streaming search routine triggered in sync case");
            }

            // streaming case where we need to search
            std::vector<LockableBufferState*> sorted_bufs;
            std::for_each(stream->_control->buffers.begin(), stream->_control->buffers.end(), [&](auto& lbstate) { sorted_bufs.push_back(&lbstate); });
            std::sort(sorted_bufs.begin(), sorted_bufs.end(),
                      [](const auto* lhs, const auto* rhs) { return lhs->buffer_state.iteration < rhs->buffer_state.iteration; });

            // search sorted buffers for smallest iteration that is greater than (or equal to) expected
            const auto it = std::find_if(sorted_bufs.begin(), sorted_bufs.end(),
                                         [&](const auto* lbstate) { return next_expected_iteration <= lbstate->buffer_state.iteration; });

            if (it == sorted_bufs.end()) {
                throw std::logic_error("Unable to find buffer with expected iteration");
            }

            b = std::ref(**it);
        }

        const auto checkout_result = b.get().buffer_sync.checkout_read(control_lock);
        const bool has_buffer = (checkout_result == BufferSync::CheckoutResult::SUCCESS);
        if (!has_buffer) {
            return nullptr;
        }
        stream->_last_index = b.get().buffer_state.buffer_id;
        stream->_last_iteration = b.get().buffer_state.iteration;

        const auto buffer_id = b.get().buffer_state.buffer_id;
        const auto ptr = new Stream::BufferState(b.get().buffer_state);
        const auto del = [stream, buffer_id](Stream::BufferState* state) {
            auto control_lock = stream->get_control_lock();
            stream->_control->buffers.at(buffer_id).buffer_sync.checkin_read(control_lock);
            delete state;
        };

        return std::shared_ptr<Stream::BufferState>(ptr, del);
    }

    void StreamManager::release_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, const Stream::BufferState& buffer_state) {
        if (stream->sync(control_lock)) {
            // stream->remove_pending_acknowledgement(control_lock, buffer_state.buffer_id, _context);
        }
    }

    size_t StreamManager::subscriber_count(const Lock& lock, const Stream::Lock& control_lock, Stream* stream) {
        return stream->_control->subscriber_count;
    }

}  // namespace MomentumX
