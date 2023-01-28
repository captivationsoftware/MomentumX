
#include <atomic>
#include <functional>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <random>
#include <set>
#include <string>
#include <tuple>

#include "buffer.h"
#include "context.h"
#include "stream.h"
#include "utils.h"

namespace MomentumX {

    Context::Context(const std::string& context_path)
        : _terminated(false), _context_path(context_path), _buffer_manager(), _stream_manager(this, &_buffer_manager) {
        Utils::Logger::get_logger().info(std::string("Created Context (" + std::to_string((uint64_t)this) + ")"));
    };

    Context::~Context() {
        term();
        Utils::Logger::get_logger().info(std::string("Deleted Context (" + std::to_string((uint64_t)this) + ")"));
    }

    void Context::term() {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_terminated)
            return;

        for (Stream* stream : _subscriptions) {
            _stream_manager.unsubscribe(stream);
        }

        Utils::Logger::get_logger().info(std::string("Terminated Context (" + std::to_string((uint64_t)this) + ")"));

        _terminated = true;
    }

    bool Context::is_terminated() {
        return _terminated;
    }

    Stream* Context::stream(std::string stream_name, size_t buffer_size, size_t buffer_count, bool sync) {
        // Ensure valid stream name
        Utils::validate_stream(stream_name);

        return _stream_manager.create(stream_name, buffer_size, buffer_count, sync, Stream::Role::PRODUCER);
    }

    bool Context::is_subscribed(std::string stream_name) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        // Ensure valid stream name
        Utils::validate_stream(stream_name);

        try {
            return _stream_manager.is_subscribed(stream_name);
        } catch (...) {
            return false;
        }
    }

    Stream* Context::subscribe(std::string stream_name) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        // Ensure valid stream name
        Utils::validate_stream(stream_name);

        if (is_subscribed(stream_name)) {
            return _stream_manager.find(stream_name);
        }

        std::lock_guard<std::mutex> lock(_mutex);
        Stream* stream = _stream_manager.subscribe(stream_name);
        _subscriptions.insert(stream);
        return stream;
    }

    void Context::unsubscribe(Stream* stream) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        _stream_manager.unsubscribe(stream);

        std::lock_guard<std::mutex> lock(_mutex);
        _subscriptions.erase(stream);
    }

    Stream::BufferState* Context::next(Stream* stream) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        return _stream_manager.next_buffer_state(stream);
    }

    bool Context::send(Stream* stream, Stream::BufferState* buffer_state) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        return _stream_manager.send_buffer_state(stream, buffer_state);
    }

    Stream::BufferState* Context::receive(Stream* stream, uint64_t minimum_timestamp) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        Stream::BufferState* buffer_state = _stream_manager.receive_buffer_state(stream, minimum_timestamp);

        if (buffer_state != NULL && buffer_state->buffer_id == 0) {
            unsubscribe(stream);
        }

        return buffer_state;
    }

    Stream::BufferState* Context::get_by_buffer_id(Stream* stream, uint16_t buffer_id) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        return _stream_manager.get_by_buffer_id(stream, buffer_id);
    }

    void Context::flush(Stream* stream) {
        if (is_terminated()) {
            throw std::runtime_error("Terminated");
        }

        _stream_manager.flush_buffer_state(stream);
    }

    void Context::release(Stream* stream, Stream::BufferState* buffer_state) {
        _stream_manager.release_buffer_state(stream, buffer_state);
    }

    uint8_t* Context::data_address(Stream* stream, uint16_t buffer_id) {
        Buffer* buffer = _buffer_manager.find(stream->paths(), buffer_id);
        if (buffer != NULL) {
            return buffer->address();
        }

        return NULL;
    }

    size_t Context::subscriber_count(Stream* stream) {
        return _stream_manager.subscriber_count(stream);
    }

    void Context::log_level(Utils::Logger::Level level) {
        Utils::Logger::get_logger().set_level(level);
    }

    std::string Context::context_path() const {
        return _context_path;
    }

}  // namespace MomentumX