#ifndef MOMENTUMX_CONTEXT_H
#define MOMENTUMX_CONTEXT_H

#include <string>
#include <functional>
#include <atomic>
#include <map>
#include <memory>
#include <list>
#include <set>
#include <tuple>
#include <random>
#include <iomanip>
#include <mutex>

#include "buffer.h"
#include "stream.h"
#include "utils.h"

namespace MomentumX
{

    class Context
    {

    public:
        Context() : 
                    _terminated(false),
                    _buffer_manager(),
                    _stream_manager(this, &_buffer_manager) {
            Utils::Logger::get_logger().info(
                std::string("Created Context (" + std::to_string((uint64_t)this) + ")"));
        };

        ~Context() {
            term();
            Utils::Logger::get_logger().info(
                std::string("Deleted Context (" + std::to_string((uint64_t)this) + ")")
            );
        }

        void term() {
            std::lock_guard<std::mutex> lock(_mutex);
            if (_terminated) return;

            for (auto const &stream : _subscriptions) {
                _stream_manager.unsubscribe(stream);
            }

            Utils::Logger::get_logger().info(
                std::string("Terminated Context (" + std::to_string((uint64_t)this) + ")"));

            _terminated = true;
        }

        bool is_terminated() {
            return _terminated;
        }

        Stream *stream(std::string stream_name, size_t buffer_size, size_t buffer_count = 0, bool sync = false) {
            // Ensure valid stream name
            Utils::validate_stream(stream_name);

            return _stream_manager.create(
                stream_name,
                buffer_size,
                buffer_count,
                sync,
                Stream::Role::PRODUCER
            );
        }

        bool is_subscribed(std::string stream_name) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            // Ensure valid stream name
            Utils::validate_stream(stream_name);

            try {
                return _stream_manager.is_subscribed(stream_name);
            }
            catch (...) {
                return false;
            }
        }

        Stream *subscribe(std::string stream_name) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            // Ensure valid stream name
            Utils::validate_stream(stream_name);

            if (is_subscribed(stream_name)) {
                return _stream_manager.find(stream_name);
            }

            std::lock_guard<std::mutex> lock(_mutex);
            Stream *stream = _stream_manager.subscribe(stream_name);
            _subscriptions.insert(stream);
            return stream;
        }

        void unsubscribe(Stream *stream) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            _stream_manager.unsubscribe(stream);

            std::lock_guard<std::mutex> lock(_mutex);
            _subscriptions.erase(stream);
        }

        Stream::BufferState *next(Stream *stream) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            return _stream_manager.next_buffer_state(stream);
        }

        bool send(Stream *stream, Stream::BufferState *buffer_state) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            return _stream_manager.send_buffer_state(stream, buffer_state);
        }

        Stream::BufferState *receive(Stream *stream, uint64_t minimum_timestamp = 1) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            Stream::BufferState *buffer_state = _stream_manager.receive_buffer_state(
                stream, minimum_timestamp
            );

            if (buffer_state != NULL && buffer_state->buffer_id == 0) {
                unsubscribe(stream);
            }

            return buffer_state;
        }

        Stream::BufferState *get_by_buffer_id(Stream *stream, uint16_t buffer_id) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            return _stream_manager.get_by_buffer_id(
                stream, buffer_id
            );
        }

        void flush(Stream *stream) {
            if (is_terminated()) {
                throw std::runtime_error("Terminated");
            }

            _stream_manager.flush_buffer_state(stream);
        }

        void release(Stream *stream, Stream::BufferState *buffer_state) {
            _stream_manager.release_buffer_state(stream, buffer_state);
        }

        uint8_t *data_address(Stream *stream, uint16_t buffer_id) {
            Buffer *buffer = _buffer_manager.find(stream->name(), buffer_id);
            if (buffer != NULL) {
                return buffer->address();
            }

            return NULL;
        }

        size_t subscriber_count(Stream *stream) {
            return _stream_manager.subscriber_count(stream);
        }

        void log_level(Utils::Logger::Level level) {
            Utils::Logger::get_logger().set_level(level);
        }

        template <Stream::Role TRole>
        static std::shared_ptr<Context> scoped_singleton_impl() {
            static std::mutex m;
            std::lock_guard<std::mutex> lock(m);

            static std::weak_ptr<Context> as_weak;
            std::shared_ptr<Context> as_strong = as_weak.lock();
            if (!as_strong)
            {
                as_strong = std::make_shared<Context>();
                as_weak = as_strong;
            }

            return as_strong;
        }

        static std::shared_ptr<Context> scoped_producer_singleton() { return scoped_singleton_impl<Stream::Role::PRODUCER>(); } // DRY hack
        static std::shared_ptr<Context> scoped_consumer_singleton() { return scoped_singleton_impl<Stream::Role::CONSUMER>(); } // DRY hack

    private:
        friend class StreamManager;

        std::atomic<bool> _terminated;
        BufferManager _buffer_manager;
        StreamManager _stream_manager;
        std::set<Stream*> _subscriptions;
        std::mutex _mutex;
    };

}

#endif
