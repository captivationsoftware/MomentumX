#ifndef MOMENTUMX_CONTEXT_H
#define MOMENTUMX_CONTEXT_H

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

#include "stream.h"

namespace MomentumX {

    class Context {
       public:
        Context(const std::string& context_path);
        ~Context();
        void term();
        bool is_terminated();
        Stream* stream(std::string stream_name, size_t buffer_size, size_t buffer_count = 0, bool sync = false);
        bool is_subscribed(std::string stream_name);
        Stream* subscribe(std::string stream_name);
        void unsubscribe(Stream* stream);
        Stream::BufferState* next(Stream* stream);
        bool send(Stream* stream, Stream::BufferState* buffer_state);
        Stream::BufferState* receive(Stream* stream, uint64_t minimum_timestamp = 1);
        Stream::BufferState* get_by_buffer_id(Stream* stream, uint16_t buffer_id);
        void flush(Stream* stream);
        void release(Stream* stream, Stream::BufferState* buffer_state);
        uint8_t* data_address(Stream* stream, uint16_t buffer_id);
        size_t subscriber_count(Stream* stream);
        void log_level(Utils::Logger::Level level);

        template <Stream::Role TRole>
        static std::shared_ptr<Context> scoped_singleton_impl(const std::string& context) {
            static std::mutex m;
            std::lock_guard<std::mutex> lock(m);

            static std::map<std::string, std::weak_ptr<Context>> as_weak_set;
            std::weak_ptr<Context>& as_weak = as_weak_set[context];
            std::shared_ptr<Context> as_strong = as_weak.lock();
            if (!as_strong) {
                as_strong = std::make_shared<Context>(context);
                as_weak = as_strong;
            }

            return as_strong;
        }

        static std::shared_ptr<Context> scoped_producer_singleton(const std::string& context) {
            return scoped_singleton_impl<Stream::Role::PRODUCER>(context);  // DRY hack
        }
        static std::shared_ptr<Context> scoped_consumer_singleton(const std::string& context) {
            return scoped_singleton_impl<Stream::Role::CONSUMER>(context);  // DRY hack
        }

        std::string context_path() const;

       private:
        friend class StreamManager;

        std::atomic<bool> _terminated;
        std::string _context_path;
        BufferManager _buffer_manager;
        StreamManager _stream_manager;
        std::set<Stream*> _subscriptions;
        std::mutex _mutex;
    };

}  // namespace MomentumX

#endif
