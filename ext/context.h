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
        Stream* stream(std::string stream_name, size_t buffer_size, size_t buffer_count = 0, bool sync = false);
        Stream* subscribe(std::string stream_name);
        void unsubscribe(Stream* stream);
        std::shared_ptr<Stream::BufferState> next(Stream* stream);
        bool send(Stream* stream, const Stream::BufferState& buffer_state);
        bool can_receive(Stream* stream, uint64_t minimum_timestamp = 1);
        std::shared_ptr<Stream::BufferState> receive(Stream* stream, uint64_t minimum_timestamp = 1);
        void release(Stream* stream, const Stream::BufferState& buffer_state);
        uint8_t* data_address(Stream* stream, uint16_t buffer_id);
        size_t subscriber_count(Stream* stream);
        void log_level(Utils::Logger::Level level);

        std::string context_path() const;

       private:
        friend class StreamManager;

        std::string _context_path;
        BufferManager _buffer_manager;
        StreamManager _stream_manager;
        std::set<Stream*> _subscriptions;
    };

}  // namespace MomentumX

#endif
