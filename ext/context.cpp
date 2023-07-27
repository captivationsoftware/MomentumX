
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

    Context::Context(const std::string& context_path) : _context_path(context_path), _buffer_manager(), _stream_manager(this, &_buffer_manager) {
        Utils::Logger::get_logger().debug(std::string("Created Context (" + std::to_string((uint64_t)this) + ")"));
    };

    Context::~Context() {
        Utils::Logger::get_logger().debug(std::string("Destroyed Context (" + std::to_string((uint64_t)this) + ")"));
    }

    Stream* Context::stream(std::string stream_name, size_t buffer_size, size_t buffer_count, bool sync) {
        // Ensure valid stream name
        Utils::validate_stream(stream_name);

        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        return _stream_manager.create(sm_lock, bm_lock, stream_name, buffer_size, buffer_count, sync, Stream::Role::PRODUCER);
    }

    Stream* Context::subscribe(std::string stream_name) {
        // Ensure valid stream name
        Utils::validate_stream(stream_name);

        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        return _stream_manager.subscribe(sm_lock, bm_lock, stream_name);
    }

    void Context::unsubscribe(Stream* stream) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        _stream_manager.unsubscribe(sm_lock, bm_lock, stream);
    }

    std::shared_ptr<Stream::BufferState> Context::next(Stream* stream) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        auto ct_lock = _stream_manager.get_control_lock(*stream);
        return _stream_manager.next_buffer_state(sm_lock, bm_lock, ct_lock, stream);
    }

    bool Context::send(Stream* stream, const Stream::BufferState& buffer_state) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        auto ct_lock = _stream_manager.get_control_lock(*stream);
        return _stream_manager.send_buffer_state(sm_lock, bm_lock, ct_lock, stream, buffer_state);
    }

    bool Context::can_receive(Stream* stream, uint64_t minimum_timestamp) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto ct_lock = _stream_manager.get_control_lock(*stream);
        return _stream_manager.has_next_buffer_state(sm_lock, ct_lock, stream, minimum_timestamp);
    }

    std::shared_ptr<Stream::BufferState> Context::receive(Stream* stream, uint64_t minimum_timestamp) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        auto ct_lock = _stream_manager.get_control_lock(*stream);
        std::shared_ptr<Stream::BufferState> buffer_state = _stream_manager.receive_buffer_state(sm_lock, bm_lock, ct_lock, stream, minimum_timestamp);

        return buffer_state;
    }

    void Context::release(Stream* stream, const Stream::BufferState& buffer_state) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto ct_lock = _stream_manager.get_control_lock(*stream);
        _stream_manager.release_buffer_state(sm_lock, ct_lock, stream, buffer_state);
    }

    uint8_t* Context::data_address(Stream* stream, uint16_t buffer_id) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto bm_lock = _stream_manager.get_buffer_manager_lock();
        const auto ct_lock = _stream_manager.get_control_lock(*stream);
        std::shared_ptr<Buffer> buffer = _buffer_manager.find(bm_lock, stream->paths(ct_lock), buffer_id);
        if (buffer != NULL) {
            return buffer->address();
        }

        return NULL;
    }

    size_t Context::subscriber_count(Stream* stream) {
        const auto sm_lock = _stream_manager.get_stream_manager_lock();
        const auto ct_lock = _stream_manager.get_control_lock(*stream);
        return _stream_manager.subscriber_count(sm_lock, ct_lock, stream);
    }

    void Context::log_level(Utils::Logger::Level level) {
        Utils::Logger::get_logger().set_level(level);
    }

    std::string Context::context_path() const {
        return _context_path;
    }

}  // namespace MomentumX