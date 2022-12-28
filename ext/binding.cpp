#include "buffer.h"
#include "context.h"
#include "momentumx.h"
#include "stream.h"
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

namespace py = pybind11;
using namespace py::literals; // provides `_a` literal for parameter names

using BufferState = MomentumX::Stream::BufferState;
using LogLevel = MomentumX::Utils::Logger::Level;
using MomentumX::Buffer;
using MomentumX::Context;
using MomentumX::Stream;


struct StreamShim
{
    Context *ctx{nullptr}; // no deleter, requires explicit destructor call
    Stream *stream{nullptr}; // no deleter, requires explicit destructor call

    StreamShim(Context *ctx, Stream *stream) { 
        this->ctx = ctx; 
        this->stream = stream; 
    }

    // lifecycle
    auto flush() -> void { mx_stream_flush(this->ctx, this->stream); }
    auto next() -> BufferState * { return mx_stream_next(this->ctx, this->stream); }
    auto receive(uint64_t minimum_ts = 1) -> BufferState * { return mx_stream_receive(this->ctx, this->stream, minimum_ts); }
    auto release(BufferState *buffer_state) -> bool { return mx_stream_release(this->ctx, this->stream, buffer_state); }
    auto data_address(uint16_t buffer_id) -> uint8_t * { return mx_data_address(this->ctx, this->stream, buffer_id); }
    auto send(BufferState *buffer_state) -> bool { return mx_stream_send(this->ctx, this->stream, buffer_state); }
    auto send_string(const std::string& str) -> bool {
        BufferState *buffer = next();
        if (buffer == 0) {
            return false;
        }
        uint8_t *arr = data_address(buffer->buffer_id);

        std::copy(str.begin(), str.end(), arr);
        buffer->data_size = str.size();

        return mx_stream_send(this->ctx, this->stream, buffer); 
    }

    auto receive_string() -> std::string {
        BufferState *buffer = receive();
        if (buffer == 0) {
            return "";
        }
        uint8_t *arr = data_address(buffer->buffer_id);
        std::string message = reinterpret_cast<char *>(arr);
        release(buffer);
        return message;
    }

    // members
    auto is_alive() -> bool { return this->stream->is_alive(); }
    auto name() -> std::string { return this->stream->name(); }
    auto fd() -> size_t { return this->stream->fd(); }
    auto sync() -> bool { return this->stream->sync(); }
    auto buffer_size() -> size_t { return this->stream->buffer_size(); }
    auto buffer_count() -> size_t { return this->stream->buffer_count(); }
};


struct ContextShim
{
    Context *ctx{nullptr}; // no deleter, requires explicit destructor call

    ContextShim(LogLevel log_level = LogLevel::WARNING) { ctx = mx_context(static_cast<uint8_t>(log_level)); }

    // lifecycle
    auto term() -> bool { return mx_term(this->ctx); }
    auto is_terminated() -> bool { return mx_is_terminated(this->ctx); }
    auto destroy() -> bool { return mx_destroy(this->ctx); }
    auto data_address(Stream *stream, uint16_t buffer_id) -> uint8_t * { return mx_data_address(this->ctx, stream, buffer_id); }
    auto get_by_buffer_id(Stream *stream, uint16_t buffer_id) -> BufferState * { return mx_get_by_buffer_id(this->ctx, stream, buffer_id); }
    auto is_stream_sync(Stream *stream) -> bool { return mx_is_stream_sync(this->ctx, stream); }
    auto is_subscribed(const char *stream_name) -> bool { return mx_is_subscribed(this->ctx, stream_name); }
    auto log_level(LogLevel log_level) -> void { mx_log_level(this->ctx, static_cast<uint8_t>(log_level)); }
    auto stream(const char *stream_name, size_t buffer_size, size_t buffer_count, bool sync = false) -> StreamShim { return StreamShim(this->ctx, mx_stream(this->ctx, stream_name, buffer_size, buffer_count, sync)); }
    auto stream_flush(Stream *stream) -> void { mx_stream_flush(this->ctx, stream); }
    auto stream_next(Stream *stream) -> BufferState * { return mx_stream_next(this->ctx, stream); }
    auto stream_receive(Stream *stream, uint64_t minimum_ts = 1) -> BufferState * { return mx_stream_receive(this->ctx, stream, minimum_ts); }
    auto stream_release(Stream *stream, BufferState *buffer_state) -> bool { return mx_stream_release(this->ctx, stream, buffer_state); }
    auto stream_send(Stream *stream, BufferState *buffer_state) -> bool { return mx_stream_send(this->ctx, stream, buffer_state); }
    auto subscribe(const char *stream_name) -> StreamShim { return StreamShim(this->ctx, mx_subscribe(this->ctx, stream_name)); }
    auto subscriber_count(Stream *stream) -> size_t { return mx_subscriber_count(this->ctx, stream); }
    auto unsubscribe(Stream *stream) -> bool { return mx_unsubscribe(this->ctx, stream); }
};


PYBIND11_MODULE(_mx, m)
{
    py::enum_<LogLevel>(m, "LogLevel")
        .value("DEBUG", LogLevel::DEBUG)
        .value("INFO", LogLevel::INFO)
        .value("WARNING", LogLevel::WARNING)
        .value("ERROR", LogLevel::ERROR);

    py::class_<Stream::BufferState>(m, "BufferState")
        .def_readwrite("buffer_id", &BufferState::buffer_id)
        .def_readwrite("buffer_size", &BufferState::buffer_size)
        .def_readwrite("buffer_count", &BufferState::buffer_count)
        .def_readwrite("data_size", &BufferState::data_size)
        .def_readwrite("data_timestamp", &BufferState::data_timestamp)
        .def_readwrite("iteration", &BufferState::iteration);

    py::class_<StreamShim>(m, "Stream")
        .def("flush", &StreamShim::flush)
        .def("next", &StreamShim::next, py::return_value_policy::reference_internal)
        .def("receive", &StreamShim::receive, py::return_value_policy::reference_internal, "minimum_ts"_a = 1)
        .def("receive_string", &StreamShim::receive_string, py::return_value_policy::reference_internal)
        .def("release", &StreamShim::release, "buffer_state"_a)
        .def("send", &StreamShim::send, "buffer_state"_a)
        .def("send_string", &StreamShim::send_string, "message"_a)
        .def("is_alive", &StreamShim::is_alive)
        .def("name", &StreamShim::name)
        .def("fd", &StreamShim::fd)
        .def("sync", &StreamShim::sync)
        .def("buffer_size", &StreamShim::buffer_size)
        .def("buffer_count", &StreamShim::buffer_count);

    py::class_<ContextShim>(m, "Context")
        .def(py::init<>())
        .def(py::init<LogLevel>())

        .def("term", &ContextShim::term)                                                                                                                // ->bool
        .def("is_terminated", &ContextShim::is_terminated)                                                                                              // ->bool
        .def("destroy", &ContextShim::destroy)                                                                                                          // ->bool
        .def("data_address", &ContextShim::data_address, py::return_value_policy::reference_internal, "stream"_a, "buffer_id"_a)                        // ->uint8_t*
        .def("get_by_buffer_id", &ContextShim::get_by_buffer_id, py::return_value_policy::reference_internal, "stream"_a, "buffer_id"_a)                // ->BufferState*
        .def("is_stream_sync", &ContextShim::is_stream_sync, "stream"_a)                                                                                // ->bool
        .def("is_subscribed", &ContextShim::is_subscribed, "stream_name"_a)                                                                             // ->bool
        .def("log_level", &ContextShim::log_level, "log_level"_a)                                                                                       // ->void
        .def("stream", &ContextShim::stream, py::return_value_policy::reference_internal, "stream_name"_a, "buffer_size"_a, "buffer_count"_a, "sync"_a) // ->Stream*
        .def("stream_flush", &ContextShim::stream_flush, "stream"_a)                                                                                    // ->void
        .def("stream_next", &ContextShim::stream_next, py::return_value_policy::reference_internal, "stream"_a)                                         // ->BufferState*
        .def("stream_receive", &ContextShim::stream_receive, py::return_value_policy::reference_internal, "stream"_a, "minimum_ts"_a = 1)                   // ->BufferState*
        .def("stream_release", &ContextShim::stream_release, "stream"_a, "buffer_state"_a)                                                              // ->bool
        .def("stream_send", &ContextShim::stream_send, "stream"_a, "buffer_state"_a)                                                                    // ->bool
        .def("subscribe", &ContextShim::subscribe, py::return_value_policy::reference_internal, "stream_name"_a)                                        // ->Stream*
        .def("subscriber_count", &ContextShim::subscriber_count, "stream"_a)                                                                            // ->size_t
        .def("unsubscribe", &ContextShim::unsubscribe, "stream"_a);                                                                                     // ->bool
}
