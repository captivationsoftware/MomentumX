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
    auto stream(const char *stream_name, size_t buffer_size, size_t buffer_count, bool sync = false) -> Stream * { return mx_stream(this->ctx, stream_name, buffer_size, buffer_count, sync); }
    auto stream_flush(Stream *stream) -> void { mx_stream_flush(this->ctx, stream); }
    auto stream_next(Stream *stream) -> BufferState * { return mx_stream_next(this->ctx, stream); }
    auto stream_receive(Stream *stream, uint64_t minimum_ts = 1) -> BufferState * { return mx_stream_receive(this->ctx, stream, minimum_ts); }
    auto stream_release(Stream *stream, BufferState *buffer_state) -> bool { return mx_stream_release(this->ctx, stream, buffer_state); }
    auto stream_send(Stream *stream, BufferState *buffer_state) -> bool { return mx_stream_send(this->ctx, stream, buffer_state); }
    auto subscribe(const char *stream_name) -> Stream * { return mx_subscribe(this->ctx, stream_name); }
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

    py::class_<Stream::BufferState>(m, "BufferState");
    py::class_<Stream>(m, "Stream");

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
        .def("stream_receive", &ContextShim::stream_receive, py::return_value_policy::reference_internal, "stream"_a, "minimum_ts"_a)                   // ->BufferState*
        .def("stream_release", &ContextShim::stream_release, "stream"_a, "buffer_state"_a)                                                              // ->bool
        .def("stream_send", &ContextShim::stream_send, "stream"_a, "buffer_state"_a)                                                                    // ->bool
        .def("subscribe", &ContextShim::subscribe, py::return_value_policy::reference_internal, "stream_name"_a)                                        // ->Stream*
        .def("subscriber_count", &ContextShim::subscriber_count, "stream"_a)                                                                            // ->size_t
        .def("unsubscribe", &ContextShim::unsubscribe, "stream"_a);                                                                                     // ->bool
}
