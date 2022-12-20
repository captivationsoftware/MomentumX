#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
namespace py = pybind11;

#include "momentumx.h"

using Context = MomentumX::Context;
using LogLevel = MomentumX::Utils::Logger::Level;

struct Stats
{
};

PYBIND11_MODULE(_mx, m)
{
    py::enum_<LogLevel>(m, "LogLevel")
        .value("MX_LOG_LEVEL_DEBUG", LogLevel::DEBUG)
        .value("MX_LOG_LEVEL_INFO", LogLevel::INFO)
        .value("MX_LOG_LEVEL_WARNING", LogLevel::WARNING)
        .value("MX_LOG_LEVEL_ERROR", LogLevel::ERROR)
        .export_values();

    py::class_<Context>(m, "Context")
        // .def(py::init<>, []()
        //      { return std::make_shared<Context>(); })

        // .def(py::init<LogLevel>, [](LogLevel level)
            //  { return std::make_shared<Context>(level.get()); })

        .def("set_log_level", [](Context &ctx, LogLevel level)
             { ctx.log_level(level); });

    py::class_<Stats>(m, "Stats");
}

// bool mx_term(MomentumX::Context* ctx);
// bool mx_is_terminated(MomentumX::Context* ctx);
// bool mx_destroy(MomentumX::Context* ctx);
// bool mx_is_subscribed(MomentumX::Context* ctx, const char* stream_name);
// MomentumX::Stream* mx_subscribe(MomentumX::Context* ctx, const char* stream_name);
// bool mx_unsubscribe(MomentumX::Context* ctx, MomentumX::Stream* stream);
// size_t mx_subscriber_count(MomentumX::Context* ctx, MomentumX::Stream* stream);
// MomentumX::Stream* mx_stream(MomentumX::Context* ctx, const char* stream_name, size_t buffer_size, size_t buffer_count, bool sync=false);
// MomentumX::Stream::BufferState* mx_stream_next(MomentumX::Context* ctx, MomentumX::Stream* stream);
// bool mx_stream_send(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state);
// MomentumX::Stream::BufferState* mx_stream_receive(MomentumX::Context* ctx, MomentumX::Stream* stream, uint64_t minimum_ts=1);
// MomentumX::Stream::BufferState* mx_get_by_buffer_id(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id);
// void mx_stream_flush(MomentumX::Context* ctx, MomentumX::Stream* stream);
// bool mx_stream_release(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state);
// bool mx_is_stream_sync(MomentumX::Context* ctx, MomentumX::Stream* stream);
// uint8_t* mx_data_address(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id);