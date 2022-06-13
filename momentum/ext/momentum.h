#ifndef MOMENTUM_H
#define MOMENTUM_H

#include "context.h"
#include "utils.h"

extern "C" {

    extern const uint8_t MOMENTUM_LOG_LEVEL_DEBUG = static_cast<uint8_t>(Momentum::Utils::Logger::Level::DEBUG);
    extern const uint8_t MOMENTUM_LOG_LEVEL_INFO = static_cast<uint8_t>(Momentum::Utils::Logger::Level::INFO);
    extern const uint8_t MOMENTUM_LOG_LEVEL_WARNING = static_cast<uint8_t>(Momentum::Utils::Logger::Level::WARNING);
    extern const uint8_t MOMENTUM_LOG_LEVEL_ERROR = static_cast<uint8_t>(Momentum::Utils::Logger::Level::ERROR);

    Momentum::Context* momentum_context(uint8_t log_level=static_cast<uint8_t>(Momentum::Utils::Logger::Level::WARNING));
    void momentum_log_level(Momentum::Context* ctx, uint8_t log_level);
    bool momentum_term(Momentum::Context* ctx);
    bool momentum_is_terminated(Momentum::Context* ctx);
    bool momentum_destroy(Momentum::Context* ctx);
    bool momentum_is_subscribed(Momentum::Context* ctx, const char* stream_name);
    Momentum::Stream* momentum_subscribe(Momentum::Context* ctx, const char* stream_name);
    bool momentum_unsubscribe(Momentum::Context* ctx, Momentum::Stream* stream);
    size_t momentum_subscriber_count(Momentum::Context* ctx, Momentum::Stream* stream);
    Momentum::Stream* momentum_stream(Momentum::Context* ctx, const char* stream_name, size_t buffer_size, size_t buffer_count, bool sync=false);
    Momentum::Stream::BufferState* momentum_stream_next(Momentum::Context* ctx, Momentum::Stream* stream);
    bool momentum_stream_send(Momentum::Context* ctx, Momentum::Stream* stream, Momentum::Stream::BufferState* buffer_state);
    Momentum::Stream::BufferState* momentum_stream_receive(Momentum::Context* ctx, Momentum::Stream* stream, uint64_t minimum_ts=1);
    Momentum::Stream::BufferState* momentum_get_by_buffer_id(Momentum::Context* ctx, Momentum::Stream* stream, uint16_t buffer_id);
    void momentum_stream_flush(Momentum::Context* ctx, Momentum::Stream* stream);
    bool momentum_stream_release(Momentum::Context* ctx, Momentum::Stream* stream, Momentum::Stream::BufferState* buffer_state);
    bool momentum_is_stream_sync(Momentum::Context* ctx, Momentum::Stream* stream);
    uint8_t* momentum_data_address(Momentum::Context* ctx, Momentum::Stream* stream, uint16_t buffer_id);
}


#endif