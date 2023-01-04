#ifndef MOMENTUMX_H
#define MOMENTUMX_H

#include "context.h"
#include "utils.h"

extern "C" {

    static const uint8_t MX_LOG_LEVEL_DEBUG = static_cast<uint8_t>(MomentumX::Utils::Logger::Level::DEBUG);
    static const uint8_t MX_LOG_LEVEL_INFO = static_cast<uint8_t>(MomentumX::Utils::Logger::Level::INFO);
    static const uint8_t MX_LOG_LEVEL_WARNING = static_cast<uint8_t>(MomentumX::Utils::Logger::Level::WARNING);
    static const uint8_t MX_LOG_LEVEL_ERROR = static_cast<uint8_t>(MomentumX::Utils::Logger::Level::ERROR);

    MomentumX::Context* mx_context();
    void mx_log_level(MomentumX::Context* ctx, uint8_t log_level);
    bool mx_term(MomentumX::Context* ctx);
    bool mx_is_terminated(MomentumX::Context* ctx);
    bool mx_destroy(MomentumX::Context* ctx);
    bool mx_is_subscribed(MomentumX::Context* ctx, const char* stream_name);
    MomentumX::Stream* mx_subscribe(MomentumX::Context* ctx, const char* stream_name);
    bool mx_unsubscribe(MomentumX::Context* ctx, MomentumX::Stream* stream);
    size_t mx_subscriber_count(MomentumX::Context* ctx, MomentumX::Stream* stream);
    MomentumX::Stream* mx_stream(MomentumX::Context* ctx, const char* stream_name, size_t buffer_size, size_t buffer_count, bool sync=false);
    MomentumX::Stream::BufferState* mx_stream_next(MomentumX::Context* ctx, MomentumX::Stream* stream);
    bool mx_stream_send(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state);
    MomentumX::Stream::BufferState* mx_stream_receive(MomentumX::Context* ctx, MomentumX::Stream* stream, uint64_t minimum_ts=1);
    MomentumX::Stream::BufferState* mx_get_by_buffer_id(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id);
    void mx_stream_flush(MomentumX::Context* ctx, MomentumX::Stream* stream);
    bool mx_stream_release(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state);
    bool mx_is_stream_sync(MomentumX::Context* ctx, MomentumX::Stream* stream);
    uint8_t* mx_data_address(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id);
}


#endif