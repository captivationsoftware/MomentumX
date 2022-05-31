#ifndef MOMENTUM_H
#define MOMENTUM_H

#include "context.h"

extern "C" {

    Momentum::Context* momentum_context();
    void momentum_debug(Momentum::Context* ctx, bool value);
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
    void momentum_stream_flush(Momentum::Context* ctx, Momentum::Stream* stream);
    bool momentum_stream_release(Momentum::Context* ctx, Momentum::Stream* stream, Momentum::Stream::BufferState* buffer_state);
    uint8_t* momentum_data_address(Momentum::Context* ctx, Momentum::Stream* stream, uint16_t buffer_id);
}


#endif