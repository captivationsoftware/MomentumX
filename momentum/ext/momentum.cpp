#include "momentum.h"

Momentum::Context* momentum_context() {
    Momentum::Context* ctx = new Momentum::Context();
    return ctx;
}

void momentum_debug(Momentum::Context* ctx, bool value) {
    // return ctx->debug(value);
}

bool momentum_term(Momentum::Context* ctx) { 
    try {
        ctx->term();
        return true;
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return false;
    }
}

bool momentum_is_terminated(Momentum::Context* ctx) {
    return ctx->is_terminated();
}

bool momentum_destroy(Momentum::Context* ctx) {
    if (ctx != NULL) {
        delete ctx;
        return true;
    } else {
        return false;
    }
}

bool momentum_is_subscribed(Momentum::Context* ctx, const char* stream_name) {
    return ctx->is_subscribed(std::string(stream_name));
}

Momentum::Stream* momentum_subscribe(Momentum::Context* ctx, const char* stream_name) {
    try {
        return ctx->subscribe(std::string(stream_name));
    } catch(std::string ex) {
        std::cerr << ex << std::endl;
        return NULL;
    }
}

bool momentum_unsubscribe(Momentum::Context* ctx, Momentum::Stream* stream) {
    try {
        ctx->unsubscribe(stream);
        return true;
    } catch(std::string ex) {
        std::cerr << ex << std::endl;
        return false;
    }
}

size_t momentum_subscriber_count(Momentum::Context* ctx, Momentum::Stream* stream) {
    return ctx->subscriber_count(stream);
}

Momentum::Stream* momentum_stream(Momentum::Context* ctx, const char* stream_name, size_t buffer_size, size_t buffer_count, bool sync) {
    try {
        return ctx->stream(std::string(stream_name), buffer_size, buffer_count, sync);
    } catch(std::string ex) {
        std::cerr << ex << std::endl;
        return NULL;
    }
}

Momentum::Stream::BufferState* momentum_stream_next(Momentum::Context* ctx, Momentum::Stream* stream) {
    try {
        return ctx->next(stream);
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return NULL;
    }
    
}

bool momentum_stream_send(Momentum::Context* ctx, Momentum::Stream* stream, Momentum::Stream::BufferState* buffer_state) {
    try {
        return ctx->send(stream, buffer_state);
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return false;
    }
}

Momentum::Stream::BufferState* momentum_stream_receive(Momentum::Context* ctx, Momentum::Stream* stream, uint64_t minimum_timestamp) {
    try {
        return ctx->receive(stream, minimum_timestamp);
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return NULL;
    }
}

void momentum_stream_flush(Momentum::Context* ctx, Momentum::Stream* stream) {
    try {
        return ctx->flush(stream);
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
    }
}


bool momentum_stream_release(Momentum::Context* ctx, Momentum::Stream* stream, Momentum::Stream::BufferState* buffer_state) {
    try {
        ctx->release(stream, buffer_state);
        return true;
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return false;
    }
}

uint8_t* momentum_data_address(Momentum::Context* ctx, Momentum::Stream* stream, uint16_t buffer_id) {
    try {
        return ctx->data_address(stream, buffer_id);
    } catch (std::string ex) {
        std::cerr << ex << std::endl;
        return NULL;
    }
}