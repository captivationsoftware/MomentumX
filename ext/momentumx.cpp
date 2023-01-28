#include "momentumx.h"

MomentumX::Context* mx_context() {
    return new MomentumX::Context("/dev/shm");
}

void mx_log_level(MomentumX::Context* ctx, uint8_t log_level) {
    ctx->log_level(
        static_cast<MomentumX::Utils::Logger::Level>(log_level)
    );
}

bool mx_term(MomentumX::Context* ctx) { 
    try {
        ctx->term();
        return true;
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return false;
    }
}

bool mx_is_terminated(MomentumX::Context* ctx) {
    return ctx->is_terminated();
}

bool mx_destroy(MomentumX::Context* ctx) {
    if (ctx != NULL) {
        delete ctx;
        return true;
    } else {
        return false;
    }
}

bool mx_is_subscribed(MomentumX::Context* ctx, const char* stream_name) {
    return ctx->is_subscribed(std::string(stream_name));
}

MomentumX::Stream* mx_subscribe(MomentumX::Context* ctx, const char* stream_name) {
    try {
        return ctx->subscribe(std::string(stream_name));
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
}

bool mx_unsubscribe(MomentumX::Context* ctx, MomentumX::Stream* stream) {
    try {
        ctx->unsubscribe(stream);
        return true;
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return false;
    }
}

size_t mx_subscriber_count(MomentumX::Context* ctx, MomentumX::Stream* stream) {
    return ctx->subscriber_count(stream);
}

MomentumX::Stream* mx_stream(MomentumX::Context* ctx, const char* stream_name, size_t buffer_size, size_t buffer_count, bool sync) {
    try {
        return ctx->stream(std::string(stream_name), buffer_size, buffer_count, sync);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
}

MomentumX::Stream::BufferState* mx_stream_next(MomentumX::Context* ctx, MomentumX::Stream* stream) {
    try {
        return ctx->next(stream);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
    
}

bool mx_stream_send(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state) {
    try {
        return ctx->send(stream, buffer_state);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return false;
    }
}

MomentumX::Stream::BufferState* mx_stream_receive(MomentumX::Context* ctx, MomentumX::Stream* stream, uint64_t minimum_timestamp) {
    try {
        return ctx->receive(stream, minimum_timestamp);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
}

MomentumX::Stream::BufferState* mx_get_by_buffer_id(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id) {
    try {
        return ctx->get_by_buffer_id(stream, buffer_id);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
}

void mx_stream_flush(MomentumX::Context* ctx, MomentumX::Stream* stream) {
    try {
        return ctx->flush(stream);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
    }
}


bool mx_stream_release(MomentumX::Context* ctx, MomentumX::Stream* stream, MomentumX::Stream::BufferState* buffer_state) {
    try {
        ctx->release(stream, buffer_state);
        return true;
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return false;
    }
}

bool mx_is_stream_sync(MomentumX::Context* ctx, MomentumX::Stream* stream) {
    return stream->sync();
}


uint8_t* mx_data_address(MomentumX::Context* ctx, MomentumX::Stream* stream, uint16_t buffer_id) {
    try {
        return ctx->data_address(stream, buffer_id);
    } catch (std::exception& ex) {
        MomentumX::Utils::Logger::get_logger().error(ex.what());
        return NULL;
    }
}