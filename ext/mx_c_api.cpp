#include "mx_c_api.h"

#include "buffer.h"
#include "context.h"
#include "stream.h"
#include "utils.h"

#include <memory>
#include <string>
#include <unordered_map>

/* Concrete types behind the opaque pointers */

struct MxContext {
    MomentumX::Context inner;
    std::string        path_cache;  // stable storage so c_str() stays valid

    explicit MxContext(const char* path)
        : inner(path), path_cache(inner.context_path()) {}
};

using BufStatePtr = std::shared_ptr<MomentumX::Stream::BufferState>;

struct MxStream {
    MomentumX::Stream* ptr;  // non-owning; owned by the context StreamManager

    // Keeps the write-lock alive from mx_next until mx_send (mirrors Python WriteBufferShim).
    BufStatePtr write_claim;

    // Keeps each read-lock alive from mx_receive until mx_release (mirrors Python ReadBufferShim).
    std::unordered_map<uint16_t, BufStatePtr> held_reads;

    explicit MxStream(MomentumX::Stream* p) : ptr(p) {}
};

/* ---- helpers ---- */

static MxBufferState to_c(const MomentumX::BufferState& bs) {
    MxBufferState s{};
    s.buffer_id      = bs.buffer_id;
    s.buffer_size    = bs.buffer_size;
    s.buffer_count   = bs.buffer_count;
    s.data_size      = bs.data_size;
    s.data_timestamp = bs.data_timestamp;
    s.iteration      = bs.iteration;
    return s;
}

static MomentumX::BufferState from_c(const MxBufferState* s) {
    return MomentumX::BufferState(
        s->buffer_id, s->buffer_size, s->buffer_count,
        s->data_size, s->data_timestamp, s->iteration);
}

/* ---- Context ---- */

MxContext* mx_context_create(const char* context_path) {
    try {
        return new MxContext(context_path);
    } catch (...) {
        return nullptr;
    }
}

void mx_context_destroy(MxContext* ctx) {
    delete ctx;
}

const char* mx_context_path(MxContext* ctx) {
    if (!ctx) return nullptr;
    return ctx->path_cache.c_str();
}

/* ---- Streams ---- */

MxStream* mx_stream_create(MxContext* ctx,
                           const char* name,
                           size_t buffer_size,
                           size_t buffer_count,
                           bool sync) {
    if (!ctx || !name) return nullptr;
    try {
        MomentumX::Stream* s = ctx->inner.stream(name, buffer_size, buffer_count, sync);
        if (!s) return nullptr;
        return new MxStream(s);
    } catch (...) {
        return nullptr;
    }
}

MxStream* mx_stream_subscribe(MxContext* ctx, const char* name) {
    if (!ctx || !name) return nullptr;
    try {
        MomentumX::Stream* s = ctx->inner.subscribe(name);
        if (!s) return nullptr;
        return new MxStream(s);
    } catch (...) {
        return nullptr;
    }
}

void mx_stream_unsubscribe(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return;
    try {
        // 1. Release any outstanding read/write claims so checkin_read/checkin_write
        //    fire before the ControlBlock shared memory is touched by unsubscribe().
        stream->write_claim.reset();
        stream->held_reads.clear();

        // 2. Logical unsubscribe: decrements subscriber_count and removes pending
        //    sync acknowledgements.  Mirrors what Python does when a Consumer is GC'd.
        //    Producer streams throw std::logic_error here — catch it locally so step 3
        //    is always reached regardless of producer/consumer role.
        try { stream->ptr->unsubscribe(); } catch (...) {}

        // 3. Memory-level cleanup: removes the stream's data buffers from the
        //    BufferManager (does NOT unmap the ControlBlock — that happens in
        //    Stream::~Stream() when the Context is later destroyed).
        ctx->inner.unsubscribe(stream->ptr);
    } catch (...) {}
    delete stream;
}

/* ---- Producer ---- */

int mx_next(MxContext* ctx, MxStream* stream, MxBufferState* out) {
    if (!ctx || !stream || !out) return -1;
    try {
        auto bs = ctx->inner.next(stream->ptr);
        if (!bs) return 0;
        *out = to_c(*bs);
        // Keep the write-lock alive until mx_send is called.
        stream->write_claim = bs;
        return 1;
    } catch (...) {
        return -1;
    }
}

int mx_send(MxContext* ctx, MxStream* stream, const MxBufferState* state) {
    if (!ctx || !stream || !state) return -1;
    try {
        bool ok = ctx->inner.send(stream->ptr, from_c(state));
        // Release the write-lock now that the buffer has been sent.
        stream->write_claim.reset();
        return ok ? 1 : 0;
    } catch (...) {
        stream->write_claim.reset();
        return -1;
    }
}

/* ---- Consumer ---- */

bool mx_can_receive(MxContext* ctx, MxStream* stream, uint64_t min_ts) {
    if (!ctx || !stream) return false;
    try {
        return ctx->inner.can_receive(stream->ptr, min_ts);
    } catch (...) {
        return false;
    }
}

int mx_receive(MxContext* ctx, MxStream* stream, uint64_t min_ts, MxBufferState* out) {
    if (!ctx || !stream || !out) return -1;
    try {
        auto bs = ctx->inner.receive(stream->ptr, min_ts);
        if (!bs) return 0;
        *out = to_c(*bs);
        // Keep the read-lock alive until mx_release is called.
        stream->held_reads[bs->buffer_id] = bs;
        return 1;
    } catch (...) {
        return -1;
    }
}

void mx_release(MxContext* ctx, MxStream* stream, const MxBufferState* state) {
    if (!ctx || !stream || !state) return;
    try {
        // Dropping the shared_ptr triggers checkin_read in the custom deleter.
        stream->held_reads.erase(state->buffer_id);
        ctx->inner.release(stream->ptr, from_c(state));
    } catch (...) {}
}

/* ---- Data access ---- */

uint8_t* mx_data_address(MxContext* ctx, MxStream* stream, uint16_t buffer_id) {
    if (!ctx || !stream) return nullptr;
    try {
        return ctx->inner.data_address(stream->ptr, buffer_id);
    } catch (...) {
        return nullptr;
    }
}

/* ---- Metadata ---- */

size_t mx_subscriber_count(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return 0;
    try {
        return ctx->inner.subscriber_count(stream->ptr);
    } catch (...) {
        return 0;
    }
}

size_t mx_stream_buffer_size(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return 0;
    try {
        auto lock = stream->ptr->get_control_lock();
        return stream->ptr->buffer_size(lock);
    } catch (...) {
        return 0;
    }
}

size_t mx_stream_buffer_count(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return 0;
    try {
        auto lock = stream->ptr->get_control_lock();
        return stream->ptr->buffer_count(lock);
    } catch (...) {
        return 0;
    }
}

bool mx_stream_is_sync(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return false;
    try {
        auto lock = stream->ptr->get_control_lock();
        return stream->ptr->sync(lock);
    } catch (...) {
        return false;
    }
}

const char* mx_stream_name(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return nullptr;
    try {
        auto lock = stream->ptr->get_control_lock();
        // name() returns a const std::string& whose backing storage lives in
        // the memory-mapped ControlBlock, so the pointer is stable.
        return stream->ptr->name(lock).c_str();
    } catch (...) {
        return nullptr;
    }
}

void mx_end(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return;
    try {
        auto lock = stream->ptr->get_control_lock();
        stream->ptr->end(lock);
    } catch (...) {}
}

bool mx_is_ended(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return false;
    try {
        auto lock = stream->ptr->get_control_lock();
        return stream->ptr->is_ended(lock);
    } catch (...) {
        return false;
    }
}

uint64_t mx_last_sent_iteration(MxContext* ctx, MxStream* stream) {
    if (!ctx || !stream) return 0;
    try {
        auto lock = stream->ptr->get_control_lock();
        return stream->ptr->last_sent_iteration(lock);
    } catch (...) {
        return 0;
    }
}

void mx_set_log_level(MxContext* ctx, MxLogLevel level) {
    if (!ctx) return;
    MomentumX::Utils::Logger::Level l;
    switch (level) {
        case MX_LOG_DEBUG:   l = MomentumX::Utils::Logger::Level::DEBUG;   break;
        case MX_LOG_WARNING: l = MomentumX::Utils::Logger::Level::WARNING; break;
        case MX_LOG_ERROR:   l = MomentumX::Utils::Logger::Level::ERROR;   break;
        default:             l = MomentumX::Utils::Logger::Level::INFO;    break;
    }
    ctx->inner.log_level(l);
}
