#ifndef MOMENTUMX_C_API_H
#define MOMENTUMX_C_API_H

#include <stddef.h>
#include <stdint.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Opaque handle types */
typedef struct MxContext MxContext;
typedef struct MxStream MxStream;

/* Mirror of MomentumX::BufferState - trivially copyable */
typedef struct {
    uint16_t buffer_id;
    size_t   buffer_size;
    size_t   buffer_count;
    size_t   data_size;
    uint64_t data_timestamp;
    uint64_t iteration;
} MxBufferState;

/* Log level matching MomentumX::Utils::Logger::Level */
typedef enum {
    MX_LOG_DEBUG   = 0,
    MX_LOG_INFO    = 1,
    MX_LOG_WARNING = 2,
    MX_LOG_ERROR   = 3,
} MxLogLevel;

/*
 * Context lifecycle
 *
 * mx_context_create: returns NULL on failure.
 * mx_context_destroy: frees the context; all streams must already be freed.
 */
MxContext* mx_context_create(const char* context_path);
void       mx_context_destroy(MxContext* ctx);
const char* mx_context_path(MxContext* ctx);

/*
 * Stream creation / subscription
 *
 * mx_stream_create: create a producer stream. Returns NULL on failure.
 * mx_stream_subscribe: subscribe as a consumer. Returns NULL on failure.
 * mx_stream_unsubscribe: unsubscribe and free the MxStream handle.
 */
MxStream* mx_stream_create(MxContext* ctx,
                           const char* name,
                           size_t buffer_size,
                           size_t buffer_count,
                           bool sync);
MxStream* mx_stream_subscribe(MxContext* ctx, const char* name);
void      mx_stream_unsubscribe(MxContext* ctx, MxStream* stream);

/*
 * Producer operations
 *
 * mx_next: get the next writable buffer. Returns 1 on success, 0 if none
 *          available, -1 on error.
 * mx_send: send a buffer state. Returns 1 on success, 0 if not sent, -1 on
 *          error.
 */
int mx_next(MxContext* ctx, MxStream* stream, MxBufferState* out);
int mx_send(MxContext* ctx, MxStream* stream, const MxBufferState* state);

/*
 * Consumer operations
 *
 * mx_can_receive: returns true if a buffer is ready to receive.
 * mx_receive: receive the next buffer state. Returns 1 on success, 0 if none
 *             available, -1 on error.
 * mx_release: release a previously received buffer.
 */
bool mx_can_receive(MxContext* ctx, MxStream* stream, uint64_t min_ts);
int  mx_receive(MxContext* ctx, MxStream* stream, uint64_t min_ts, MxBufferState* out);
void mx_release(MxContext* ctx, MxStream* stream, const MxBufferState* state);

/*
 * Shared-memory data access
 *
 * mx_data_address: returns a pointer into the shared-memory region for the
 *                  given buffer. Valid until the buffer is released / sent.
 *                  Returns NULL if the buffer cannot be found.
 */
uint8_t* mx_data_address(MxContext* ctx, MxStream* stream, uint16_t buffer_id);

/* Stream metadata */
size_t      mx_subscriber_count(MxContext* ctx, MxStream* stream);
size_t      mx_stream_buffer_size(MxContext* ctx, MxStream* stream);
size_t      mx_stream_buffer_count(MxContext* ctx, MxStream* stream);
bool        mx_stream_is_sync(MxContext* ctx, MxStream* stream);
const char* mx_stream_name(MxContext* ctx, MxStream* stream);

/*
 * Stream lifecycle — producer side
 *
 * mx_end: mark the stream as ended (no more data will be sent).
 * mx_is_ended: returns true once end() has been called.
 * mx_last_sent_iteration: monotonically increasing iteration number of the
 *                         last buffer that was sent on this stream.
 */
void     mx_end(MxContext* ctx, MxStream* stream);
bool     mx_is_ended(MxContext* ctx, MxStream* stream);
uint64_t mx_last_sent_iteration(MxContext* ctx, MxStream* stream);

void mx_set_log_level(MxContext* ctx, MxLogLevel level);

#ifdef __cplusplus
}
#endif

#endif /* MOMENTUMX_C_API_H */
