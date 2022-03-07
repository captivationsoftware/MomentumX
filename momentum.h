#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>
#include <atomic>
#include <sys/types.h>
#include <zmq.h>


typedef const void (*callback_t)(uint8_t *, size_t, size_t, uint64_t, uint64_t);


static const std::string PATH_DELIM = "__";
static const std::string NAMESPACE = "momentum";
static const std::string SHM_PATH_BASE = "/dev/shm";
static const std::string IPC_ENDPOINT_BASE = "ipc://@" + NAMESPACE + PATH_DELIM;
static const size_t PAGE_SIZE = getpagesize();
static const size_t MAX_STREAM_SIZE = 32;
static const size_t MAX_PATH_SIZE = 40;

struct Buffer {
    int id;
    pid_t owner_pid;
    int fd;
    size_t length;
    uint8_t *address;
};

struct Message {
    char stream[MAX_STREAM_SIZE];
    int buffer_id;
    size_t buffer_length;
    pid_t buffer_owner_pid;
    size_t data_length;
    uint64_t ts;
    uint64_t id;
};


class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool terminated();
    void term();
    int subscribe(std::string stream, callback_t callback);
    int unsubscribe(std::string stream, callback_t callback);
    int send_data(std::string stream, uint8_t *data, size_t length);
    int send_buffer(std::string stream, Buffer * buffer, size_t length);
    Buffer *acquire_buffer(std::string stream, size_t length);
    void release_buffer(std::string stream, Buffer *buffer);

    // public options
    uint64_t _max_latency = -1;             // intentionally wrap
    uint64_t _max_byte_allocations = -1;    // intentionally wrap
    uint64_t _min_buffers = 1;


private:
    std::atomic<bool>  _terminated{false};    

    std::map<std::string, std::vector<callback_t>> _consumers_by_stream;
    std::map<std::string, std::queue<Buffer *>> _buffers_by_stream;
    
    Buffer *_last_acquired_buffer = NULL;

    pid_t _pid;

    std::set<std::string> _producer_streams;
    std::set<std::string> _consumer_streams;

    std::map<std::string, Buffer *> _buffer_by_shm_path;
    std::mutex _buffer_by_shm_path_mutex;

    Buffer *allocate_buffer(std::string stream, int id, size_t length, int flags, pid_t owner_pid=-1);
    void resize_buffer(Buffer * buffer, size_t length);

    std::string to_shm_path(pid_t pid, std::string stream, int id);
    void update_shm_time(std::string shm_path);
    
    void *_zmq_ctx = NULL;
    void *_producer_sock = NULL;
    void *_consumer_sock = NULL;

    uint64_t _msg_id = 0;
};

extern "C" {

    extern const uint8_t MOMENTUM_OPT_MAX_LATENCY = 10;
    extern const uint8_t MOMENTUM_OPT_MIN_BUFFERS = 20;

    // public interface
    MomentumContext* momentum_context();
    void momentum_term(MomentumContext *ctx);
    void momentum_destroy(MomentumContext *ctx);
    bool momentum_terminated(MomentumContext *ctx);
    int momentum_subscribe(MomentumContext *ctx, const char *stream, callback_t callback);
    int momentum_unsubscribe(MomentumContext *ctx, const char *stream, callback_t callback);
    int momentum_send_data(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length);
    int momentum_send_buffer(MomentumContext *ctx, const char *stream, Buffer *buffer, size_t length);
    Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length);
    void momentum_release_buffer(MomentumContext *ctx, Buffer * buffer);
    uint8_t* momentum_get_buffer_address(Buffer *buffer);
    size_t momentum_get_buffer_length(Buffer *buffer);
    void momentum_configure(MomentumContext *ctx, uint8_t option, const void *value);
}

#endif