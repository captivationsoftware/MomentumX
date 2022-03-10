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


typedef const void (*callback_t)(uint8_t *, size_t, size_t, uint64_t);


static const std::string PATH_DELIM = "__";
static const std::string NAMESPACE = "momentum";
static const std::string PROTOCOL = NAMESPACE + "://";
static const std::string SHM_PATH_BASE = "/dev/shm";
static const std::string DEBUG_ENV = "DEBUG";
static const std::string IPC_ENDPOINT_BASE = "ipc://@" + NAMESPACE + PATH_DELIM;
static const size_t PAGE_SIZE = getpagesize();
static const size_t MAX_STREAM_SIZE = 43; // 32 chars for stream name, and 11 for protocol (i.e. "momentum://)
static const size_t MAX_PATH_SIZE = 256; // 255 maximum linux file name + 1 for the leading slash
static const uint64_t NANOS_PER_SECOND = 1000000000;

struct Buffer {
    char shm_path[MAX_PATH_SIZE];
    int fd;
    size_t length;
    uint8_t *address;
};

struct Message {
    char stream[MAX_STREAM_SIZE];
    char buffer_shm_path[MAX_PATH_SIZE];
    size_t buffer_length;
    size_t data_length;
    uint64_t ts;
    uint64_t id;
};


class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool is_terminated();
    bool is_streaming(std::string stream);
    void term();
    int subscribe(std::string stream, callback_t callback);
    int unsubscribe(std::string stream, callback_t callback);
    int send_data(std::string stream, uint8_t *data, size_t length, uint64_t ts=0);
    int send_buffer(Buffer * buffer, size_t length, uint64_t ts=0);
    Buffer *acquire_buffer(std::string stream, size_t length);
    void release_buffer(Buffer *buffer);

    // public options
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

    Buffer *allocate_buffer(std::string shm_path, size_t length, int flags);
    void resize_buffer(Buffer *buffer, size_t length, bool truncate=false);
    void deallocate_buffer(Buffer *buffer);
    std::string to_shm_path(pid_t pid, std::string stream, uint64_t id);
    void shm_iter(std::function<void(std::string)> callback);
    void update_shm_time(std::string shm_path, uint64_t ts);
    uint64_t now();
    std::string stream_from_shm_path(std::string shm_path);
    bool is_valid_stream(std::string stream);

    void *_zmq_ctx = NULL;
    void *_producer_sock = NULL;
    void *_consumer_sock = NULL;

    uint64_t _msg_id = 0;

    bool _debug;
};

extern "C" {

    extern const uint8_t MOMENTUM_OPT_MIN_BUFFERS = 1;

    // public interface
    MomentumContext* momentum_context();
    void momentum_term(MomentumContext *ctx);
    void momentum_destroy(MomentumContext *ctx);
    bool momentum_terminated(MomentumContext *ctx);
    int momentum_subscribe(MomentumContext *ctx, const char *stream, callback_t callback);
    int momentum_unsubscribe(MomentumContext *ctx, const char *stream, callback_t callback);
    Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length);
    void momentum_release_buffer(MomentumContext *ctx, Buffer *buffer);
    int momentum_send_buffer(MomentumContext *ctx, Buffer * buffer, size_t data_length, uint64_t ts);
    int momentum_send_data(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length, uint64_t ts);
    uint8_t* momentum_get_buffer_address(Buffer *buffer);
    size_t momentum_get_buffer_length(Buffer *buffer);
    void momentum_configure(MomentumContext *ctx, uint8_t option, const void *value);
}

#endif