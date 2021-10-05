#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>
#include <atomic>

#include <zmq.h>


static const std::string NAMESPACE = "momentum";
static const size_t PAGE_SIZE = getpagesize();
static const std::string DEV_SHM_PATH = "/dev/shm/";
static const std::string IPC_ENDPOINT_BASE = "ipc://@" + NAMESPACE + "_";
static const size_t MAX_STREAM_SIZE = 64;
static const size_t MAX_PATH_SIZE = 72;

struct Buffer {
    int fd;
    size_t length;
    bool readonly;
    char path[MAX_PATH_SIZE];
    uint8_t *address;
};

struct Message {
    char stream[MAX_STREAM_SIZE];
    char path[MAX_PATH_SIZE];
    size_t data_length;
    size_t buffer_length;
};

class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool terminated();
    void term();
    int subscribe(std::string stream, const void (*handler)(uint8_t *, size_t));
    int unsubscribe(std::string stream, const void (*handler)(uint8_t *, size_t));
    // int send(std::string stream, Buffer *buffer, size_t length);
    int send(std::string stream, uint8_t *data, size_t length);
    Buffer *acquire_buffer(std::string stream, size_t length);
    void release_buffer(Buffer *buffer);

private:
    std::atomic<bool>  _terminated{false};    

    std::map<std::string, std::vector<const void (*)(uint8_t *, size_t)>> _consumers_by_stream;
    std::map<std::string, std::queue<Buffer *>> _buffers_by_stream;
    
    Buffer *_last_acquired_buffer = NULL;

    std::set<std::string> _producer_streams;
    std::set<std::string> _consumer_streams;

    std::map<std::string, Buffer *> _buffer_by_shm_path;
    std::mutex _buffer_by_shm_path_mutex;

    Buffer *allocate_buffer(std::string shm_path, size_t length, bool fail_on_exists);
    void resize_buffer(Buffer * buffer, size_t length);
    int send_message(std::string stream, Buffer * buffer, size_t length);


    void *_zmq_ctx = NULL;
    void *_producer_sock = NULL;
    void *_consumer_sock = NULL;
};

extern "C" {

    // public interface
    MomentumContext* momentum_context();
    void momentum_term(MomentumContext *ctx);
    void momentum_destroy(MomentumContext *ctx);
    bool momentum_terminated(MomentumContext *ctx);
    int momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *, size_t));
    int momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *, size_t));
    int momentum_send(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length);
    // int momentum_send(MomentumContext *ctx, const char *stream, Buffer *buffer, size_t length);
    Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length);
    void momentum_release_buffer(MomentumContext *ctx, Buffer * buffer);
}

#endif