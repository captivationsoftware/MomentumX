#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>
#include <atomic>

static const size_t PAGE_SIZE = getpagesize();

static const std::string DEV_SHM_PATH = "/dev/shm/";

static const size_t SIZE_T_SIZE = sizeof(size_t);

struct Buffer {
    int fd;
    std::string path;
    size_t length;
    bool readonly;
    uint8_t *address;
};

class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool terminated();
    void term();
    int subscribe(std::string stream, const void (*handler)(uint8_t *, size_t));
    int unsubscribe(std::string stream, const void (*handler)(uint8_t *, size_t));
    int send(std::string stream, uint8_t *data, size_t length);
    Buffer *acquire_buffer(std::string stream, size_t length);
    void release_buffer(Buffer *buffer);

private:
    std::atomic<bool>  _terminated{false};    

    std::map<std::string, std::vector<const void (*)(uint8_t *, size_t)>> _consumers_by_stream;
    std::map<std::string, std::queue<Buffer *>> _buffers_by_stream;
    
    Buffer *_last_acquired_buffer = NULL;

    std::set<std::string> _consumer_streams;
    std::mutex _consumer_streams_mutex;

    std::map<std::string, Buffer *> _buffer_by_shm_path;
    std::mutex _buffer_by_shm_path_mutex;

    Buffer *allocate_buffer(std::string shm_path, size_t length, bool fail_on_exists);
    void resize_buffer(Buffer * buffer, size_t length);
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
    Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length);
    void momentum_release_buffer(MomentumContext *ctx, Buffer * buffer);
    uint8_t* momentum_buffer_data(MomentumContext *ctx, Buffer *buffer);
}

#endif