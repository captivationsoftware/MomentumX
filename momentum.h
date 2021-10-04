#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>



static const size_t PAGE_SIZE = getpagesize();

struct Buffer {
    int fd;
    std::string path;
    uint8_t *address;
    size_t length;
};

class MomentumContext {

public:
    MomentumContext(std::string name);
    ~MomentumContext();
    bool terminated();
    void term();
    int subscribe(std::string stream, const void (*handler)(uint8_t *));
    int unsubscribe(std::string stream, const void (*handler)(uint8_t *));
    int send(std::string stream, const uint8_t *data, size_t length);
    Buffer *acquire_buffer(std::string stream, size_t length);


private:
    std::string _name;
    std::map<std::string, std::vector<const void (*)(uint8_t *)>> _consumers_by_stream;
    std::map<std::string, std::queue<Buffer *>> _buffers_by_stream;
    std::set<std::string> _consumer_streams;
    std::set<std::string> _producer_streams; 
    std::vector<Buffer *> _consumer_buffers; 
    std::vector<Buffer *> _producer_buffers;
    volatile bool _terminated = false;
    Buffer *_last_acquired_buffer = NULL;
    std::thread _watcher_thread;
    std::mutex _mutex;
};

extern "C" {

    // public interface
    MomentumContext *momentum_context(const char *name);
    void momentum_term(MomentumContext *ctx);
    void momentum_destroy(MomentumContext *ctx);
    bool momentum_terminated(MomentumContext *ctx);
    int momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    int momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    int momentum_send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length);
    uint8_t* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length);

}

#endif