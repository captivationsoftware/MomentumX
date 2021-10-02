#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <string>
#include <vector>
#include <map>


static const size_t PAGE_SIZE = getpagesize();

struct mmap_t {
    uint8_t *address;
    size_t length;
};

class MomentumContext {

public:
    MomentumContext(std::string name);
    ~MomentumContext();
    bool terminated();
    void term();
    void subscribe(std::string stream, const void (*handler)(uint8_t *));
    void unsubscribe(std::string stream, const void (*handler)(uint8_t *));
    void send(std::string stream, const uint8_t *data, size_t length);


private:
    std::string _name;
    std::map<std::string, std::vector<const void (*)(uint8_t *)>> _consumers_by_stream;
    std::map<std::string, mmap_t> _mmap_by_shm_path;
    volatile bool _terminated = false;
    std::thread _worker;
};

extern "C" {

    // public interface
    MomentumContext *momentum_context(const char *name);
    void momentum_term(MomentumContext *ctx);
    void momentum_destroy(MomentumContext *ctx);
    bool momentum_terminated(MomentumContext *ctx);
    void momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    void momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    void momentum_send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length);

}

#endif