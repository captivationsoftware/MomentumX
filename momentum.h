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
    MomentumContext *Momentum_context(const char *name);
    void Momentum_term(MomentumContext *ctx);
    void Momentum_destroy(MomentumContext *ctx);
    bool Momentum_terminated(MomentumContext *ctx);
    void Momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    void Momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *));
    void Momentum_send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length);

}

#endif