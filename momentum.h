#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <vector>
#include <map>

extern "C" {

namespace Momentum {


    static const size_t PAGE_SIZE = getpagesize();
    
    struct mmap_t {
        uint8_t *address;
        size_t length;
    };

    class MomentumContext {

    public:
        MomentumContext(const char *name);
        ~MomentumContext();
        bool terminated();
        void term();
        void subscribe(const char *stream, const void (*handler)(const char *));
        void unsubscribe(const char *stream, const void (*handler)(const char *));
        void send(const char *stream, const uint8_t *data, size_t length);


    private:
        const char *_name;
        std::map<const char *, std::vector<const void (*)(const char *)>> _consumers_by_stream;
        std::map<const char *, mmap_t> _mmap_by_shm_path;
        volatile bool _terminated = false;
        std::thread _worker;
    };

    // public interface
    MomentumContext *context(const char *name);
    void term(MomentumContext *ctx);
    void destroy(MomentumContext *ctx);
    bool terminated(MomentumContext *ctx);
    void subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(const char *));
    void unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(const char *));
    void send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length);

}

}

#endif