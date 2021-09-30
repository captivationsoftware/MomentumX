#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <vector>
#include <map>

#ifdef __cplusplus
extern "C" {
#endif

namespace Momentum {

    // static const int PAGE_SIZE = getpagesize();
    class MomentumContext {

    public:
        MomentumContext();
        ~MomentumContext();
        bool terminated();
        void term();
        void subscribe(const char *stream, const void (*handler)(const char *));
        void unsubscribe(const char *stream, const void (*handler)(const char *));
        void send(const char *stream, const char *data);

    private:
        std::map<std::string, std::vector<const void (*)(const char *)>> _consumers_by_stream;
        volatile bool _terminated = false;
        std::thread _worker;
    };

    // public interface
    MomentumContext *context();
    void term(MomentumContext *ctx);
    void destroy(MomentumContext *ctx);
    bool terminated(MomentumContext *ctx);
    void subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(const char *));
    void unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(const char *));
    void send(MomentumContext *ctx, const char *stream, const char *data);
}

#ifdef __cplusplus
}
#endif

#endif