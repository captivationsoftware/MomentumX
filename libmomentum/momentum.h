#ifndef MOMENTUM_H
#define MOMENTUM_H


#include <thread>
#include <sys/socket.h>
#include <sys/un.h>

#ifdef __cplusplus
extern "C" {
#endif

// static const int PAGE_SIZE = getpagesize();

class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool is_terminated();
    void term();

private:
    bool terminated;

};

MomentumContext::MomentumContext() {
    terminated = false;
    std::cout << "created momentum context" << std::endl;
}


MomentumContext::~MomentumContext() {
    terminated = true;
    std::cout << "destroyed momemntum context" << std::endl;

}

bool MomentumContext::is_terminated() {
    return terminated;
}

void MomentumContext::term() {
    terminated = true;
};



MomentumContext* context();

void term(MomentumContext* ctx);
void destroy(MomentumContext* ctx);
bool is_terminated(MomentumContext* ctx);

#ifdef __cplusplus
}
#endif

#endif