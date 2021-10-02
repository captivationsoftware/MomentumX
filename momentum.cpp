#include <algorithm>
#include <iostream>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <sys/shm.h>
#include <csignal>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>        
#include <fcntl.h> 
#include <sstream>  
#include "momentum.h"


static std::vector<MomentumContext *> contexts;

static void (*previous_sigint_handler)(int) = NULL;

void signal_handler(int signal) {
    for (MomentumContext *ctx : contexts) {
        momentum_term(ctx);
        momentum_destroy(ctx);
    }

    if (previous_sigint_handler) {
        previous_sigint_handler(signal);
    }
};

MomentumContext::MomentumContext(std::string name) {
    _name = name;

    if (!previous_sigint_handler) {
        previous_sigint_handler = std::signal(SIGINT, signal_handler);
    }

    contexts.push_back(this);

    _worker = std::thread([&] {
        // int _epollfd = epoll_create1(0);
        
        while (!_terminated) {
            usleep(1);    
        }
        
        // close(_epollfd);
    });

    _worker.detach();
}


MomentumContext::~MomentumContext() {
    term();

    // close shared memory
    for (auto const& tuple : _mmap_by_shm_path) {
        std::string shm_path = tuple.first;
        mmap_t mem = tuple.second;
        shm_unlink(shm_path.c_str());
        munmap(mem.address, mem.length);
    }
}

bool MomentumContext::terminated() {
    return _terminated;
}

void MomentumContext::term() {
    _terminated = true;
};

void MomentumContext::subscribe(std::string stream, const void (*handler)(uint8_t *)) {
    if (!_consumers_by_stream.count(stream)) {
        _consumers_by_stream[stream] = std::vector<const void (*)(uint8_t *)>();
    }
    _consumers_by_stream[stream].push_back(handler);
}

void MomentumContext::unsubscribe(std::string stream, const void (*handler)(uint8_t *)) {
    if (_consumers_by_stream.count(stream)) {
        _consumers_by_stream[stream].erase(
            std::remove(
                _consumers_by_stream[stream].begin(), 
                _consumers_by_stream[stream].end(), 
                handler
            ), 
            _consumers_by_stream[stream].end()
        );
    }
}

void MomentumContext::send(std::string stream, const uint8_t *data, size_t length) {
    std::string shm_path("/" + stream);
    
    size_t mmap_length = (length / PAGE_SIZE + 1) * PAGE_SIZE;
    
    mmap_t mem;

    // create the mmap file if not exists
    if (!_mmap_by_shm_path.count(shm_path)) {
        int shm_fd;
        
        shm_fd = shm_open(shm_path.c_str(), O_CREAT | O_RDWR, S_IRWXU);
        if (shm_fd < 0) {
            std::perror(strerror(errno));
        } else {
            ftruncate(shm_fd, mmap_length);
            mem.length = mmap_length;
            mem.address = (uint8_t *) mmap(NULL, mmap_length, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
            _mmap_by_shm_path[shm_path] = mem;

            // done with the file descriptor                
            close(shm_fd);
        }
    }

    // resize the mmap file if necessary
    if (_mmap_by_shm_path.count(shm_path)) {
        mem = _mmap_by_shm_path[shm_path];

        if (mmap_length > mem.length) {
            mem.address = (uint8_t *) mremap(mem.address, mem.length, mmap_length, 0);
            mem.length = mmap_length;
        }

        memcpy(mem.address, data, length);
    }
}

MomentumContext* momentum_context(const char *name) {
    std::string name_str(name);

    MomentumContext* ctx = new MomentumContext(name_str);
    return ctx;
}

void momentum_term(MomentumContext* ctx) {
    ctx->term();
}

bool momentum_terminated(MomentumContext *ctx) {
    return ctx->terminated();
}

void momentum_destroy(MomentumContext *ctx) {
    delete ctx;
}

void momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *)) {
    std::string stream_str(stream);
    ctx->subscribe(stream_str, handler);
}

void momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *)) {
    std::string stream_str(stream);
    ctx->unsubscribe(stream_str, handler);
}

void momentum_send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length) {
    std::string stream_str(stream);
    ctx->send(stream_str, data, length);
}

// void producer() {  

//     struct sockaddr_un addr;

//     int producer_sock, consumer_sock;
    
//     producer_sock = guard(socket(AF_UNIX, SOCK_STREAM, 0));

//     memset(&addr, 0, sizeof(addr));
//     addr.sun_family = AF_UNIX;
//     // *addr.sun_path = '\0';
//     // strncpy(addr.sun_path + 1, "foo", sizeof(addr.sun_path) - 2);
//     // *addr.sun_path = '\0';
//     strncpy(addr.sun_path, "/tmp/foo", sizeof(addr.sun_path) - 1);


//     guard(bind(producer_sock, (struct sockaddr*) &addr, sizeof(addr)));

//     guard(listen(producer_sock, 1024));

//     cout << "Listening..." << endl;

//     while(1) {
//         consumer_sock = accept(producer_sock, nullptr, 0);
//         if (consumer_sock < 0) {
//             continue;
//         }

//         char buffer[PAGE_SIZE * 10];
//         long bytes_received = 0;
//         long message_count = 0;

//         auto start = chrono::steady_clock::now();

//         int rc;
//         while ((rc = recv(consumer_sock, &buffer, sizeof(buffer), 0)) > 0) {
//             message_count += 1;
//             bytes_received += rc;

//             if (message_count % 1000 == 0) {    
//                 auto end = chrono::steady_clock::now();
//                 chrono::duration<double> elapsed_seconds = end - start;
//                 cout << "Recvd " << message_count / elapsed_seconds.count() << " msgs/sec" << endl;
//                 cout << "Recvd " << bytes_received / elapsed_seconds.count() / 1e6 << " MB/sec" << endl;
//             }
//         }
//     }

// }


// void consumer() {

//     struct sockaddr_un addr;
    
//     char buffer[PAGE_SIZE * 10];

//     int consumer_sock;

//     consumer_sock = guard(socket(AF_UNIX, SOCK_STREAM, 0));
    
//     addr.sun_family = AF_UNIX;
//     // *addr.sun_path = '\0';
//     // strncpy(addr.sun_path + 1, "foo", sizeof(addr.sun_path) - 2);
//     strncpy(addr.sun_path, "/tmp/foo", sizeof(addr.sun_path) - 1);
    
//     guard(connect(consumer_sock, (struct sockaddr*) &addr, sizeof(addr)));

//     while (1) {
//         // rc = sendfile(producer_sock, consumer_sock, 0, sizeof(buffer));
//         int rc = send(consumer_sock, &buffer, sizeof(buffer), 0);
//         cout << rc;
//     }
// }

// void launch() {
//     if (fork() == 0) {
//         producer();
//     } else {
//         consumer();
//     }
// }


