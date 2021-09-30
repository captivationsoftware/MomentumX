#include <iostream>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <csignal>
#include <thread>

#include "momentum.h"

namespace Momentum {

    static std::vector<MomentumContext *> contexts;

    static void (*previous_sigint_handler)(int) = NULL;

    void signal_handler(int signal) {
        for (MomentumContext *ctx : contexts) {
            term(ctx);
            destroy(ctx);
        }

        if (previous_sigint_handler) {
            previous_sigint_handler(signal);
        }
    };

    MomentumContext::MomentumContext() {
        if (!previous_sigint_handler) {
            previous_sigint_handler = std::signal(SIGINT, signal_handler);
        }

        contexts.push_back(this);

        _worker = std::thread([&] {
            int _epollfd = epoll_create1(0);
            
            while (!_terminated) {
                usleep(1);    
            }
            
            close(_epollfd);
        });

        _worker.detach();
    }


    MomentumContext::~MomentumContext() {
        term();
    }

    bool MomentumContext::terminated() {
        return _terminated;
    }

    void MomentumContext::term() {
        _terminated = true;
    };

    void MomentumContext::subscribe(const char *stream, const void (*handler)(const char *)) {
        if (!_consumers_by_stream.count(stream)) {
            _consumers_by_stream[stream] = std::vector<const void (*)(const char *)>();
        }
        _consumers_by_stream[stream].push_back(handler);
    }

    void MomentumContext::send(const char *stream, const char *data) {
        if (_consumers_by_stream.count(stream) > 0) {
            for (auto const& handler : _consumers_by_stream[stream]) {
                handler(data);
            }
        }
    }

    MomentumContext* context() {
        MomentumContext* ctx = new MomentumContext();
        return ctx;
    }

    void term(MomentumContext* ctx) {
        ctx->term();
    }

    bool terminated(MomentumContext *ctx) {
        return ctx->terminated();
    }

    void destroy(MomentumContext *ctx) {
        std::cout << "Destroying" << std::endl;
        delete ctx;
    }

    void subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(const char *)) {
        ctx->subscribe(stream, handler);
    }

    void send(MomentumContext *ctx, const char *stream, const char *data) {
        ctx->send(stream, data);
    }
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


