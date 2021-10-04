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
#include <sys/file.h>        
#include <sstream>  
#include "momentum.h"

static std::vector<MomentumContext *> contexts;

static void (*previous_sigint_handler)(int) = NULL;

void cleanup() {
    for (MomentumContext *ctx : contexts) {
        try {
            momentum_destroy(ctx);
        } catch(const std::exception& e) { }
    }
}

void signal_handler(int signal) {
    cleanup();

    if (previous_sigint_handler) {
        previous_sigint_handler(signal);
    }
};

void fail(std::string reason) {
    std::perror(reason.c_str());

    cleanup();
    exit(EXIT_FAILURE);
}

MomentumContext::MomentumContext(std::string name) {
    _name = name;

    if (!previous_sigint_handler) {
        previous_sigint_handler = std::signal(SIGINT, signal_handler);
    }

    contexts.push_back(this);



    _watcher_thread = std::thread([&] {
        while (!_terminated) {
            _mutex.lock();

            _mutex.unlock();
        }
    });

    _watcher_thread.detach();
}


MomentumContext::~MomentumContext() {
    term();

    for (Buffer *buffer : _producer_buffers) {
        munmap(buffer->address, buffer->length);
        close(buffer->fd);
        shm_unlink(buffer->path.c_str());
        delete buffer;
    }

    _producer_buffers.clear();
}

bool MomentumContext::terminated() {
    return _terminated;
}

void MomentumContext::term() {
    _terminated = true;
};

int MomentumContext::subscribe(std::string stream, const void (*handler)(uint8_t *)) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    if (!_consumers_by_stream.count(stream)) {
        _consumers_by_stream[stream] = std::vector<const void (*)(uint8_t *)>();
    }
    _consumers_by_stream[stream].push_back(handler);

    return 0;
}

int MomentumContext::unsubscribe(std::string stream, const void (*handler)(uint8_t *)) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

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

    return 0;
}

int MomentumContext::send(std::string stream, const uint8_t *data, size_t length) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    Buffer *buffer = acquire_buffer(stream, length);

    memcpy(buffer->address, data, length);

    flock(buffer->fd, LOCK_UN);

    return 0;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    if (stream.find(std::string("__")) != std::string::npos) {
        return NULL;
    } 

    Buffer *buffer = NULL;

    _mutex.lock();
    _producer_streams.insert(stream);

    if (_buffers_by_stream.count(stream) == 0) {
        _buffers_by_stream[stream] = std::queue<Buffer *>();
    }

    // First, see if we found a free buffer within our existing resources
    if (_buffers_by_stream.count(stream) > 0) {
        size_t visit_count = 0;

        // find the next buffer to use
        while (visit_count++ < _buffers_by_stream[stream].size()) {

            // pull a candidate buffer, rotating the queue in the process
            Buffer *candidate_buffer = _buffers_by_stream[stream].front();
            _buffers_by_stream[stream].pop();
            _buffers_by_stream[stream].push(candidate_buffer);

            if (candidate_buffer != _last_acquired_buffer && flock(candidate_buffer->fd, LOCK_EX | LOCK_NB) > -1) {
                // found a buffer that is different than the last iteration
                // that we were able to lock; 
                buffer = candidate_buffer;
                break;
            }
        }
    }

    // If we couldn't find a buffer, then create a new one...
    if (buffer == NULL) {
        size_t buffer_count = 0;
        size_t allocation_count = 0;
        while (allocation_count < 1) {
            std::string shm_path("/momentum_" + stream + "__" + std::to_string(buffer_count++));
            int fd = shm_open(shm_path.c_str(), O_CREAT | O_EXCL | O_RDWR, S_IRWXU);
            if (fd < 0) {
                if (errno == EEXIST) {
                    continue;
                } 

                if (fd < 0) {
                    fail("Shared memory allocation");
                }
            } else {
                allocation_count++;
            }

            buffer = new Buffer();
            buffer->length = 0;
            buffer->path = shm_path;
            buffer->fd = fd;
            _producer_buffers.push_back(buffer);
            _buffers_by_stream[stream].push(buffer);
        }
    }

    _mutex.unlock();    

    size_t length_required = (length / PAGE_SIZE + 1) * PAGE_SIZE;
    bool meets_length_requirement = buffer->length >= length_required;

    int retval;
    if (!meets_length_requirement) {
        retval = ftruncate(buffer->fd, length_required);
        if (retval < 0) fail("Shared memory file truncate");
    
        // Mmap the file, or remap if previously mapped
        if (buffer->length == 0) {
            buffer->address = (uint8_t *) mmap(NULL, length_required, PROT_READ | PROT_WRITE, MAP_SHARED, buffer->fd, 0);
        } else {
            buffer->address = (uint8_t *) mremap(buffer->address, buffer->length, length_required, MREMAP_MAYMOVE);
        }
        if (buffer->address == MAP_FAILED) {
            fail("Mapping shared memory");
        } else {
            buffer->length = length_required;
        }
    }

    _last_acquired_buffer = buffer;

    return buffer;
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

int momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *)) {
    std::string stream_str(stream);
    return ctx->subscribe(stream_str, handler);
}

int momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *)) {
    std::string stream_str(stream);
    return ctx->unsubscribe(stream_str, handler);
}

int momentum_send(MomentumContext *ctx, const char *stream, const uint8_t *data, size_t length) {
    std::string stream_str(stream);
    return ctx->send(stream_str, data, length);
}

uint8_t* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length) {
    std::string stream_str(stream);
    Buffer *buffer = ctx->acquire_buffer(stream_str, length);
    
    if (buffer == NULL) return NULL;
    else return buffer->address;
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


