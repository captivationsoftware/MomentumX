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
#include <dirent.h>
#include <chrono>

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

MomentumContext::MomentumContext() {
    if (!previous_sigint_handler) {
        previous_sigint_handler = std::signal(SIGINT, signal_handler);
    }

    _zmq_ctx = zmq_ctx_new();

    std::thread listener([&] {
        int bytes_received;

        Message message;

        while(!_terminated) {
            if (_consumer_sock != NULL) {
                bytes_received = zmq_recv(_consumer_sock, &message, sizeof(message), 0);
                if (bytes_received < 0) {
                    std::perror("Failed to receive message");
                }

                std::string stream = message.stream;
                std::string shm_path = message.path;

                Buffer *buffer;
                if (_buffer_by_shm_path.count(shm_path) == 0) {
                    buffer = allocate_buffer(shm_path, message.buffer_length, true);
                } else {
                    buffer = _buffer_by_shm_path[shm_path];
                    if (buffer->length < message.buffer_length) {
                        resize_buffer(buffer, message.buffer_length);
                    }
                }

                for (auto const& handler : _consumers_by_stream[stream]) {
                    handler(buffer->address, message.data_length);
                }

            } else {
                usleep(1);
            }
        }

    });

    listener.detach();

    contexts.push_back(this);
}


MomentumContext::~MomentumContext() {
    term();

    for (auto const& tuple : _buffer_by_shm_path) {
        std::string shm_path = tuple.first;
        Buffer *buffer = tuple.second;

        munmap(buffer->address, buffer->length);
        close(buffer->fd);

        if (!buffer->readonly) {
            shm_unlink(buffer->path);
        }

        delete buffer;
    }

}

bool MomentumContext::terminated() {
    return _terminated;
}

void MomentumContext::term() {
    _terminated = true;

    if (_consumer_sock != NULL) {
        zmq_close(_consumer_sock);
    }

    if (_producer_sock != NULL) {
        zmq_close(_producer_sock);
    }

    if (_zmq_ctx != NULL) {
        zmq_ctx_term(_zmq_ctx);
    }
};

int MomentumContext::subscribe(std::string stream, const void (*handler)(uint8_t *, size_t)) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    if (_consumer_sock == NULL) {
        _consumer_sock = zmq_socket(_zmq_ctx, ZMQ_SUB);
    }

    if (_consumer_streams.count(stream) == 0) {
        zmq_setsockopt(_consumer_sock, ZMQ_SUBSCRIBE, "", 0);

        std::string endpoint(IPC_ENDPOINT_BASE + stream);
        int rc = zmq_connect(_consumer_sock, endpoint.c_str()); 
        if (rc < 0) {
            return -1;
        } else {
            _consumer_streams.insert(stream);
        }
    }
    
    
    if (!_consumers_by_stream.count(stream)) {
        _consumers_by_stream[stream] = std::vector<const void (*)(uint8_t *, size_t)>();
    }
    _consumers_by_stream[stream].push_back(handler);
   

    return 0;
}

int MomentumContext::unsubscribe(std::string stream, const void (*handler)(uint8_t *, size_t)) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    if (_consumer_streams.count(stream) == 0) {

        std::string endpoint(IPC_ENDPOINT_BASE + stream);
        int rc = zmq_disconnect(_consumer_sock, endpoint.c_str()); 
        
        if (rc < 0) {
            return -1;
        } else {
            _consumer_streams.erase(stream);
        }
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

int MomentumContext::send(std::string stream, uint8_t *data, size_t length) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    Buffer *buffer = acquire_buffer(stream, length);

    memcpy(buffer->address, data, length);

    release_buffer(buffer);

    return send_message(stream, buffer, length);
}

int MomentumContext::send_message(std::string stream, Buffer *buffer, size_t length) {
    Message message;
    message.data_length = length;
    message.buffer_length = buffer->length;
    strcpy(message.stream, stream.c_str());
    strcpy(message.path, buffer->path);

    int rc = zmq_send(_producer_sock, &message, sizeof(message), 0); 
    if (rc < 0) {
        std::perror("Failed to send message");
        return -1;
    }
    return 0;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    if (stream.find(std::string("__")) != std::string::npos) {
        return NULL;
    } 

    if (_producer_sock == NULL) {
        _producer_sock = zmq_socket(_zmq_ctx, ZMQ_PUB);
    }

    if (_producer_streams.count(stream) == 0) {

        std::string endpoint(IPC_ENDPOINT_BASE + stream);

        int rc = zmq_bind(_producer_sock, endpoint.c_str()); 

        if (rc < 0) {
            std::perror("Binding socket to endpoint");
            return NULL;
        } else {
            _producer_streams.insert(stream);
        }
    }
    Buffer *buffer = NULL;

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
            buffer = allocate_buffer(shm_path, length, false);

            if (buffer != NULL) {
                allocation_count++;
                _buffers_by_stream[stream].push(buffer);
            }
        }
    } else if (length > buffer->length) {
        // buffer did exist but its undersized, so resize it
        resize_buffer(buffer, length);
    }

    _last_acquired_buffer = buffer;

    return buffer;
}

void MomentumContext::release_buffer(Buffer *buffer) {
    flock(buffer->fd, LOCK_UN);
}

Buffer* MomentumContext::allocate_buffer(std::string shm_path, size_t length, bool readonly) {
    int flags =  readonly ? O_RDWR : O_RDWR | O_CREAT | O_EXCL;
    int fd = shm_open(shm_path.c_str(), flags, S_IRWXU);
    if (fd < 0) {
        if (errno == EEXIST) {
            return NULL;
        } 

        if (fd < 0) {
            fail("Shared memory allocation");
        }
    } 

    Buffer *buffer = new Buffer();
    buffer->length = 0;
    buffer->fd = fd;
    buffer->readonly = readonly;
    strcpy(buffer->path, shm_path.c_str());

    if (length > buffer->length) {
        resize_buffer(buffer, length);
    }

    _buffer_by_shm_path_mutex.lock();
    _buffer_by_shm_path[shm_path] = buffer;
    _buffer_by_shm_path_mutex.unlock();

    return buffer;
}

void MomentumContext::resize_buffer(Buffer *buffer, size_t length) {
    size_t length_required = (length / PAGE_SIZE + 1) * PAGE_SIZE;
    bool meets_length_requirement = buffer->length >= length_required;

    if (!meets_length_requirement) {
        int retval = ftruncate(buffer->fd, length_required);
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
}

MomentumContext* momentum_context() {
    MomentumContext* ctx = new MomentumContext();
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

int momentum_subscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *, size_t)) {
    std::string stream_str(stream);
    return ctx->subscribe(stream_str, handler);
}

int momentum_unsubscribe(MomentumContext *ctx, const char *stream, const void (*handler)(uint8_t *, size_t)) {
    std::string stream_str(stream);
    return ctx->unsubscribe(stream_str, handler);
}

// int momentum_send_copy(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length) {
//     std::string stream_str(stream);
//     return ctx->send_copy(stream_str, data, length);
// }

int momentum_send(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length) {
    std::string stream_str(stream);
    return ctx->send(stream_str, data, length);
}

Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length) {
    std::string stream_str(stream);
    return ctx->acquire_buffer(stream_str, length);
}

void momentum_release_buffer(MomentumContext *ctx, Buffer *buffer) {
    ctx->release_buffer(buffer);
}

