#include <algorithm>
#include <iostream>
#include <errno.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <sys/shm.h>
#include <csignal>
#include <cmath>
#include <thread>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/file.h>        
#include <dirent.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <dirent.h>

#include "momentum.h"

static std::vector<MomentumContext *> contexts;

void cleanup() {
    for (MomentumContext *ctx : contexts) {
        try {
            momentum_destroy(ctx);
        } catch(const std::exception& e) { }
    }
}

void fail(std::string reason) {
    std::perror(reason.c_str());

    cleanup();
    exit(EXIT_FAILURE);
}

MomentumContext::MomentumContext() {
    // Get our pid
    _pid = getpid();

    if (fork() != 0) {
        // Initialize our ZMQ context
        _zmq_ctx = zmq_ctx_new();

        std::thread listener([&] {
            int bytes_received;
            uint64_t ts = 0;

            Message message;

            while(!_terminated) {
                if (_consumer_sock != NULL) {
                    bytes_received = zmq_recv(_consumer_sock, &message, sizeof(message), 0);
                    if (bytes_received < 0) {
                        std::perror("Failed to receive message");
                    }

                    if (message.ts <= ts) {
                        continue;
                    } else {
                        ts = message.ts;
                    }

                    std::string stream = message.stream;
                    uint64_t latency_ms = (std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count() - ts) / 1e6;

                    if (latency_ms > _max_latency) {
                        continue;
                    }

                    Buffer *buffer;    
                    
                    std::string shm_path = to_shm_path(message.buffer_owner_pid, stream, message.buffer_id);
                    if (_buffer_by_shm_path.count(shm_path) == 0) {
                        buffer = allocate_buffer(stream, message.buffer_id, message.buffer_length, O_RDWR, message.buffer_owner_pid);
                    } else {
                        buffer = _buffer_by_shm_path[shm_path];
                        if (buffer->length < message.buffer_length) {
                            resize_buffer(buffer, message.buffer_length);
                        }
                    }
                    flock(buffer->fd, LOCK_SH);
                    for (auto const& callback : _consumers_by_stream[stream]) {
                        callback(buffer->address, message.data_length, message.buffer_length, message.id, latency_ms);  
                    }
                    flock(buffer->fd, LOCK_UN);

                } else {
                    usleep(1);
                }
            }

        });

        listener.detach();

        contexts.push_back(this);
    } else {
        // wait for our parent to die... :(
        while (getppid() == _pid) {
            usleep(1);
        }

        // clean up any unnecessary files created by the parent process
        struct dirent *entry;
        DIR *dir;
        dir = opendir(std::string(SHM_PATH_BASE + "/" + NAMESPACE).c_str());
        if (dir == NULL) {
            std::perror("Could not access shm directory");
        } else {
            int file_count = 0;
            int delete_count = 0;
            while ((entry = readdir(dir))) {
                if (entry->d_name[0] == '.' || (entry->d_name[0] == '.' && entry->d_name[1] == '.')) {
                    continue;
                }

                file_count++;
                if (std::string(entry->d_name).rfind(std::to_string(_pid) + PATH_DELIM) == 0) {
                    shm_unlink(std::string(NAMESPACE + "/" + entry->d_name).c_str());
                    delete_count++;
                }
            }
            closedir(dir);

            // if we deleted every file in the folder, then delete the folder too.
            if (file_count == delete_count && NAMESPACE.size() > 0) {
                rmdir(std::string(SHM_PATH_BASE + "/" + NAMESPACE).c_str());
            }
        }

        // and then exit!
        exit(0);
    }

}


MomentumContext::~MomentumContext() {
    term();

    for (auto const& tuple : _buffer_by_shm_path) {
        std::string shm_path = tuple.first;
        Buffer *buffer = tuple.second;

        munmap(buffer->address, buffer->length);
        close(buffer->fd);
        
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

int MomentumContext::subscribe(std::string stream, callback_t callback) {
    if (_terminated) return -1;

    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
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
        _consumers_by_stream[stream] = std::vector<callback_t>();
    }
    _consumers_by_stream[stream].push_back(callback);
   

    return 0;
}

int MomentumContext::unsubscribe(std::string stream, callback_t callback) {
    if (_terminated) return -1;

    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
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
                callback
            ), 
            _consumers_by_stream[stream].end()
        );
    }

    return 0;
}

int MomentumContext::send_data(std::string stream, uint8_t *data, size_t length) {
    if (_terminated) return -1;

    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
        return -1;
    } 

    Buffer *buffer = acquire_buffer(stream, length);
    
    memcpy(buffer->address, data, length);

    release_buffer(stream, buffer);

    return send_buffer(stream, buffer, length);
}

int MomentumContext::send_buffer(std::string stream, Buffer *buffer, size_t length) {
    if (_terminated) return -1;

    Message message;
    message.data_length = length;
    message.buffer_id = buffer->id;
    message.buffer_owner_pid = buffer->owner_pid;
    message.buffer_length = buffer->length;
    message.ts = std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count();
    message.id = ++_msg_id;

    strcpy(message.stream, stream.c_str());
    int rc = zmq_send(_producer_sock, &message, sizeof(message), 0); 
    if (rc < 0) {
        std::perror("Failed to send message");
        return -1;
    }
    return 0;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
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

            if (candidate_buffer != _last_acquired_buffer) {
                // found a buffer that is different than the last iteration...
                if (flock(candidate_buffer->fd, LOCK_EX | LOCK_NB) > -1) {
                    // and we were also able to set the exclusive lock... 
                    buffer = candidate_buffer;
                    break;
                }
            }
        }
    } 

    // If we don't have enough buffers, then create them...
    bool below_minimum_buffers = _buffers_by_stream[stream].size() < _min_buffers;

    if (buffer == NULL || below_minimum_buffers) {
        size_t allocations_required = below_minimum_buffers ? _min_buffers : 1;

        Buffer* first_buffer = NULL;
        int id = 1;
        while (allocations_required > 0) {
            buffer = allocate_buffer(stream, id++, length, O_RDWR | O_CREAT | O_EXCL);
            
            if (buffer != NULL) {
                allocations_required--;
                _buffers_by_stream[stream].push(buffer);
                
                // Since we may create numerous buffers, make note of this first buffer to 
                // maintain some semblance of sequential ordering.
                if (first_buffer == NULL) {
                    first_buffer = buffer;
                }
            }
        }

        buffer = first_buffer;

    } else if (length > buffer->length) {
        // buffer did exist but its undersized, so resize it
        resize_buffer(buffer, length);
    }

    _last_acquired_buffer = buffer;

    return buffer;
}

void MomentumContext::release_buffer(std::string stream, Buffer *buffer) {
    if (_terminated) return;

    // update buffer modified time
    update_shm_time(to_shm_path(buffer->owner_pid, stream, buffer->id));

    flock(buffer->fd, LOCK_UN);
}

Buffer* MomentumContext::allocate_buffer(std::string stream, int id, size_t length, int flags, pid_t owner_pid) {
    if (_terminated) return NULL;

    // Create the file momentum path to store the shm files
    mkdir(std::string(SHM_PATH_BASE + "/" + NAMESPACE).c_str(), 0700);

    owner_pid = owner_pid < 0 ? _pid : owner_pid;

    std::string shm_path = to_shm_path(owner_pid, stream, id);
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
    buffer->id = id;
    buffer->fd = fd;
    buffer->owner_pid = owner_pid;

    resize_buffer(buffer, length);

    _buffer_by_shm_path_mutex.lock();
    _buffer_by_shm_path[shm_path] = buffer;
    _buffer_by_shm_path_mutex.unlock();

    return buffer;
}

void MomentumContext::resize_buffer(Buffer *buffer, size_t length) {
    if (_terminated) return;

    size_t length_required = ceil(length / (double) PAGE_SIZE) * PAGE_SIZE;
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

std::string MomentumContext::to_shm_path(pid_t pid, std::string stream, int id) {
    std::ostringstream oss;
    oss << "/" << NAMESPACE << "/";
    oss << std::to_string(pid) << PATH_DELIM << stream << PATH_DELIM;
    oss << std::setw(8) << std::setfill('0') << id;
    return oss.str();
}

void MomentumContext::update_shm_time(std::string shm_path) {
    struct stat fileinfo;
    struct timespec updated_times[2];
    
    std::string filepath(SHM_PATH_BASE + shm_path);

    if (stat(filepath.c_str(), &fileinfo) < 0) {
        std::perror("Failed to obtain file timestamps");
    }
    
    updated_times[0] = fileinfo.st_atim;
    clock_gettime(CLOCK_REALTIME, &updated_times[1]);

    if (utimensat(AT_FDCWD, filepath.c_str(), updated_times, 0) < 0) {
        std::perror("Failed to write file timestamps");
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

int momentum_subscribe(MomentumContext *ctx, const char *stream, callback_t callback) {
    std::string stream_str(stream);
    return ctx->subscribe(stream_str, callback);
}

int momentum_unsubscribe(MomentumContext *ctx, const char *stream, callback_t callback) {
    std::string stream_str(stream);
    return ctx->unsubscribe(stream_str, callback);
}

int momentum_send_data(MomentumContext *ctx, const char *stream, uint8_t *data, size_t length) {
    std::string stream_str(stream);
    return ctx->send_data(stream_str, data, length);
}

int momentum_send_data(MomentumContext *ctx, const char *stream, Buffer *buffer, size_t length) {
    std::string stream_str(stream);
    return ctx->send_buffer(stream_str, buffer, length);
}

Buffer* momentum_acquire_buffer(MomentumContext *ctx, const char *stream, size_t length) {
    std::string stream_str(stream);
    return ctx->acquire_buffer(stream_str, length);
}

void momentum_release_buffer(MomentumContext *ctx, const char *stream, Buffer *buffer) {
    std::string stream_str(stream);
    ctx->release_buffer(stream_str, buffer);
}

uint8_t* momentum_get_buffer_address(Buffer *buffer) {
    return buffer->address;
}

size_t momentum_get_buffer_length(Buffer *buffer) {
    return buffer->length;
}

void momentum_configure(MomentumContext *ctx, uint8_t option, const void *value) {
    if (option == MOMENTUM_OPT_MAX_LATENCY) {
        ctx->_max_latency = (uint64_t) value;
    } else if (option == MOMENTUM_OPT_MIN_BUFFERS) {
        ctx->_min_buffers = (uint64_t) value;
    }
}
