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

static std::vector<MomentumContext*> contexts;

void cleanup() {
    for (MomentumContext* ctx : contexts) {
        try {
            momentum_destroy(ctx);
        } catch(const std::exception& e) { }
    }
}

void fail(const std::string& reason) {
    std::perror(reason.c_str());

    cleanup();
    exit(EXIT_FAILURE);
}

MomentumContext::MomentumContext() {
    // Get our pid
    _pid = getpid();

    // enable debug mode via DEBUG=true or DEBUG=1 env variable
    char* env_value = getenv(DEBUG_ENV.c_str());
    _debug = env_value != NULL && (strcmp(env_value, "true") == 0 || strcmp(env_value, "1") == 0);

    if (fork() != 0) {
        // Initialize our ZMQ context
        _zmq_ctx = zmq_ctx_new();

        std::thread listener([&] {
            int bytes_received;

            Message message;

            while(!_terminated) {
                if (_consumer_sock != NULL) {
                    bytes_received = zmq_recv(_consumer_sock, &message, sizeof(message), 0);
                    if (bytes_received < 0) {
                        if (_debug) {
                            std::perror("Message receive failed to return a valid number of bytes");
                        }
                        continue;
                    }

                    std::string stream = message.stream;

                    Buffer* buffer;    

                    if (_buffer_by_shm_path.count(message.buffer_shm_path) == 0) {
                        buffer = allocate_buffer(message.buffer_shm_path, message.buffer_length, O_RDWR);
                    } else {
                        buffer = _buffer_by_shm_path[message.buffer_shm_path];
                        if (buffer->length < message.buffer_length) {
                            resize_buffer(buffer, message.buffer_length);
                        }
                    }
                    flock(buffer->fd, LOCK_SH);

                    // with the file locked, do a quick validity check to ensure the buffer has the same
                    // modified time as was provided in the message
                    struct stat fileinfo;
                    if (stat(std::string(SHM_PATH_BASE + message.buffer_shm_path).c_str(), &fileinfo) < 0) {
                        if (errno == ENOENT) {
                            deallocate_buffer(buffer);
                        } else {
                            if (_debug) {
                                std::perror("Failed to obtain file timestamps");
                            }
                        }
                    }

                    uint64_t buffer_ts = fileinfo.st_mtim.tv_sec * NANOS_PER_SECOND + fileinfo.st_mtim.tv_nsec; 
                    if (message.ts == buffer_ts) {
                        for (auto const& callback : _consumers_by_stream[stream]) {
                            callback(buffer->address, message.data_length, message.buffer_length, message.id);  
                        }
                    } else {
                        if (_debug) {
                            std::perror("Buffer / message mismatch");
                        }
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

        shm_iter([&](std::string filename) {
            if (filename.rfind(std::string(NAMESPACE + PATH_DELIM + std::to_string(_pid) + PATH_DELIM)) == 0) {
                shm_unlink(filename.c_str());
            }
        });

        // and then exit!
        exit(0);
    }
}

MomentumContext::~MomentumContext() {
    term();

    for (auto const& tuple : _buffer_by_shm_path) {
        deallocate_buffer(tuple.second);
    }
}

bool MomentumContext::is_terminated() const {
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

    if (!is_valid_stream(stream)) return -1; 
    stream.erase(0, PROTOCOL.size());

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

    if (!is_valid_stream(stream)) return -1; 
    stream.erase(0, PROTOCOL.size());

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

int MomentumContext::send_data(std::string stream, uint8_t* data, size_t length, uint64_t ts) {
    if (_terminated) return -1;

    if (!is_valid_stream(stream)) return -1; 
    stream.erase(0, PROTOCOL.size());

    Buffer* buffer = acquire_buffer(stream, length);
    
    memcpy(buffer->address, data, length);

    return send_buffer(buffer, length, ts);
}


int MomentumContext::send_buffer(Buffer* buffer, size_t length, uint64_t ts) {
    if (_terminated) return -1;

    Message message;
    message.data_length = length;
    message.buffer_length = buffer->length;
    message.id = ++_msg_id;
    strcpy(message.stream, stream_from_shm_path(buffer->shm_path).c_str());  
    strcpy(message.buffer_shm_path, buffer->shm_path);

    // adjust the buffer timestamp
    if (ts == 0) {
        ts = now();
    }
    message.ts = ts;
    update_shm_time(buffer->shm_path, ts);
    
    release_buffer(buffer);
    
    // send the buffer
    int rc = zmq_send(_producer_sock, &message, sizeof(message), 0); 
    if (rc < 0) {
        if (_debug) {
            std::perror("Failed to send message");
        }
        return -1;
    }
    
    return 0;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    if (!is_valid_stream(stream)) return NULL;
    stream.erase(0, PROTOCOL.size());
    
    if (_producer_sock == NULL) {
        _producer_sock = zmq_socket(_zmq_ctx, ZMQ_PUB);
    }

    if (_producer_streams.count(stream) == 0) {

        std::string endpoint(IPC_ENDPOINT_BASE + stream);

        int rc = zmq_bind(_producer_sock, endpoint.c_str()); 

        if (rc < 0) {
            if (_debug) {
                std::perror("Binding socket to endpoint");
            }
            return NULL;
        } else {
            _producer_streams.insert(stream);
        }
    }

    Buffer* buffer = NULL;

    if (_buffers_by_stream.count(stream) == 0) {
        _buffers_by_stream[stream] = std::queue<Buffer*>();
    }

    // First, see if we found a free buffer within our existing resources
    if (_buffers_by_stream.count(stream) > 0) {
        size_t visit_count = 0;

        // find the next buffer to use
        while (visit_count++ < _buffers_by_stream[stream].size()) {

            // pull a candidate buffer, rotating the queue in the process
            Buffer* candidate_buffer = _buffers_by_stream[stream].front();
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
        int id = 0;

        while (allocations_required > 0) {
            buffer = allocate_buffer(to_shm_path(_pid, stream, id++), length, O_RDWR | O_CREAT | O_EXCL);
            
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
        resize_buffer(buffer, length, true);
    }

    _last_acquired_buffer = buffer;

    return buffer;
}

void MomentumContext::release_buffer(Buffer* buffer) {
    if (!_terminated) {
        flock(buffer->fd, LOCK_UN);
    } 
}

Buffer* MomentumContext::allocate_buffer(const std::string& shm_path, size_t length, int flags) {
    if (_terminated) return NULL;
    
    int fd = shm_open(shm_path.c_str(), flags, S_IRWXU);
    if (fd < 0) {
        if (errno == EEXIST) {
            return NULL;
        } 

        if (fd < 0) {
            fail("Shared memory allocation");
        }
    } 

    Buffer* buffer = new Buffer();
    buffer->fd = fd;
    strcpy(buffer->shm_path, shm_path.c_str());

    resize_buffer(buffer, length, (flags & O_CREAT) == O_CREAT);

    _buffer_by_shm_path_mutex.lock();
    _buffer_by_shm_path[shm_path] = buffer;
    _buffer_by_shm_path_mutex.unlock();

    return buffer;
}

void MomentumContext::resize_buffer(Buffer* buffer, size_t length, bool truncate) {
    if (_terminated) return;

    size_t length_required = ceil(length / (double) PAGE_SIZE)*  PAGE_SIZE;
    bool meets_length_requirement = buffer->length >= length_required;

    if (!meets_length_requirement) {
        if (truncate) {
            int retval = ftruncate(buffer->fd, length_required);
            if (retval < 0) fail("Shared memory file truncate");
        }

        // Mmap the file, or remap if previously mapped
        if (buffer->length == 0) {
            buffer->address = (uint8_t* ) mmap(NULL, length_required, PROT_READ | PROT_WRITE, MAP_SHARED, buffer->fd, 0);
        } else {
            buffer->address = (uint8_t* ) mremap(buffer->address, buffer->length, length_required, MREMAP_MAYMOVE);
        }
        if (buffer->address == MAP_FAILED) {
            fail("Mapping shared memory");
        } else {
            buffer->length = length_required;
        }
    }
}

void MomentumContext::deallocate_buffer(Buffer* buffer) {
    munmap(buffer->address, buffer->length);
    close(buffer->fd);

    _buffer_by_shm_path_mutex.lock();
    _buffer_by_shm_path.erase(buffer->shm_path);
    _buffer_by_shm_path_mutex.unlock();

    delete buffer;
}

void MomentumContext::update_shm_time(const std::string& shm_path, uint64_t ts) {
    struct stat fileinfo;
    struct timespec updated_times[2];
    
    std::string filepath(SHM_PATH_BASE + shm_path);

    if (stat(filepath.c_str(), &fileinfo) < 0) {
        if (_debug) {
            std::perror("Failed to obtain file timestamps");
        }
    }

    updated_times[0] = fileinfo.st_atim;
    updated_times[1].tv_sec = ts / NANOS_PER_SECOND; 
    updated_times[1].tv_nsec = ts - (updated_times[1].tv_sec * NANOS_PER_SECOND);

    if (utimensat(AT_FDCWD, filepath.c_str(), updated_times, 0) < 0) {
        if (_debug) {
            std::perror("Failed to write file timestamps");
        }
    }

    if (stat(filepath.c_str(), &fileinfo) < 0) {
        if (_debug) {
            std::perror("Failed to obtain file timestamps");
        }
    }

    updated_times[1].tv_sec = ts / NANOS_PER_SECOND; 
    updated_times[1].tv_nsec = ts - (updated_times[1].tv_sec * NANOS_PER_SECOND);
}

uint64_t MomentumContext::now() const {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::high_resolution_clock::now().time_since_epoch()
    ).count();
}


void MomentumContext::shm_iter(std::function<void(std::string)> callback) {
    std::set<std::string> filenames;

    // clean up any unnecessary files created by the parent process
    struct dirent* entry;
    DIR* dir;
    dir = opendir(SHM_PATH_BASE.c_str());
    if (dir == NULL) {
        if (_debug) {
            std::perror("Could not access shm directory");
        }
    } else {
        while ((entry = readdir(dir))) {
            if (entry->d_name[0] == '.' || (entry->d_name[0] == '.' && entry->d_name[1] == '.')) {
                continue;
            }

            filenames.insert(std::string(entry->d_name));
        }
        closedir(dir);
    }

    for (std::string filename : filenames) {
        callback(filename);
    }
}

std::string MomentumContext::to_shm_path(pid_t pid, const std::string& stream, uint64_t id) const {
    // Build the path to the underlying shm file 
    std::ostringstream oss;
    oss << "/" << NAMESPACE << PATH_DELIM;
    oss << std::to_string(pid) << PATH_DELIM << stream << PATH_DELIM;
    oss << std::setw(8) << std::setfill('0') << id;
    return oss.str();
}


std::string MomentumContext::stream_from_shm_path(std::string shm_path) const {
    size_t pos = 0;
    std::string token;
    int token_index = 0;
    while ((pos = shm_path.find(PATH_DELIM)) != std::string::npos) {
        token = shm_path.substr(0, pos);
        if (token_index++ == 2) {
            return token;
        }
        shm_path.erase(0, pos + PATH_DELIM.length());
    }
    return "";
}

bool MomentumContext::is_valid_stream(const std::string& stream) const {
    // stream must start with protocol (i.e. "momentum://")
    if (stream.find(PROTOCOL) != 0) {
        if (_debug) {
            std::perror(std::string("Stream must start with \"" + PROTOCOL + "\"").c_str());
        }
        return false;
    }

    // stream must not contain path delimiter (i.e. "__")
    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
        if (_debug) {
            std::perror(std::string("Stream must not contain \"" + PATH_DELIM + "\"").c_str());
        }
        return false;
    } 

    return true;
}


MomentumContext* momentum_context() {
    MomentumContext* ctx = new MomentumContext();
    return ctx;
}

void momentum_term(MomentumContext* ctx) {
    ctx->term();
}

bool momentum_is_terminated(MomentumContext* ctx) {
    return ctx->is_terminated();
}

void momentum_destroy(MomentumContext* ctx) {
    delete ctx;
}

int momentum_subscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->subscribe(std::string(stream), callback);
}

int momentum_unsubscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->unsubscribe(std::string(stream), callback);
}

Buffer* momentum_acquire_buffer(MomentumContext* ctx, const char* stream, size_t length) {
    return ctx->acquire_buffer(std::string(stream), length);
}

void momentum_release_buffer(MomentumContext* ctx, Buffer* buffer) {
    return ctx->release_buffer(buffer);
}

int momentum_send_buffer(MomentumContext* ctx, Buffer* buffer, size_t length, uint64_t ts) {
    return ctx->send_buffer(buffer, length, ts ? ts : 0);
}

int momentum_send_data(MomentumContext* ctx, const char* stream, uint8_t* data, size_t length, uint64_t ts) {
    return ctx->send_data(std::string(stream), data, length, ts ? ts : 0);
}

uint8_t* momentum_get_buffer_address(Buffer* buffer) {
    return buffer->address;
}

size_t momentum_get_buffer_length(Buffer* buffer) {
    return buffer->length;
}

void momentum_configure(MomentumContext* ctx, uint8_t option, const void* value) {
    if (option == MOMENTUM_OPT_MIN_BUFFERS) {
        ctx->_min_buffers = (uint64_t) value;
    }
}
