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

    std::thread watcher([&] {
        std::map<std::string, struct stat> stat_by_shm_path;

        while (!_terminated) {
            std::set<std::string> updated_files;
            std::vector<std::string> filenames;
            
            DIR *dev_shm = opendir(DEV_SHM_PATH.c_str());
            struct dirent *ent;
            while ((ent = readdir(dev_shm)) != NULL) {
                filenames.push_back(ent->d_name);
            }
            closedir(dev_shm);
            delete ent;

            std::map<std::string, std::string> stream_by_shm_path;

            _consumer_streams_mutex.lock();
            for (std::string stream : _consumer_streams) {
                std::string match_string = "momentum_" + stream + "__";

                for (std::string filename : filenames) {
                    if (filename.find(match_string) != std::string::npos) {
                        stream_by_shm_path[filename] = stream;
                        struct stat stat_result;
                        if (stat((DEV_SHM_PATH + filename).c_str(), &stat_result) > -1) {
                            if (
                                stat_by_shm_path.count(filename) == 0 
                                || stat_by_shm_path[filename].st_mtim.tv_nsec < stat_result.st_mtim.tv_nsec
                            ) {
                                updated_files.insert(filename);
                            }
                            stat_by_shm_path[filename] = stat_result;
                        }
                        
                    }
                }
            }
            _consumer_streams_mutex.unlock();

            for (std::string shm_path : updated_files) {
                Buffer *buffer;
                _buffer_by_shm_path_mutex.lock();
                if (_buffer_by_shm_path.count(shm_path) == 0) {
                    _buffer_by_shm_path_mutex.unlock();
                    buffer = allocate_buffer(shm_path, stat_by_shm_path[shm_path].st_size - SIZE_T_SIZE, true);
                } else {
                    buffer = _buffer_by_shm_path[shm_path];
                    _buffer_by_shm_path_mutex.unlock();
                }

                if (buffer == NULL) {
                    fail("Unable to create readable shared memory");
                } 

                if (flock(buffer->fd, LOCK_SH) > -1) {
                    size_t length;
                    memcpy(&length, buffer->address, SIZE_T_SIZE);

                    for (auto const& handler : _consumers_by_stream[stream_by_shm_path[shm_path]]) {
                        handler(buffer->address + SIZE_T_SIZE, length);
                    }
                    // for (size_t i = SIZE_T_SIZE; i < (SIZE_T_SIZE + length); i++) {
                    //     std::cout << buffer->address[i];
                    // }                
                    // std::cout << std::endl;

                    flock(buffer->fd, LOCK_UN);
                } 
            }

        }

    });

    watcher.detach();

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
            shm_unlink(buffer->path.c_str());
        }

        delete buffer;
    }

}

bool MomentumContext::terminated() {
    return _terminated;
}

void MomentumContext::term() {
    _terminated = true;
};

int MomentumContext::subscribe(std::string stream, const void (*handler)(uint8_t *, size_t)) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    _consumer_streams_mutex.lock();

    _consumer_streams.insert(stream);
    
    if (!_consumers_by_stream.count(stream)) {
        _consumers_by_stream[stream] = std::vector<const void (*)(uint8_t *, size_t)>();
    }
    _consumers_by_stream[stream].push_back(handler);
   
    _consumer_streams_mutex.unlock();

    return 0;
}

int MomentumContext::unsubscribe(std::string stream, const void (*handler)(uint8_t *, size_t)) {
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

int MomentumContext::send(std::string stream, uint8_t *data, size_t length) {
    if (_terminated) return -1;

    if (stream.find(std::string("__")) != std::string::npos) {
        return -1;
    } 

    Buffer *buffer = acquire_buffer(stream, length);

    memcpy(buffer->address, &length, SIZE_T_SIZE);
    memcpy(buffer->address + SIZE_T_SIZE, data, length);

    release_buffer(buffer);

    return 0;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    if (stream.find(std::string("__")) != std::string::npos) {
        return NULL;
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

    // update the access time
    futimens(buffer->fd, NULL);

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
    buffer->path = shm_path;
    buffer->fd = fd;
    buffer->readonly = readonly;

    if (length > buffer->length) {
        resize_buffer(buffer, length);
    }

    _buffer_by_shm_path_mutex.lock();
    _buffer_by_shm_path[shm_path] = buffer;
    _buffer_by_shm_path_mutex.unlock();

    return buffer;
}

void MomentumContext::resize_buffer(Buffer *buffer, size_t length) {
    size_t length_required = ((length + SIZE_T_SIZE) / PAGE_SIZE + 1) * PAGE_SIZE;
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

uint8_t* momentum_buffer_data(MomentumContext *ctx, Buffer *buffer) {
    if (buffer != NULL) return buffer->address;
    else return NULL;
}
