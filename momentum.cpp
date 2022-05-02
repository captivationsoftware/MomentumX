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
#include <sys/file.h>        
#include <dirent.h>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>

#include "momentum.h"

static std::vector<MomentumContext*> contexts;

void cleanup() {
    for (MomentumContext* ctx : contexts) {
        if (ctx != NULL) {
            delete ctx;
        }            
    }
}

MomentumContext::MomentumContext() {
    // Get our pid
    _pid = getpid();

    if (fork() != 0) {

        std::thread consume([&] {
            char message[1024];

            while(!_terminated) {
                if (_consumer_streams.size() > 0) {
                    for (auto const& stream : _consumer_streams) {
                        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
                            mq_receive(consumer_mq, message, sizeof(message), NULL);

                            // we may have exitted while blocking, so if so, return
                            if (_terminated) return;

                            uint8_t type;
                            std::string stream, payload;

                            std::stringstream parser(message);
                            parser >> type >> stream >> payload;

                            switch(type) {
                                case MESSAGE_TYPE_TERM:
                                    for (auto const& callback : _callbacks_by_stream[stream]) {
                                        unsubscribe(stream, callback);
                                    }
                                    break;
                                case MESSAGE_TYPE_BUFFER:
                                    break;
                            }            

                        }

                    }
                } else {
                    usleep(1);
                }
                // if (_consumer_sock != NULL) {
                //     bytes_received = zmq_recv(_consumer_sock, message, sizeof(message), 0);
                //     if (bytes_received < 0) {
                //         if (_debug) {
                //             std::cerr << DEBUG_PREFIX << "Received " << bytes_received << " message bytes" << std::endl;
                //         }
                //         continue;
                //     }

                //     std::string stream = message.stream;

                //     Buffer* buffer;    

                //     if (_buffer_by_shm_path.count(message.buffer_shm_path) == 0) {
                //         buffer = allocate_buffer(message.buffer_shm_path, message.buffer_length, O_RDWR);
                //     } else {
                //         buffer = _buffer_by_shm_path[message.buffer_shm_path];
                //         if (buffer->length < message.buffer_length) {
                //             resize_buffer(buffer, message.buffer_length);
                //         }
                //     }
                    
                //     // with the file locked, do a quick validity check to ensure the buffer has the same
                //     // modified time as was provided in the message
                //     uint64_t buffer_write_ts;
                //     get_shm_time(message.buffer_shm_path, NULL, &buffer_write_ts);


                //     bool buffer_unlinked = buffer_write_ts == 0;


                //     if (lock_sh(buffer, false)) {
                //         if (buffer_unlinked || message.ts == buffer_write_ts) {
                //             // update buffer access (read) time
                //             set_shm_time(message.buffer_shm_path, now(), 0);

                //             for (auto const& callback : _callbacks_by_stream[stream]) {
                //                 callback(buffer->address, message.data_length, message.buffer_length, message.id);  
                //             }
                //         } else {
                //             // too late
                //         }
    
                //         // lock_un(buffer);
                //     }

                //     // If the buffer was unlinked, deallocate it now that our callback has executed.
                //     // This ensures that the callback above would still have access to the buffer's fd 
                //     // within this process, but closes it afterwards to prevent future reads
                //     if (buffer_unlinked) {
                //         std::cout << "Buffer unlinked" << std::endl;
                //         deallocate_buffer(buffer);
                //     }
                // } else {
                //     usleep(1);
                // }
            }
        });
        
        consume.detach();

        std::thread produce([&] {
            char message[1024];

            while(!_terminated) {
                if (_producer_streams.size() > 0) {

                    for (auto const& producer_stream : _producer_streams) {
                        // wait for a message 
                        mqd_t producer_mq = _producer_mq_by_stream[producer_stream];
                        mq_receive(producer_mq, message, sizeof(message), NULL);

                        // we may have been terminated while awaiting, so exit if that's the case
                        if (_terminated) return;

                        uint8_t type;
                        std::string stream, payload;

                        std::stringstream parser(message);
                        parser >> type >> stream >> payload;

                        mqd_t consumer_mq;

                        switch (type) {
                            case MESSAGE_TYPE_SUBSCRIBE:
                                
                                // create the consumer_mq for this consumer
                                consumer_mq = mq_open(payload.c_str(), O_RDWR);
                                if (consumer_mq < 0) {
                                    if (_debug) {
                                        std::cerr << DEBUG_PREFIX << "Failed to find mq for stream: " << stream << std::endl;
                                    }
                                } else {
                                    _mq_by_mq_name[payload] = consumer_mq;
                                    _consumer_mqs_by_stream[stream].push_back(consumer_mq);
                                    if (_debug) {
                                        std::cout << DEBUG_PREFIX << "Added subscriber to stream: " << stream << std::endl;
                                    }
                                }    
                                

                                break;

                            case MESSAGE_TYPE_UNSUBSCRIBE:
                                if (_mq_by_mq_name.count(payload) > 0) {

                                    consumer_mq = _mq_by_mq_name[payload];
                                    mq_close(consumer_mq);
                                    _mq_by_mq_name.erase(payload);
    
                                    _consumer_mqs_by_stream[stream].erase(
                                        std::remove(
                                            _consumer_mqs_by_stream[stream].begin(),
                                            _consumer_mqs_by_stream[stream].end(),
                                            consumer_mq
                                        ),
                                        _consumer_mqs_by_stream[stream].end()
                                    );

                                    if (_debug) {
                                        std::cout << DEBUG_PREFIX << "Removed subscriber from stream: " << stream << std::endl;
                                    }
                                }

                                break;
                            case MESSAGE_TYPE_ACK:
                                if (_debug) {
                                    std::cout << DEBUG_PREFIX << "Ack stream: " << stream << std::endl;
                                }
                                break;
                        }

                    }
                } else {
                    usleep(1);
                }

                // // if (_producer_sock != NULL) {

                //     bytes_received = zmq_recv(_producer_sock, &subscription, sizeof(subscription));
                //     if (bytes_received < 0) {
                //         if (_debug) {
                //             std::cerr << DEBUG_PREFIX << "Received " << bytes_received << " subscription bytes" << std::endl;
                //         }
                //         continue;
                //     }

                //     std::cout << "received " << std::string(subscription) << std::endl;
                //}    
                // else {
                //     usleep(1);
                // }
            }

        });

        produce.detach();


        contexts.push_back(this);
    } else {
        // wait for our parent to die... :(
        while (getppid() == _pid) {
            sleep(1);
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

    // iterate over a copy of the buffer by shm_path map, deallocating all buffers
    for (auto const& tuple : std::map<std::string, Buffer*>(_buffer_by_shm_path)) {
        deallocate_buffer(tuple.second);
    }

    
}

bool MomentumContext::is_terminated() const {
    return _terminated;
}

bool MomentumContext::term() {
    if (_terminated) {
        return false;
    }

    // for every stream we are producing, notify the consumers that we are terminating
    for (auto const& stream : _producer_streams) {
        std::string producer_mq_name = to_mq_name(0, stream);

        std::string message(MESSAGE_TYPE_TERM + MESSAGE_DELIM + stream + MESSAGE_DELIM + producer_mq_name);
        
        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
            if (mq_send(consumer_mq, message.c_str(), message.length(), 0) < 0) {
                std::cerr << DEBUG_PREFIX << "Failed to notify term for stream: " << stream << std::endl;
            };
        }

        if (mq_unlink(producer_mq_name.c_str()) < 0) {
            std::cout << DEBUG_PREFIX << "Failed to unlink producer mq for stream: " << stream << std::endl;
        }
    }

    // for every stream we are consuming, issue an unsubscribe
    for (auto const& stream : _consumer_streams) {
        for (auto const& callback : _callbacks_by_stream[stream]) {
            unsubscribe(stream, callback);
        }
    }

    
    _terminated = true;

    return true;
};

bool MomentumContext::subscribe(std::string stream, callback_t callback) {
    if (_terminated) return false;

    normalize_stream(stream); 
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    }

    // Open the producer mq  
    std::string producer_mq_name = to_mq_name(0, stream);
    mqd_t producer_mq = mq_open(producer_mq_name.c_str(), O_RDWR);
    if (producer_mq < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to find mq for stream: " << stream << std::endl;
        }
        return false;
    } 

    // ...and then open our own consumer mq
    struct mq_attr attrs;
    memset(&attrs, 0, sizeof(attrs));
    attrs.mq_maxmsg = 1;
    attrs.mq_msgsize = 1024;
    std::string consumer_mq_name = to_mq_name(_pid, stream);
    mqd_t consumer_mq = mq_open(consumer_mq_name.c_str(), O_RDWR | O_CREAT, MQ_MODE, &attrs);
    if (consumer_mq < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to create mq for stream: " << stream << std::endl;
        }
        mq_close(producer_mq);
        return false;
    }

    // ...and send the producer subscription message
    std::string message(MESSAGE_TYPE_SUBSCRIBE + MESSAGE_DELIM + stream + MESSAGE_DELIM + consumer_mq_name);
    if (mq_send(producer_mq, message.c_str(), message.length(), 0) < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to send subscribe message for stream: " << stream << std::endl;
        }

        mq_close(producer_mq);

        mq_close(consumer_mq);
        if (mq_unlink(consumer_mq_name.c_str()) < 0) {
            if (_debug) {        
                std::cerr << DEBUG_PREFIX << "Failed to unlink consumer mq for stream: " << stream << std::endl;
            }
        }
        
        return false;
    };
    
    // If we made it here, everything went well so cache references
    _producer_mq_by_stream[stream] = producer_mq;
    
    if (_consumer_mqs_by_stream.count(stream) == 0) {
        _consumer_mqs_by_stream[stream] = std::vector<mqd_t>();
    }
    _consumer_mqs_by_stream[stream].push_back(consumer_mq);


    if (!_callbacks_by_stream.count(stream)) {
        _callbacks_by_stream[stream] = std::vector<callback_t>();
    }
    _callbacks_by_stream[stream].push_back(callback);
   
    _consumer_streams.insert(stream);

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Subscribed to stream: " << stream << std::endl;
    }

    return true;
}

bool MomentumContext::unsubscribe(std::string stream, callback_t callback) {
    if (_terminated) return false;

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream " << stream << std::endl;
        }
        return false;
    } 

    // exit early if we are not subscribed to this stream
    if (_consumer_streams.count(stream) == 0) {
        return false;
    }

    // first notify the producer that we're exiting
    std::string message(MESSAGE_TYPE_UNSUBSCRIBE + MESSAGE_DELIM + stream + MESSAGE_DELIM + to_mq_name(_pid, stream));
    if (mq_send(_producer_mq_by_stream[stream], message.c_str(), message.length(), 0) < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to send unsubscribe message for stream: " << stream << std::endl;
        }
        return false;
    };

    mq_close(_producer_mq_by_stream[stream]);
    _producer_mq_by_stream.erase(stream);

    for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
        mq_close(consumer_mq);
    }
    _consumer_mqs_by_stream.erase(stream);
    std::string consumer_mq_name = to_mq_name(_pid, stream); 
    if (mq_unlink(consumer_mq_name.c_str()) < 0) {
        std::cout << DEBUG_PREFIX << "Failed to unlink consumer mq: " << consumer_mq_name << std::endl;
    }
    
    _consumer_streams.erase(stream);

    if (_callbacks_by_stream.count(stream)) {
        _callbacks_by_stream[stream].erase(
            std::remove(
                _callbacks_by_stream[stream].begin(), 
                _callbacks_by_stream[stream].end(), 
                callback
            ), 
            _callbacks_by_stream[stream].end()
        );
    }

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Unsubscribed from stream: " << stream << std::endl;
    }

    return true;
}

bool MomentumContext::send_data(std::string stream, uint8_t* data, size_t length, uint64_t ts) {
    if (_terminated) return false;

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    } 

    Buffer* buffer = acquire_buffer(stream, length);
    
    if (buffer == NULL) return false;

    memcpy(buffer->address, data, length);

    return send_buffer(buffer, length, ts);
}


bool MomentumContext::send_buffer(Buffer* buffer, size_t length, uint64_t ts) {
    if (_terminated) return false;

    if (buffer == NULL) return false;

    Message message;
    message.data_length = length;
    message.buffer_length = buffer->length;
    message.id = ++_msg_id;
    // strcpy(message.stream, stream_from_shm_path(buffer->shm_path).c_str());  
    strcpy(message.buffer_shm_path, buffer->shm_path);

    // adjust the buffer timestamp
    if (ts == 0) {
        ts = now();
    }

    message.ts = ts;
    
    // update buffer modify (write) time
    set_shm_time(buffer->shm_path, 0, ts);
    
    // release_buffer(buffer);

    // send the buffer
    // int rc = zmq_send(_producer_sock, &message, sizeof(message), 0); 
    // if (rc < 0) {
    //     if (_debug) {
    //         std::cerr << DEBUG_PREFIX << "Failed to send message" << std::endl;
    //     }
    //     return false;
    // }
    
    return true;
}

Buffer* MomentumContext::acquire_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return NULL;
    };

    if (_producer_mq_by_stream.count(stream) == 0) {
        struct mq_attr attrs;
        memset(&attrs, 0, sizeof(attrs));
        attrs.mq_maxmsg = 1;
        attrs.mq_msgsize = 1024;

        mqd_t producer_mq = mq_open(to_mq_name(0, stream).c_str(), O_RDWR | O_CREAT, MQ_MODE, &attrs);
        if (producer_mq < 0) {
            if (_debug) {
                std::cerr << DEBUG_PREFIX << "Failed to open mq for stream: " << stream << std::endl;
            }
            return false;
        }

        _producer_mq_by_stream[stream] = producer_mq;        
    }

    _producer_streams.insert(stream);


    if (_buffers_by_stream.count(stream) == 0) {
        _buffers_by_stream[stream] = std::queue<Buffer*>();
    }

    // If we don't have enough buffers, then create them...
    if (_buffers_by_stream[stream].size() < _min_buffers) {
        size_t allocations_required = _min_buffers - _buffers_by_stream[stream].size();

        int id = 1; // start buffer count at 1
        while (allocations_required > 0 && _buffers_by_stream[stream].size() < _max_buffers) {
            Buffer* allocated = allocate_buffer(to_shm_path(_pid, stream, id++), length, O_RDWR | O_CREAT | O_EXCL);
            
            if (allocated != NULL) {
                allocations_required--;
                _buffers_by_stream[stream].push(allocated);
            }
        }
    }

    // If we have buffers, look to lock a free one down
    Buffer* buffer = NULL;
    if (_buffers_by_stream.count(stream) > 0) {

        // pull a candidate buffer, rotating the queue in the process
        Buffer* candidate_buffer = _buffers_by_stream[stream].front();
        _buffers_by_stream[stream].pop();
        _buffers_by_stream[stream].push(candidate_buffer);

        if (_buffers_by_stream.count(stream) == 1 || candidate_buffer != _last_acquired_buffer) {
            if (lock_ex(candidate_buffer, true) > -1) {
                buffer = candidate_buffer;
            } else {
                // something unexpected happened (likely, program termination)
                return NULL;
            }
        }
    }     

    if (length > buffer->length) {
        // buffer did exist but its undersized, so resize it
        resize_buffer(buffer, length, true);
    }

    _last_acquired_buffer = buffer;

    return buffer;
}

bool MomentumContext::release_buffer(Buffer* buffer) {
    if (!_terminated) {
        lock_un(buffer);
        return true;
    } 
    return false;
}

Buffer* MomentumContext::allocate_buffer(const std::string& shm_path, size_t length, int flags) {
    if (_terminated) return NULL;

    int fd = shm_open(shm_path.c_str(), flags, S_IRWXU);
    if (fd < 0) {
        if (errno == EEXIST) {
            return NULL;
        } 

        if (fd < 0) {
            if (_debug) {
                std::cerr << DEBUG_PREFIX << "Failed to allocate shm file: " << shm_path << std::endl;
            }
        }
    } 

    struct stat file_stat;  
    if (fstat (fd, &file_stat) < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to stat shm file: " << shm_path << std::endl;
            
        }
        close(fd);
        shm_unlink(shm_path.c_str());
        return NULL;
    }  
    
    Buffer* buffer = new Buffer();
    buffer->fd = fd;
    strcpy(buffer->shm_path, shm_path.c_str());

    buffer->inode = file_stat.st_ino;

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
            if (retval < 0) {
                if (_debug) {
                    std::cerr << DEBUG_PREFIX << "Failed to truncate file: " << buffer->shm_path << std::endl;
                }
            }
        }

        // Mmap the file, or remap if previously mapped
        if (buffer->length == 0) {
            buffer->address = (uint8_t* ) mmap(NULL, length_required, PROT_READ | PROT_WRITE, MAP_SHARED, buffer->fd, 0);
        } else {
            buffer->address = (uint8_t* ) mremap(buffer->address, buffer->length, length_required, MREMAP_MAYMOVE);
        }
        if (buffer->address == MAP_FAILED) {
            if (_debug) {
                std::cerr << DEBUG_PREFIX << "Failed to mmap shared memory file: " << buffer->shm_path << std::endl;
            }
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


void MomentumContext::get_shm_time(const std::string& shm_path, uint64_t* read_ts, uint64_t* write_ts) {
    struct stat fileinfo;
    
    std::string filepath(SHM_PATH_BASE + shm_path);
    if (stat(filepath.c_str(), &fileinfo) < 0) {
        // if file was deleted, then set read/write timestamps to null
        if (errno == ENOENT) {
            if (read_ts != NULL) *read_ts = 0;

            if (write_ts != NULL) *write_ts = 0;
        } else {
            if (_debug) {
                std::cerr << DEBUG_PREFIX << "Failed to stat file: " << filepath << std::endl;
            }
        }
    } else {
        if (read_ts != NULL) {
            *read_ts = fileinfo.st_atim.tv_sec * NANOS_PER_SECOND + fileinfo.st_atim.tv_nsec;
        }

        if (write_ts != NULL) {
            *write_ts = fileinfo.st_mtim.tv_sec * NANOS_PER_SECOND + fileinfo.st_mtim.tv_nsec;
        }
    }
}


void MomentumContext::set_shm_time(const std::string& shm_path, uint64_t read_ts, uint64_t write_ts) {
    uint64_t existing_read_ts;
    uint64_t existing_write_ts;
    struct timespec updated_times[2];
    
    std::string filepath(SHM_PATH_BASE + shm_path);

    // get the existing timestamps
    get_shm_time(shm_path, &existing_read_ts, &existing_write_ts);

    uint64_t next_read_ts = read_ts == 0 ? existing_read_ts : read_ts;
    uint64_t next_write_ts = write_ts == 0 ? existing_write_ts : write_ts;

    updated_times[0].tv_sec = next_read_ts / NANOS_PER_SECOND; 
    updated_times[0].tv_nsec = next_read_ts - (updated_times[0].tv_sec * NANOS_PER_SECOND);
    updated_times[1].tv_sec = next_write_ts / NANOS_PER_SECOND; 
    updated_times[1].tv_nsec = next_write_ts - (updated_times[1].tv_sec * NANOS_PER_SECOND);
    
    if (utimensat(AT_FDCWD, filepath.c_str(), updated_times, 0) < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to update file timestamp: " << filepath << std::endl;
        }
    }
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
            std::cerr << DEBUG_PREFIX << "Failed to access shm directory: " << SHM_PATH_BASE << std::endl;
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
    // stream must not contain path delimiter (i.e. "_")
    if (stream.find(std::string(PATH_DELIM)) != std::string::npos) {
        return false;
    } 

    return true;
}


void MomentumContext::normalize_stream(std::string& stream) {
    // convert to lowercase
    for (char &ch : stream) {
        ch = tolower(ch);
    }

    // remove the protocol string prefix if present
    if (stream.find(PROTOCOL) != std::string::npos) {
        stream.erase(0, PROTOCOL.size());
    } 
}

bool MomentumContext::lock_ex(const Buffer* buffer, bool block) {
    struct flock lck;
    memset(&lck, 0, sizeof(lck));
    lck.l_type = F_WRLCK;
    return fcntl(buffer->fd, block ? F_SETLKW : F_SETLK, &lck) > -1;
}

bool MomentumContext::lock_sh(const Buffer* buffer, bool block) {
    struct flock lck;
    memset(&lck, 0, sizeof(lck));
    lck.l_type = F_RDLCK;
    return fcntl(buffer->fd, block ? F_SETLKW : F_SETLK, &lck) > -1;
}

bool MomentumContext::lock_un(const Buffer* buffer) {
    struct flock lck;
    memset(&lck, 0, sizeof(lck));
    lck.l_type = F_UNLCK;
    return fcntl(buffer->fd, F_SETLK, &lck) > -1;
}

bool MomentumContext::test_lock_ex(const Buffer* buffer) const {
    struct flock lck;
    memset(&lck, 0, sizeof(lck));
    if (fcntl(buffer->fd, F_GETLK, &lck) < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to obtain status of lock for file: " << buffer->shm_path << " " << errno << std::endl;
        }
        return false;
    }
    if (lck.l_type == F_UNLCK) {
        // since locking will succeed if we already hold the lock, only return true if we don't hold the lock
        bool is_free = true;

        std::string _, file_id;
        pid_t pid;
        std::ifstream proc_locks("/proc/locks");
        while (proc_locks >> _ >> _ >> _ >> _ >> pid >> file_id >> _ >> _) {
            if (pid == _pid && file_id.find(":" + buffer->inode) != std::string::npos) {
                is_free = false;
                break;
            }
        }

        proc_locks.close();

        return is_free;
    } else {
        return false;
    }
}

std::string MomentumContext::to_mq_name(pid_t pid, const std::string& stream) {
    // Build the path to the underlying shm file 
    std::ostringstream oss;
    oss << "/" << NAMESPACE << PATH_DELIM << stream;
    if (pid > 0) {
        oss << PATH_DELIM << std::to_string(pid);
    }
    return oss.str();
}



MomentumContext* momentum_context() {
    MomentumContext* ctx = new MomentumContext();
    return ctx;
}

bool momentum_term(MomentumContext* ctx) {
    return ctx->term();
}

bool momentum_is_terminated(MomentumContext* ctx) {
    return ctx->is_terminated();
}

bool momentum_destroy(MomentumContext* ctx) {
    if (ctx != NULL) {
        delete ctx;
        return true;
    } else {
        return false;
    }
}

bool momentum_subscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->subscribe(std::string(stream), callback);
}

bool momentum_unsubscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->unsubscribe(std::string(stream), callback);
}

Buffer* momentum_acquire_buffer(MomentumContext* ctx, const char* stream, size_t length) {
    return ctx->acquire_buffer(std::string(stream), length);
}

bool momentum_release_buffer(MomentumContext* ctx, Buffer* buffer) {
    return ctx->release_buffer(buffer);
}

bool momentum_send_buffer(MomentumContext* ctx, Buffer* buffer, size_t length, uint64_t ts) {
    return ctx->send_buffer(buffer, length, ts ? ts : 0);
}

bool momentum_send_data(MomentumContext* ctx, const char* stream, uint8_t* data, size_t length, uint64_t ts) {
    return ctx->send_data(std::string(stream), data, length, ts ? ts : 0);
}

uint8_t* momentum_get_buffer_address(Buffer* buffer) {
    return buffer->address;
}

size_t momentum_get_buffer_length(Buffer* buffer) {
    return buffer->length;
}

bool momentum_configure(MomentumContext* ctx, uint8_t option, const void* value) {
    if (option == MOMENTUM_OPT_MIN_BUFFERS) {
        ctx->_min_buffers = (uint64_t) value;
    } else if (option == MOMENTUM_OPT_DEBUG) {
        ctx->_debug = (bool) value;
    }
    return true;
}
