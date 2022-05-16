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
#include <sys/select.h>

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

        _message_handler = std::thread([&] {
            Message message;
            int bytes_received;
            fd_set read_fds;
            mqd_t max_fd;

            while(!_terminating && !_terminated) {
                
                // build our set of read fds
                max_fd = 0;
                FD_ZERO(&read_fds);
                
                // add all consumer mqs to the list of read fds...
                std::set<mqd_t> all_fds;

                { 
                    std::lock_guard<std::mutex> lock(_consumer_mutex);
                    
                    for (auto const& consumer_stream : _consumer_streams) {
                        for (auto const& consumer_fd : _consumer_mqs_by_stream[consumer_stream]) {
                            all_fds.insert(consumer_fd);

                            FD_SET(consumer_fd, &read_fds);
                        
                            if (consumer_fd > max_fd) {
                                max_fd = consumer_fd;
                            }
                        }
                    }
                }

                // add all producer mqs to the list of read fds...
                {
                    std::lock_guard<std::mutex> lock(_producer_mutex);

                    for (auto const& producer_stream : _producer_streams) {
                        mqd_t producer_fd = _producer_mq_by_stream[producer_stream];
                        all_fds.insert(producer_fd);

                        FD_SET(producer_fd, &read_fds);
                        
                        if (producer_fd > max_fd) {
                            max_fd = producer_fd;
                        }
                    }
                }

                if (all_fds.size() == 0) {
                    usleep(1);
                    continue;
                }

                if (pselect(max_fd + 1, &read_fds, NULL, NULL, &MESSAGE_TIMEOUT, NULL) < 0) {
                    continue;
                }

                for (auto const& fd : all_fds) {
                    if (!FD_ISSET(fd, &read_fds)) {
                        continue;
                    }                

                    bytes_received = mq_receive(fd, (char *) &message, sizeof(Message), 0);
                    if (bytes_received < 0) {
                        continue;
                    }

                    switch(message.type) {
                        case Message::TERM_MESSAGE:
                            {
                                std::string stream(message.term_message.stream);
                                std::vector<callback_t> callbacks;
                                {
                                    std::lock_guard<std::mutex> lock(_consumer_mutex);
                                    callbacks = _callbacks_by_stream[stream];
                                }

                                for (auto const& callback : callbacks) {
                                    unsubscribe(stream, callback, false);
                                }
                            }

                            break;

                        case Message::BUFFER_MESSAGE:
                            {
                                // parse out buffer message variables
                                std::string stream(message.buffer_message.stream);
                                std::string buffer_shm_path(message.buffer_message.buffer_shm_path);
                                size_t buffer_length = message.buffer_message.buffer_length;
                                size_t data_length = message.buffer_message.data_length; 
                                uint64_t ts = message.buffer_message.ts;
                                long long int message_id = message.buffer_message.id;
                                bool blocking = message.buffer_message.blocking;

                                if (message_id != ADMIN_MESSAGE_ID) { 
                                    std::lock_guard<std::mutex> lock(_message_mutex);
                                    if (message_id <= _last_message_id_by_stream[stream]) {
                                        if (_debug) {
                                            std::cout << DEBUG_PREFIX << "Received stale/duplicate message id: " << message_id << std::endl;
                                        }
                                        continue;
                                    }
                                    _last_message_id_by_stream[stream] = message_id;
                                }

                                Buffer* buffer;    
                                bool is_new_buffer;

                                { 
                                    std::lock_guard<std::mutex> lock(_buffer_mutex);
                                    is_new_buffer = _buffer_by_shm_path.count(buffer_shm_path) == 0;
                                }

                                if (is_new_buffer) {
                                    buffer = allocate_buffer(buffer_shm_path, buffer_length, O_RDWR);
                                } else {
                                    { 
                                        std::lock_guard<std::mutex> lock(_buffer_mutex); 
                                        buffer = _buffer_by_shm_path[buffer_shm_path];
                                    }

                                    if (buffer->length < buffer_length) {
                                        resize_buffer(buffer, buffer_length);
                                    }
                                }
                                
                                // with the file locked, do a quick validity check to ensure the buffer has the same
                                // modified time as was provided in the message
                                uint64_t buffer_write_ts;
                                get_shm_time(buffer_shm_path, NULL, &buffer_write_ts);

                                bool buffer_unlinked = buffer_write_ts == 0;
                            
                                // if the producer who sent this message is in blocking mode, send them the ack
                                if (blocking) {
                                    std::lock_guard<std::mutex> lock(_producer_mutex);
                                    
                                    Message message;
                                    message.type = Message::ACK_MESSAGE;
                                    message.ack_message.id = message_id;
                                    message.ack_message.pid = _pid;

                                    if (!force_send(_producer_mq_by_stream[stream], &message, sizeof(Message))) {
                                        std::cout << DEBUG_PREFIX << "Failed to send ack message" << std::endl;
                                    }
                                }


                                if (flock(buffer->fd, LOCK_SH | LOCK_NB) < 0) {
                                    continue;
                                }

                                // if we made it here, we were able to access the buffer!
                                
                                if (buffer_unlinked || ts == buffer_write_ts) {
                                    // update buffer access (read) time
                                    set_shm_time(buffer_shm_path, now(), 0);
                                    
                                    std::lock_guard<std::mutex> lock(_consumer_mutex);
                                    for (auto const& callback : _callbacks_by_stream[stream]) {
                                        callback(buffer->address, data_length, buffer_length, message_id);  
                                    }
                                } else {
                                    // too late
                                }

                                if (flock(buffer->fd, LOCK_UN | LOCK_NB) < 0) {
                                    std::cerr << DEBUG_PREFIX << "Failed to release buffer file lock" << std::endl;
                                }

                            }

                            break;
                    
                        case Message::SUBSCRIBE_MESSAGE:
                            {
                                std::string stream(message.subscription_message.stream);
                                pid_t pid = message.subscription_message.pid;

                                std::string consumer_mq_name = to_mq_name(pid, stream);

                                // create the consumer_mq for this consumer
                                mqd_t consumer_mq;
                                consumer_mq = mq_open(consumer_mq_name.c_str(), O_RDWR | O_NONBLOCK);
                                if (consumer_mq < 0) {
                                    if (_debug) {
                                        std::cerr << DEBUG_PREFIX << "Failed to find mq for stream: " << stream << std::endl;
                                    }
                                } else {
                                    {
                                        std::unique_lock<std::mutex> lock(_consumer_mutex);

                                        _mq_by_mq_name[consumer_mq_name] = consumer_mq;
                                        _pid_by_consumer_mq[consumer_mq] = pid;
                                        _consumer_mqs_by_stream[stream].push_back(consumer_mq);
                                        if (_debug) {
                                            std::cout << DEBUG_PREFIX << "Added subscriber to stream: " << stream << std::endl;
                                        }
                                    }

                                    {
                                        // send the list of buffers to the consumer on subscribe
                                        std::lock_guard<std::mutex> buffer_lock(_buffer_mutex);
                                        for (auto const& buffer : _buffers_by_stream[stream]) {
                                            std::unique_lock<std::mutex> message_lock(_message_mutex);
                                            if (_last_message_by_shm_path.count(buffer->shm_path) > 0) {
                                                Message* message = _last_message_by_shm_path[buffer->shm_path];
                                                if (_blocking) {
                                                    force_send(consumer_mq, message, sizeof(Message));
                                                } else {
                                                    send(consumer_mq, message, sizeof(Message));
                                                }
                                            }
                                        }                                        
                                    }
                                }    

                                _consumer_availability.notify_all();
                                
                                // unlink the consumer file so that it does not dangle around after all instances are closed
                                mq_unlink(consumer_mq_name.c_str());
                            }    
                            break;

                        case Message::UNSUBSCRIBE_MESSAGE:
                            {
                                std::string stream(message.subscription_message.stream);
                                pid_t pid = message.subscription_message.pid;

                                std::string consumer_mq_name = to_mq_name(pid, stream);

                                mqd_t consumer_mq;
                                if (_mq_by_mq_name.count(consumer_mq_name) > 0) {
                                    {
                                        std::unique_lock<std::mutex> lock(_consumer_mutex);
                                        consumer_mq = _mq_by_mq_name[consumer_mq_name];
                                        mq_close(consumer_mq);
                                        _mq_by_mq_name.erase(consumer_mq_name);
                                        _pid_by_consumer_mq.erase(consumer_mq);

                                        _consumer_mqs_by_stream[stream].erase(
                                            std::remove(
                                                _consumer_mqs_by_stream[stream].begin(),
                                                _consumer_mqs_by_stream[stream].end(),
                                                consumer_mq
                                            ),
                                            _consumer_mqs_by_stream[stream].end()
                                        );

                                        if (_consumer_mqs_by_stream[stream].size() == 0) {
                                            _consumer_mqs_by_stream.erase(stream);
                                        }
                                    }
                                    {
                                        std::unique_lock<std::mutex> lock(_ack_mutex);
                                        _message_ids_pending_by_pid.erase(pid);
                                    }

                                    _consumer_availability.notify_all();
                                    _acks.notify_all();

                                    if (_debug) {
                                        std::cout << DEBUG_PREFIX << "Removed subscriber from stream: " << stream << std::endl;
                                    }
                                }
                            }

                            break;

                        case Message::ACK_MESSAGE:
                            {
                                pid_t pid = message.ack_message.pid;
                                long long int message_id = message.ack_message.id;

                                std::unique_lock<std::mutex> lock(_ack_mutex);
                                _message_ids_pending_by_pid[pid].erase(message_id);
                            }

                            _acks.notify_all();
                            break;
                    }
                }
            }   
        });

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
}

bool MomentumContext::is_terminated() const {
    return _terminated;
}

bool MomentumContext::term() {
    if (_terminated) return false;

    _terminating = true;

    _message_handler.join();

    // for every stream we are consuming, issue an unsubscribe
    for (auto const& stream : _consumer_streams) {
        for (auto const& callback : _callbacks_by_stream[stream]) {
            unsubscribe(stream, callback);
        }
    }
    
    // for every stream we are producing, notify the consumers that we are terminating
    for (auto const& stream : _producer_streams) {

        std::string producer_mq_name = to_mq_name(0, stream);

        Message message;
        message.type = Message::TERM_MESSAGE;
        strcpy(message.term_message.stream, stream.c_str());
        
        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
            if (!force_send(consumer_mq, &message, sizeof(Message))) {
                if (_debug) {
                    std::cerr << DEBUG_PREFIX << "Failed to notify term for stream: " << stream << std::endl;
                }
            }
        }

        mq_close(_producer_mq_by_stream[stream]);
        if (mq_unlink(producer_mq_name.c_str()) < 0) {
            if (_debug) {
                std::cout << DEBUG_PREFIX << "Failed to unlink producer mq for stream: " << stream << std::endl;

            }
        }
    }

    // iterate over a copy of the buffer by shm_path map, deallocating all buffers
    for (auto const& tuple : std::map<std::string, Buffer*>(_buffer_by_shm_path)) {
        deallocate_buffer(tuple.second);
    }

    _terminated = true;

    return true;
};

bool MomentumContext::is_subscribed(std::string stream, callback_t callback) {
    if (_terminated) return false;

    normalize_stream(stream); 
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    }

    { 
        std::lock_guard<std::mutex> lock(_consumer_mutex);
        if (_callbacks_by_stream.count(stream) > 0) {
            std::vector<callback_t>::iterator begin = _callbacks_by_stream[stream].begin();
            std::vector<callback_t>::iterator end = _callbacks_by_stream[stream].end();

            bool contains_callback = std::find(begin, end, callback) != end;
            
            return contains_callback;
        }
    }

    return false;
}

bool MomentumContext::is_stream_available(std::string stream) {
    if (_terminated) return false;

    normalize_stream(stream); 
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    }

    // Attempt to open the producer mq  
    std::string producer_mq_name = to_mq_name(0, stream);
    mqd_t producer_mq = mq_open(producer_mq_name.c_str(), O_RDONLY | O_NONBLOCK);
    if (producer_mq < 0) {
        return false;
    } else {
        return true;
    }
}

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
    mqd_t producer_mq = mq_open(producer_mq_name.c_str(), O_RDWR | O_NONBLOCK);
    if (producer_mq < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to find mq for stream: " << stream << std::endl;
        }
        return false;
    } 

    // ...and then open our own consumer mq
    struct mq_attr attrs;
    memset(&attrs, 0, sizeof(mq_attr));
    attrs.mq_maxmsg = 1;
    attrs.mq_msgsize = sizeof(Message);
    std::string consumer_mq_name = to_mq_name(_pid, stream);
    mqd_t consumer_mq = mq_open(consumer_mq_name.c_str(), O_RDWR | O_CREAT | O_NONBLOCK, MQ_MODE, &attrs);
    if (consumer_mq < 0) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to create mq for stream: " << stream << std::endl;
        }
        mq_close(producer_mq);
        return false;
    }

    Message message;
    message.type = Message::SUBSCRIBE_MESSAGE;
    message.subscription_message.pid = _pid;
    strcpy(message.subscription_message.stream, stream.c_str());

    if (!force_send(producer_mq, &message, sizeof(Message))) {
        // we failed for a true failure condition, so cleanup and return false
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
    }
    
    // If we made it here, everything went well so cache references

    {
        std::unique_lock<std::mutex> lock(_consumer_mutex);

        if (_consumer_mqs_by_stream.count(stream) == 0) {
            _consumer_mqs_by_stream[stream] = std::vector<mqd_t>();
        }
        _consumer_mqs_by_stream[stream].push_back(consumer_mq);


        if (!_callbacks_by_stream.count(stream)) {
            _callbacks_by_stream[stream] = std::vector<callback_t>();
        }
        _callbacks_by_stream[stream].push_back(callback);
    
        _consumer_streams.insert(stream);
    }

    {
        std::unique_lock<std::mutex> lock(_producer_mutex);
        _producer_mq_by_stream[stream] = producer_mq;
    }
    
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Subscribed to stream: " << stream << std::endl;
    }

    return true;
}

bool MomentumContext::unsubscribe(std::string stream, callback_t callback, bool notify) {
    if (_terminated) return false; // only disallow terminated if we are truly terminated (i.e. not terminating)

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream " << stream << std::endl;
        }
        return false;
    } 

    // exit early if we are not subscribed to this stream
    {

        std::lock_guard<std::mutex> lock(_consumer_mutex);
        if (_consumer_streams.count(stream) == 0) {
            return false;
        }
    }

    // deallocate the buffers we've allocated for this stream 
    std::list<Buffer*> buffers;
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        buffers = _buffers_by_stream[stream];
    }   

    for (auto const& buffer : buffers) {
        deallocate_buffer(buffer);
    }

    {
        std::unique_lock<std::mutex> lock(_producer_mutex);

        if (notify) {
            Message message;
            message.type = Message::UNSUBSCRIBE_MESSAGE;
            message.subscription_message.pid = _pid;
            strcpy(message.subscription_message.stream, stream.c_str());


            if (!force_send(_producer_mq_by_stream[stream], &message, sizeof(Message))) {
                // unexepected error occurred so we must return from this function
                if (_debug) {
                    std::cerr << DEBUG_PREFIX << "Failed to send unsubscribe message for stream: " << stream << std::endl;
                }
                return false;
            }
        }

        mq_close(_producer_mq_by_stream[stream]);
        _producer_mq_by_stream.erase(stream);
    }

    { 
        std::lock_guard<std::mutex> lock(_consumer_mutex);
        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
            mq_close(consumer_mq);
        }
        _consumer_mqs_by_stream.erase(stream);
        
        _consumer_streams.erase(stream);

        if (_callbacks_by_stream.count(stream) > 0) {
            _callbacks_by_stream[stream].erase(
                std::remove(
                    _callbacks_by_stream[stream].begin(), 
                    _callbacks_by_stream[stream].end(), 
                    callback
                ), 
                _callbacks_by_stream[stream].end()
            );
        }
    }

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Unsubscribed from stream: " << stream << std::endl;
    }

    return true;
}

bool MomentumContext::send_string(std::string stream, const char* data, size_t length, uint64_t ts) {
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
    
    return release_buffer(buffer, length, ts);
}


bool MomentumContext::release_buffer(Buffer* buffer, size_t length, uint64_t ts) {
    if (_terminated) return false;
    
    if (buffer == NULL) return false;

    if (ts == 0) {
        ts = now();
    }

    // update buffer modify (write) time
    set_shm_time(buffer->shm_path, 0, ts);

    std::string stream = stream_from_shm_path(buffer->shm_path);

    // unlock the buffer prior to sending
    flock(buffer->fd, LOCK_UN | (_blocking ? 0 : LOCK_NB));
        

    std::set<pid_t> consumer_pids;
    std::vector<mqd_t> consumer_mqs;

    {
        std::unique_lock<std::mutex> lock(_consumer_mutex);
        if (_blocking) {
            _consumer_availability.wait(
                lock,
                [&] { 
                    return _consumer_mqs_by_stream[stream].size() > 0; 
                }
            );
        }
        consumer_mqs = _consumer_mqs_by_stream[stream];
    }

    long long int message_id;
    {
        std::lock_guard<std::mutex> lock(_message_mutex);
        message_id = _message_id_by_stream[stream] = _message_id_by_stream[stream] + 1;
    } 

    // create the buffer message
    Message* message = new Message();
    message->type = Message::BUFFER_MESSAGE;
    message->buffer_message.id = message_id;
    message->buffer_message.buffer_length = buffer->length;
    message->buffer_message.data_length = length;
    message->buffer_message.ts = ts;
    message->buffer_message.blocking = _blocking;
    strcpy(message->buffer_message.stream, stream.c_str());
    strcpy(message->buffer_message.buffer_shm_path, buffer->shm_path);

    // index this message by buffer shm path
    {
        std::lock_guard<std::mutex> lock(_message_mutex);
        if (_last_message_by_shm_path.count(buffer->shm_path) > 0) {
            delete _last_message_by_shm_path[buffer->shm_path];
        }
        _last_message_by_shm_path[buffer->shm_path] = message;
    }

    // send our message to each consumer
    for (const auto& consumer_mq : consumer_mqs) {
        if (_blocking) {
            pid_t consumer_pid;
            {
                std::lock_guard<std::mutex> lock(_consumer_mutex);
                consumer_pid = _pid_by_consumer_mq.count(consumer_mq) > 0 ? _pid_by_consumer_mq[consumer_mq] : 0;
                if (consumer_pid == 0) {
                    continue;
                } else {
                    consumer_pids.insert(consumer_pid);
                }
            }

            {
                std::unique_lock<std::mutex> lock(_ack_mutex);
                _message_ids_pending_by_pid[consumer_pid].insert(message_id);
            }
            
            if (!force_send(consumer_mq, message, sizeof(Message))) {
                {
                    // send failed, so remove this message from the list of pending
                    std::unique_lock<std::mutex> lock(_ack_mutex);
                    _message_ids_pending_by_pid[consumer_pid].erase(message_id);
                }

                std::cerr << DEBUG_PREFIX << "Failed to send buffer message to consumer" << std::endl;
                return false;
            }
        } else {
            send(consumer_mq, message, sizeof(Message));
        } 
    }

    // if blocking, wait for acknowledgement prior to returning
    if (_blocking) {
        // waiting for acknowledgement
        std::unique_lock<std::mutex> lock(_ack_mutex);
        _acks.wait(lock, [&] { 
            bool all_acknowledged = true;

            for (auto const& consumer_pid : consumer_pids) {
                if (_message_ids_pending_by_pid.count(consumer_pid) == 0) {
                    // consumer exited, so consider it acknowledged
                    all_acknowledged &= true;
                } else { 
                    bool message_acknowledged = _message_ids_pending_by_pid[consumer_pid].find(message_id) == _message_ids_pending_by_pid[consumer_pid].end();
                    all_acknowledged &= message_acknowledged;
                }
            }

            return all_acknowledged;
        });
    } 

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

    {
        std::lock_guard<std::mutex> lock(_producer_mutex);
        if (_producer_mq_by_stream.count(stream) == 0) {
            struct mq_attr attrs;
            memset(&attrs, 0, sizeof(mq_attr));
            attrs.mq_maxmsg = 1;
            attrs.mq_msgsize = sizeof(Message);

            std::string producer_mq_name = to_mq_name(0, stream);
            mq_unlink(producer_mq_name.c_str());
            mqd_t producer_mq = mq_open(producer_mq_name.c_str(), O_RDWR | O_CREAT | O_EXCL | O_NONBLOCK, MQ_MODE, &attrs);
            if (producer_mq < 0) {
                if (_debug) {
                    std::cerr << DEBUG_PREFIX << "Failed to open mq for stream: " << stream << std::endl;
                }
                return false;
            }

            _producer_mq_by_stream[stream] = producer_mq;        
        }

        _producer_streams.insert(stream);
    }

    Buffer* buffer = NULL;
    bool below_minimum_buffers;
    
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
    
        size_t visit_count = 0;

        // find the next buffer to use
        while (visit_count++ < _buffers_by_stream[stream].size()) {
            // pull a candidate buffer, rotating the queue in the process
            Buffer* candidate_buffer = _buffers_by_stream[stream].front();
            _buffers_by_stream[stream].pop_front();
            _buffers_by_stream[stream].push_back(candidate_buffer);

            if (candidate_buffer != _last_acquired_buffer_by_stream[stream]) {
                // found a buffer that is different than the last iteration...
                if (flock(candidate_buffer->fd, LOCK_EX | LOCK_NB) > -1) {
                    // and we were also able to set the exclusive lock... 
                    buffer = candidate_buffer;
                    break;
                }
            }
        }
        below_minimum_buffers = _buffers_by_stream[stream].size() < _min_buffers;
    }

    // If we don't have enough buffers, then create them...

    if (buffer == NULL || below_minimum_buffers) {
        size_t allocations_required = below_minimum_buffers ? _min_buffers.load(std::memory_order_relaxed) : 1;

        Buffer* first_buffer = NULL;
        int id = 1; // start buffer count at 1

        while (allocations_required > 0) {
            buffer = allocate_buffer(to_shm_path(_pid, stream, id++), length, O_RDWR | O_CREAT | O_EXCL);
            
            if (buffer != NULL) {
                allocations_required--;
                
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

    { 
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        _last_acquired_buffer_by_stream[stream] = buffer;
    }

    return buffer;
}

bool MomentumContext::get_debug() {
    return _debug;
}

void MomentumContext::set_debug(bool value) {
    _debug = value;
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Option 'debug' set to value: " << value << std::endl;
    }
}

size_t MomentumContext::get_min_buffers() {
    return _min_buffers;
}

void MomentumContext::set_min_buffers(size_t value) {
    _min_buffers = value;
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Option 'min_buffers' set to value: " << value << std::endl;
    }
}

size_t MomentumContext::get_max_buffers() {
    return _max_buffers;
}

void MomentumContext::set_max_buffers(size_t value) {
    _max_buffers = value;
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Option 'max_buffers' set to value: " << value << std::endl;
    }
}

bool MomentumContext::get_blocking() {
    return _blocking;
}

void MomentumContext::set_blocking(bool value) {
    _blocking = value;
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Option 'blocking' set to value: " << value << std::endl;
    }
}

// BEGIN PRIVATE METHODS

Buffer* MomentumContext::allocate_buffer(const std::string& shm_path, size_t length, int flags) {
    if (_terminated) return NULL;

    std::string stream = stream_from_shm_path(shm_path);

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

    Buffer* buffer = new Buffer();
    buffer->fd = fd;
    strcpy(buffer->shm_path, shm_path.c_str());

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Allocated and mapped shm file: " << buffer->shm_path << std::endl;
    }

    resize_buffer(buffer, length, (flags & O_CREAT) == O_CREAT);

    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        _buffer_by_shm_path[shm_path] = buffer;
        _buffers_by_stream[stream].push_back(buffer);
    }


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

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Resized shm file: " << buffer->shm_path << std::endl;
    }
}

void MomentumContext::deallocate_buffer(Buffer* buffer) {
    std::string stream = stream_from_shm_path(buffer->shm_path);

    std::lock_guard<std::mutex> lock(_buffer_mutex);

    if (_buffer_by_shm_path.count(buffer->shm_path) > 0) {
        munmap(buffer->address, buffer->length);
        close(buffer->fd);

        _buffer_by_shm_path.erase(buffer->shm_path);
        _buffers_by_stream[stream].remove(buffer);

        if (_last_acquired_buffer_by_stream[stream] == buffer) {
            _last_acquired_buffer_by_stream[stream] = NULL;
        }

        if (_debug) {
            std::cout << DEBUG_PREFIX << "Deallocated shm file: " << buffer->shm_path << std::endl;
        }
    
        delete buffer;
    }
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

bool MomentumContext::send(mqd_t mq, Message* message, size_t length, int priority) {
    return mq_send(mq, (const char*) message, length, priority) > -1;
}

bool MomentumContext::force_send(mqd_t mq, Message* message, size_t length, int priority) {
    while (!send(mq, message, length, priority)) {
        if (errno == EAGAIN) {
            // recipient was busy, so try again with higher priority 
            priority++;
            if (usleep(1) < 0) {
                return false;
            }
        } else {
            // an unexpected error occurred
            return false;
        }
    }
    return true;
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

bool momentum_get_debug(MomentumContext* ctx) {
    return ctx->get_debug();
}

void momentum_set_debug(MomentumContext* ctx, bool value) {
    ctx->set_debug(value);
}

size_t momentum_get_min_buffers(MomentumContext* ctx) {
    return ctx->get_min_buffers();
}

void momentum_set_min_buffers(MomentumContext* ctx, size_t value) {
    ctx->set_min_buffers(value);
}

size_t momentum_get_max_buffers(MomentumContext* ctx) {
    return ctx->get_max_buffers();
}

void momentum_set_max_buffers(MomentumContext* ctx, size_t value) {
    ctx->set_max_buffers(value);
}

bool momentum_get_blocking(MomentumContext* ctx) {
    return ctx->get_blocking();
}

void momentum_set_blocking(MomentumContext* ctx, bool value) {
    ctx->set_blocking(value);
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

bool momentum_is_stream_available(MomentumContext* ctx, const char* stream) {
    return ctx->is_stream_available(std::string(stream));
}

bool momentum_is_subscribed(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->is_subscribed(std::string(stream), callback);
}

bool momentum_subscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->subscribe(std::string(stream), callback);
}

bool momentum_unsubscribe(MomentumContext* ctx, const char* stream, callback_t callback) {
    return ctx->unsubscribe(std::string(stream), callback);
}

bool momentum_send_string(MomentumContext* ctx, const char* stream, const char* data, size_t length, uint64_t ts) {
    return ctx->send_string(std::string(stream), data, length, ts ? ts : 0);
}

Buffer* momentum_acquire_buffer(MomentumContext* ctx, const char* stream, size_t length) {
    return ctx->acquire_buffer(std::string(stream), length);
}

bool momentum_release_buffer(MomentumContext* ctx, Buffer* buffer, size_t length, uint64_t ts) {
    return ctx->release_buffer(buffer, length, ts ? ts : 0);
}

uint8_t* momentum_get_buffer_address(Buffer* buffer) {
    return buffer->address;
}

size_t momentum_get_buffer_length(Buffer* buffer) {
    return buffer->length;
}