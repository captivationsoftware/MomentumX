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

MomentumContext::MomentumContext() {
    // Get our pid
    _pid = getpid();

    if (fork() != 0) {
        // Parent process 

        _message_handler = std::thread([&] {
            Message message;
            int bytes_received;
            fd_set read_fds;
            mqd_t max_fd;

            while(!_terminating && !_terminated) {

                // build our set of read fds
                max_fd = 0;
                FD_ZERO(&read_fds);
                
                std::set<mqd_t> all_fds, consumer_streams, producer_streams;

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
                                unsubscribe(stream, false);
                            }

                            break;

                        case Message::BUFFER_MESSAGE:
                            {
                                // parse out buffer message variables
                                std::string stream(message.buffer_message.stream);
                                std::string buffer_shm_path(message.buffer_message.buffer_shm_path);
                                size_t buffer_length = message.buffer_message.buffer_length;
                                uint64_t iteration = message.buffer_message.iteration;
                                
                                {
                                    std::lock_guard<std::mutex> lock(_message_mutex);
                                    if (iteration <= _last_iteration_by_stream[stream]) {
                                        if (_debug) {
                                            std::cout << DEBUG_PREFIX << "Received duplicate message for iteration: " << iteration << std::endl;
                                        }
                                        continue;
                                    }
                                    _last_iteration_by_stream[stream] = iteration;
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
                                    size_t current_buffer_length;
                                    { 
                                        std::lock_guard<std::mutex> lock(_buffer_mutex); 
                                        buffer = _buffer_by_shm_path[buffer_shm_path];
                                        current_buffer_length = buffer->length;
                                    }

                                    if (current_buffer_length < buffer_length) {
                                        resize_buffer(buffer, buffer_length);
                                    }
                                }

                                // add to queue
                                { 
                                    std::lock_guard<std::mutex> lock(_message_mutex);
                                    _consumer_messages_by_stream[stream].remove_if([&](Message& m) { 
                                        return strcmp(m.buffer_message.buffer_shm_path, buffer_shm_path.c_str()) == 0;
                                    });
                                    _consumer_messages_by_stream[stream].push_back(message);
                                }
                            }

                            break;

                        case Message::SUBSCRIBE_ACK_MESSAGE:
                            {
                                std::string stream(message.subscription_ack_message.stream);
                                pid_t producer_pid = message.subscription_ack_message.pid;
                                
                                {
                                    std::unique_lock<std::mutex> lock(_subscription_ack_mutex);
                                    _pending_stream_subscriptions.erase(stream);
                                }

                                {
                                    std::lock_guard<std::mutex> lock(_producer_mutex);
                                    _producer_pid_by_stream[stream] = producer_pid;
                                }

                            }

                            _subscription_acks.notify_all();

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
                                    Message subscription_ack_message;
                                    subscription_ack_message.type = Message::SUBSCRIBE_ACK_MESSAGE;
                                    subscription_ack_message.subscription_ack_message.pid = _pid;
                                    strcpy(subscription_ack_message.subscription_ack_message.stream, stream.c_str());
                                    if (!notify(consumer_mq, &subscription_ack_message, HIGH_PRIORITY)) {
                                        mq_close(consumer_mq);
                                        if (_debug) {
                                            std::cerr << DEBUG_PREFIX << "Failed to send subscription ack message for stream: " << stream << std::endl;
                                        }
                                        continue;
                                    } 

                                    // if we made it here, everything is looking good, so save our lookups!
                                    {
                                        std::unique_lock<std::mutex> lock(_consumer_mutex);

                                        _mq_by_mq_name[consumer_mq_name] = consumer_mq;
                                        _pid_by_consumer_mq[consumer_mq] = pid;
                                        _consumer_mqs_by_stream[stream].push_back(consumer_mq);
                                        _streams_by_consumer_pid[pid].insert(stream);

                                        if (_debug) {
                                            std::cout << DEBUG_PREFIX << "Added subscriber to stream: " << stream << std::endl;
                                        }
                                    }

                                    std::list<Buffer*> buffers;
                                    {
                                        // send the list of buffers to the consumer on subscribe
                                        std::lock_guard<std::mutex> buffer_lock(_buffer_mutex);
                                        buffers = _buffers_by_stream[stream];
                                    }

                                    Message* buffer_message = NULL;
                                    for (auto const& buffer : buffers) {
                                        std::string shm_path;
                                        {
                                            std::lock_guard<std::mutex> buffer_lock(_buffer_mutex);
                                            shm_path = buffer->shm_path;
                                        }

                                        {
                                            std::unique_lock<std::mutex> message_lock(_message_mutex);
                                            if (_producer_message_by_shm_path.count(shm_path) > 0) {
                                                buffer_message = _producer_message_by_shm_path[shm_path];
                                            }
                                        }

                                        if (buffer_message != NULL) {
                                            notify(consumer_mq, buffer_message, HIGH_PRIORITY);
                                        }
                                    }
                                }    

                                // unlink the consumer file so that it does not dangle around after all instances are closed
                                mq_unlink(consumer_mq_name.c_str());
                            }    
                            break;

                        case Message::UNSUBSCRIBE_MESSAGE:
                            {
                                std::string stream(message.subscription_message.stream);
                                pid_t pid = message.subscription_message.pid;

                                remove_consumer(pid, stream);
                            }

                            break;

                        case Message::BUFFER_ACK_MESSAGE:
                            {
                                pid_t pid = message.buffer_ack_message.pid;
                                uint64_t iteration = message.buffer_ack_message.iteration;

                                std::unique_lock<std::mutex> lock(_buffer_ack_mutex);
                                _iterations_pending_by_pid[pid].erase(iteration);
        
                            }

                            _buffer_acks.notify_all();
                            break;
                    }
                }
            }   
        });

        _process_watcher = std::thread([&] {
            while (!_terminated && !_terminating) { 
                std::map<pid_t, std::set<std::string>> streams_by_consumer_pid;
                {
                    std::lock_guard<std::mutex> lock(_consumer_mutex);
                    streams_by_consumer_pid = _streams_by_consumer_pid;
                }

                for (auto const& tuple : streams_by_consumer_pid) {
                    if (getpgid(tuple.first) < 0) {
                        for (auto const& stream : tuple.second) {
                            remove_consumer(tuple.first, stream);
                        }
                    }
                }

                std::map<pid_t, std::set<std::string>> streams_by_producer_pid;
                {
                    std::lock_guard<std::mutex> lock(_producer_mutex);
                    streams_by_producer_pid = _streams_by_producer_pid;
                }

                for (auto const& tuple : streams_by_producer_pid) {
                    // Send empty signal 0 to the producer pid (DOES NOT ACTUALLY KILL IT!!!),
                    // and if it returns -1, then the pid no longer exists
                    if (kill(tuple.first, 0) < 0) {
                        for (auto const& stream : streams_by_producer_pid[tuple.first]) {
                            if (_debug) {
                                std::cerr << DEBUG_PREFIX << "Producer exited without sending unsubscribe for stream: " << stream << std::endl;
                            }
                            unsubscribe(stream, false);

                            // clean up any dangling parent shm files
                            dir_iter(SHM_PATH_BASE, [&](std::string filename) {
                                if (filename.rfind(std::string(NAMESPACE + PATH_DELIM + std::to_string(tuple.first) + PATH_DELIM)) == 0) {
                                    shm_unlink(filename.c_str());

                                }
                            });

                            // clean up any dangling producer mq
                            mq_unlink((to_mq_name(0, stream)).c_str());

                        }
                    }   
                }

                if (usleep(100e3) < 0) {
                    break;
                }
            }
        });

    } else {
        // Child process
        
        // wait for our parent to die
        while (getppid() == _pid) {
            sleep(1);
        }

        // clean up any dangling shm files
        dir_iter(SHM_PATH_BASE, [&](std::string filename) {
            if (filename.rfind(std::string(NAMESPACE + PATH_DELIM + std::to_string(_pid) + PATH_DELIM)) == 0) {
                shm_unlink(filename.c_str());
            }
        });

        // clean up any dangling mq files
        dir_iter(MQ_PATH_BASE, [&](std::string filename) {
            std::string suffix(PATH_DELIM + std::to_string(_pid));
            bool starts_with_namespace = filename.rfind(NAMESPACE) == 0;
            bool ends_with_pid = filename.rfind(suffix) == filename.size() - suffix.size();
            if (starts_with_namespace && ends_with_pid) {
                mq_unlink(("/" + filename).c_str());
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

    // for every stream we are consuming, issue an unsubscribe
    for (auto const& stream : _consumer_streams) {
        unsubscribe(stream);
    }

    _message_handler.join();
    _process_watcher.join();
    
    // for every stream we are producing, notify the consumers that we are terminating
    for (auto const& stream : _producer_streams) {

        std::string producer_mq_name = to_mq_name(0, stream);

        Message message;
        message.type = Message::TERM_MESSAGE;
        strcpy(message.term_message.stream, stream.c_str());
        
        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
            if (!notify(consumer_mq, &message, HIGH_PRIORITY)) {
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

bool MomentumContext::is_subscribed(std::string stream) {
    if (_terminated) return false;

    normalize_stream(stream); 
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    }
    
    std::lock_guard<std::mutex> lock(_consumer_mutex);
    bool contains_stream = std::find(_consumer_streams.begin(), _consumer_streams.end(), stream) != _consumer_streams.end();
    return contains_stream;
}

bool MomentumContext::subscribe(std::string stream) {
    if (_terminated) return false;

    normalize_stream(stream); 
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return false;
    }

    // short circuit if we're already subscribed
    if (is_subscribed(stream)) {
        if (_debug) {
            std::cout << DEBUG_PREFIX << "Already subscribed to stream: " << stream << std::endl;
        }
        return true;
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
    attrs.mq_maxmsg = 10;
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

    if (!notify(producer_mq, &message, HIGH_PRIORITY)) {
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

    // If we made it here, everything went well so cache references!
    {
        std::unique_lock<std::mutex> lock(_consumer_mutex);

        if (_consumer_mqs_by_stream.count(stream) == 0) {
            _consumer_mqs_by_stream[stream] = std::vector<mqd_t>();
        }
        _consumer_mqs_by_stream[stream].push_back(consumer_mq);

        _consumer_streams.insert(stream);
        _mq_by_mq_name[consumer_mq_name] = consumer_mq;
    }

    bool ack_received;
    {
        // waiting up to 1 second for subscription ack
        std::unique_lock<std::mutex> lock(_subscription_ack_mutex);
        _pending_stream_subscriptions.insert(stream);
        
        ack_received = _subscription_acks.wait_for(lock, std::chrono::seconds(1), [&] { 
            return _pending_stream_subscriptions.find(stream) == _pending_stream_subscriptions.end();
        });
    }


    if (!ack_received) {
        mq_close(producer_mq);
        mq_close(consumer_mq);

        if (_debug) {
            std::cout << DEBUG_PREFIX << "Timed out awaiting subscription acknowledgement for stream: " << stream << std::endl;
        }

        if (mq_unlink(consumer_mq_name.c_str()) < 0) {
            if (_debug) {        
                std::cerr << DEBUG_PREFIX << "Failed to unlink consumer mq for stream: " << stream << std::endl;
            }
        }

        // Spoke too soon there, undo everything we just added!
        {
            std::unique_lock<std::mutex> lock(_consumer_mutex);

            _consumer_mqs_by_stream[stream].erase(
                std::remove(
                    _consumer_mqs_by_stream[stream].begin(),
                    _consumer_mqs_by_stream[stream].end(),
                    consumer_mq
                ),
                _consumer_mqs_by_stream[stream].end()
            );
            _consumer_streams.erase(stream);
            _mq_by_mq_name.erase(consumer_mq_name);
        }

        return false;
    }
    
    {
        std::unique_lock<std::mutex> lock(_producer_mutex);
        _producer_mq_by_stream[stream] = producer_mq;

        pid_t producer_pid = _producer_pid_by_stream[stream];
        _streams_by_producer_pid[producer_pid].insert(stream);
    }
    
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Subscribed to stream: " << stream << std::endl;
    }

    return true;
}

bool MomentumContext::unsubscribe(std::string stream, bool notify_producer) {
    if (_terminated) return false; // only disallow terminated if we are truly terminated (i.e. not terminating)

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream " << stream << std::endl;
        }
        return false;
    } 

    // short circuit if we're already subscribed
    if (!is_subscribed(stream)) {
        if (_debug) {
            std::cout << DEBUG_PREFIX << "Already unsubscribed from stream: " << stream << std::endl;
        }
        return true;
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

        if (notify_producer) {
            Message message;
            message.type = Message::UNSUBSCRIBE_MESSAGE;
            message.subscription_message.pid = _pid;
            strcpy(message.subscription_message.stream, stream.c_str());


            if (!notify(_producer_mq_by_stream[stream], &message, HIGH_PRIORITY)) {
                // unexepected error occurred so we must return from this function
                if (_debug) {
                    std::cerr << DEBUG_PREFIX << "Failed to send unsubscribe message for stream: " << stream << std::endl;
                }
                return false;
            }
        }

        mq_close(_producer_mq_by_stream[stream]);
        _producer_mq_by_stream.erase(stream);

        pid_t producer_pid = _producer_pid_by_stream[stream];
        _streams_by_producer_pid[producer_pid].erase(stream);
    }

    { 
        std::lock_guard<std::mutex> lock(_consumer_mutex);
        for (auto const& consumer_mq : _consumer_mqs_by_stream[stream]) {
            mq_close(consumer_mq);
        }
        _consumer_mqs_by_stream.erase(stream);
        _consumer_streams.erase(stream);
    }

    {
        std::lock_guard<std::mutex> lock(_message_mutex); 
        _last_iteration_by_stream.erase(stream);
        _consumer_messages_by_stream.erase(stream);
    }

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Unsubscribed from stream: " << stream << std::endl;
    }

    return true;
}

BufferData* MomentumContext::receive_buffer(std::string stream) {
    if (_terminated) return NULL; // only disallow terminated if we are truly terminated (i.e. not terminating)

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream " << stream << std::endl;
        }
        return NULL;
    } 

    if (!is_subscribed(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Attempted to receive buffer without subscription for stream: " << stream << std::endl;
        }
        return NULL;
    }

    Buffer* buffer = NULL;
    bool has_next = true;
    Message buffer_message;
    
    int buffer_fd;
    std::string buffer_shm_path;
    while (has_next) {
        { 
            std::lock_guard<std::mutex> lock(_message_mutex);
            if (_consumer_messages_by_stream[stream].size() == 0) {
                return NULL;
            }

            buffer_message = _consumer_messages_by_stream[stream].front();
            _consumer_messages_by_stream[stream].pop_front();

            has_next = _consumer_messages_by_stream[stream].size() > 0;
        }

        {
            std::lock_guard<std::mutex> lock(_buffer_mutex);
            buffer = _buffer_by_shm_path[buffer_message.buffer_message.buffer_shm_path];
            buffer_fd = buffer->fd;
            buffer_shm_path = buffer->shm_path;
        }

        // attempt to acquire the lock on this buffer
        if (flock(buffer_fd, LOCK_SH | LOCK_NB) < 0) {
            // could not acquire, skip to next message
            buffer = NULL;
            continue;
        }

        // Ensure the buffer has the same has the same modified time as was provided in the message
        uint64_t buffer_write_ts;
        get_shm_time(buffer_shm_path, NULL, &buffer_write_ts);
        if (buffer_message.buffer_message.ts != buffer_write_ts) {
            // this message is old, so unlock and continue to the next
            flock(buffer_fd, LOCK_UN | LOCK_NB);
            buffer = NULL;
            continue; 
        }

        // if we made it here, we were able to access the buffer!
        break;
    }

    if (buffer == NULL) {
        return NULL;
    }

    // update buffer access (read) time
    set_shm_time(buffer_shm_path, now(), 0);

    BufferData* buffer_data = new BufferData();
    buffer_data->buffer = buffer;
    buffer_data->data_length = buffer_message.buffer_message.data_length;
    buffer_data->ts = buffer_message.buffer_message.ts;
    buffer_data->iteration = buffer_message.buffer_message.iteration;
    buffer_data->sync = buffer_message.buffer_message.sync;

    return buffer_data;
}

bool MomentumContext::release_buffer(std::string stream, BufferData* buffer_data) {
    if (_terminated) return false; // only disallow terminated if we are truly terminated (i.e. not terminating)

    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream " << stream << std::endl;
        }
        return false;
    } 

    int buffer_fd = buffer_data->buffer->fd;
    uint64_t iteration = buffer_data->iteration;
    bool sync = buffer_data->sync;

    delete buffer_data;

    if (flock(buffer_fd, LOCK_UN | LOCK_NB) < 0) {
        std::cerr << DEBUG_PREFIX << "Failed to release buffer file lock" << std::endl;
        return false;
    }

    // if the producer who sent this message is in blocking mode, send them the ack
    if (sync) {
        Message buffer_ack_message;
        buffer_ack_message.type = Message::BUFFER_ACK_MESSAGE;
        buffer_ack_message.buffer_ack_message.iteration = iteration;
        buffer_ack_message.buffer_ack_message.pid = _pid;

        std::lock_guard<std::mutex> lock(_producer_mutex);
        if (!notify(_producer_mq_by_stream[stream], &buffer_ack_message)) {
            std::cout << DEBUG_PREFIX << "Failed to send ack message" << std::endl;
            return false;
        }
    }
    
    return true;
}


bool MomentumContext::send_buffer(Buffer* buffer, size_t length, uint64_t ts) {
    if (_terminated) return false;
    
    if (buffer == NULL) return false;

    if (ts == 0) {
        ts = now();
    }

    std::string buffer_shm_path;
    size_t buffer_length;
    int buffer_fd;
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        buffer_shm_path = buffer->shm_path;
        buffer_length = buffer->length;
        buffer_fd = buffer->fd;
    }

    // update buffer modify (write) time
    set_shm_time(buffer_shm_path, 0, ts);

    std::string stream = stream_from_shm_path(buffer_shm_path);

    // unlock the buffer prior to sending
    flock(buffer_fd, LOCK_UN | (_sync ? 0 : LOCK_NB));
        
    {
        // if in synchronous mode, we need consumers, so fail if there are none
        std::lock_guard<std::mutex> consumer_lock(_consumer_mutex);
        if (_sync && _consumer_mqs_by_stream[stream].size() == 0) {
            return false;
        }
    }

    std::set<pid_t> consumer_pids;
    std::vector<mqd_t> consumer_mqs;

    {
        std::lock_guard<std::mutex> lock(_consumer_mutex);
        consumer_mqs = _consumer_mqs_by_stream[stream];
    }

    uint64_t iteration;
    {
        std::lock_guard<std::mutex> lock(_message_mutex);
        iteration = _iteration_by_stream[stream] = _iteration_by_stream[stream] + 1;
    } 

    // create the buffer message
    Message* message = new Message();
    message->type = Message::BUFFER_MESSAGE;
    message->buffer_message.iteration = iteration;
    message->buffer_message.buffer_length = buffer_length;
    message->buffer_message.data_length = length;
    message->buffer_message.ts = ts;
    message->buffer_message.sync = _sync;
    strcpy(message->buffer_message.stream, stream.c_str());
    strcpy(message->buffer_message.buffer_shm_path, buffer_shm_path.c_str());

    // index this message by buffer shm path
    {
        std::lock_guard<std::mutex> lock(_message_mutex);
        if (_producer_message_by_shm_path.count(buffer_shm_path) > 0) {
            delete _producer_message_by_shm_path[buffer_shm_path];
        }
        _producer_message_by_shm_path[buffer_shm_path] = message;
    }

    // send our message to each consumer
    for (const auto& consumer_mq : consumer_mqs) {
        if (_sync) {
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
                std::unique_lock<std::mutex> lock(_buffer_ack_mutex);
                _iterations_pending_by_pid[consumer_pid].insert(iteration);
            }
            
            if (!notify(consumer_mq, message)) {
                {
                    // send failed, so remove this message from the list of pending
                    std::unique_lock<std::mutex> lock(_buffer_ack_mutex);
                    _iterations_pending_by_pid[consumer_pid].erase(iteration);
                }

                std::cerr << DEBUG_PREFIX << "Failed to send buffer message to consumer" << std::endl;
                return false;
            }
        } else {
            notify(consumer_mq, message);
        } 
    }

    // if blocking, wait for acknowledgement prior to returning
    if (_sync) {
        // waiting for acknowledgement
        std::unique_lock<std::mutex> lock(_buffer_ack_mutex);
        _buffer_acks.wait(lock, [&] { 
           
            bool all_acknowledged = true;

            for (auto const& consumer_pid : consumer_pids) {
                if (_iterations_pending_by_pid.count(consumer_pid) == 0) {
                    // consumer exited, so consider it acknowledged
                    all_acknowledged &= true;
                } else { 
                    bool message_acknowledged = _iterations_pending_by_pid[consumer_pid].find(iteration) == _iterations_pending_by_pid[consumer_pid].end();
                    all_acknowledged &= message_acknowledged;
                }
            }

            return all_acknowledged;
        });
    } 

    return true;
}


Buffer* MomentumContext::next_buffer(std::string stream, size_t length) {
    if (_terminated) return NULL;


    normalize_stream(stream);
    if (!is_valid_stream(stream)) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Invalid stream: " << stream << std::endl;
        }
        return NULL;
    };

    bool has_stream;
    {
        std::lock_guard<std::mutex> lock(_producer_mutex);
        has_stream = _producer_mq_by_stream.count(stream) > 0;
    }

    if (!has_stream) {
        struct mq_attr attrs;
        memset(&attrs, 0, sizeof(mq_attr));
        attrs.mq_maxmsg = 10;
        attrs.mq_msgsize = sizeof(Message);

        std::string producer_mq_name = to_mq_name(0, stream);
        mq_unlink(producer_mq_name.c_str());
        mqd_t producer_mq = mq_open(producer_mq_name.c_str(), O_RDWR | O_CREAT | O_NONBLOCK, MQ_MODE, &attrs);
        if (producer_mq < 0) {
            if (_debug) {
                std::cerr << DEBUG_PREFIX << "Failed to open mq for stream: " << stream << std::endl;
            }
            return NULL;
        }

        {
            std::lock_guard<std::mutex> lock(_producer_mutex);
            _producer_mq_by_stream[stream] = producer_mq;        
            _producer_streams.insert(stream);
        }
    }

    Buffer* buffer = NULL;
    bool below_minimum_buffers;
    
    size_t buffer_count;
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        buffer_count = _buffers_by_stream[stream].size();
    }

    // find the next buffer to use
    for (size_t i = 0; i < buffer_count; i++) {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        
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

    

    // If we don't have enough buffers, then create them...
    below_minimum_buffers = buffer_count < _min_buffers;
    if (buffer == NULL || below_minimum_buffers) {
        size_t allocations_required = below_minimum_buffers ? _min_buffers.load() : 1;

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

    }

    size_t buffer_length;
    {
        std::lock_guard<std::mutex> lock(_buffer_mutex);
        buffer_length = buffer->length;
    }
    if (length > buffer_length) {
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

bool MomentumContext::get_sync() {
    return _sync;
}

void MomentumContext::set_sync(bool value) {
    _sync = value;
    if (_debug) {
        std::cout << DEBUG_PREFIX << "Option 'sync' set to value: " << value << std::endl;
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

    std::lock_guard<std::mutex> lock(_buffer_mutex);

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
        std::cout << DEBUG_PREFIX << "Resized shm file: " << buffer->shm_path << " " << buffer->length << std::endl;
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

bool MomentumContext::notify(mqd_t mq, Message* message, int priority) {
    while (mq_send(mq, (const char*) message, sizeof(Message), priority) < 0) {
        
        if (errno == EAGAIN) {
            { 
                // ensure that this process wasn't removed on us...
                std::lock_guard<std::mutex> lock(_consumer_mutex);
                if (_pid_by_consumer_mq.count(mq) == 0) {
                    // process was removed, so fail
                    return false;
                }
            }

            // recipient was busy, so try again after sleeping
            if (usleep(1) < 0) {
                // we were terminated while sleeping, so exit with false
                return false;
            } else {
                continue;
            }
        } else {
            // an unexpected error occurred
            return false;
        }
    }
    return true;
}

void MomentumContext::dir_iter(const std::string& base_path, std::function<void(std::string)> callback) {
    std::set<std::string> filenames;

    // clean up any unnecessary files created by the parent process
    struct dirent* entry;
    DIR* dir;
    dir = opendir(base_path.c_str());
    if (dir == NULL) {
        if (_debug) {
            std::cerr << DEBUG_PREFIX << "Failed to access directory: " << base_path << std::endl;
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
    oss << id;
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

void MomentumContext::remove_consumer(pid_t pid, std::string stream) {

    std::string consumer_mq_name = to_mq_name(pid, stream);

    mqd_t consumer_mq;
    {
        std::unique_lock<std::mutex> lock(_consumer_mutex);

        if (_mq_by_mq_name.count(consumer_mq_name) == 0) {
            return;
        }

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

        _streams_by_consumer_pid[pid].erase(stream);

        // if consumer mqs for this stream are all empty, remove the key from the data structure
        if (_consumer_mqs_by_stream[stream].size() == 0) {
            _consumer_mqs_by_stream.erase(stream);
        }
    }

    {
        std::unique_lock<std::mutex> lock(_buffer_ack_mutex);
        _iterations_pending_by_pid.erase(pid);
    }

    _buffer_acks.notify_all();

    if (_debug) {
        std::cout << DEBUG_PREFIX << "Removed subscriber from stream: " << stream << std::endl;
    }
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

bool momentum_get_sync(MomentumContext* ctx) {
    return ctx->get_sync();
}

void momentum_set_sync(MomentumContext* ctx, bool value) {
    ctx->set_sync(value);
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

bool momentum_is_subscribed(MomentumContext* ctx, const char* stream) {
    return ctx->is_subscribed(std::string(stream));
}

bool momentum_subscribe(MomentumContext* ctx, const char* stream) {
    return ctx->subscribe(std::string(stream));
}

bool momentum_unsubscribe(MomentumContext* ctx, const char* stream) {
    return ctx->unsubscribe(std::string(stream));
}

Buffer* momentum_next_buffer(MomentumContext* ctx, const char* stream, size_t length) {
    return ctx->next_buffer(std::string(stream), length);
}

bool momentum_send_buffer(MomentumContext* ctx, Buffer* buffer, size_t length, uint64_t ts) {
    return ctx->send_buffer(buffer, length, ts ? ts : 0);
}

BufferData* momentum_receive_buffer(MomentumContext* ctx, const char* stream) {
    return ctx->receive_buffer(std::string(stream));
}

bool momentum_release_buffer(MomentumContext* ctx, const char* stream, BufferData* buffer_data) {
    return ctx->release_buffer(std::string(stream), buffer_data);
}

uint8_t* momentum_get_buffer_address(Buffer* buffer) {
    return buffer->address;
}

size_t momentum_get_buffer_length(Buffer* buffer) {
    return buffer->length;
}