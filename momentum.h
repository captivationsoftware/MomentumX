#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <functional>
#include <string>
#include <vector>
#include <map>
#include <queue>
#include <mutex>
#include <atomic>
#include <sys/stat.h>
#include <sys/types.h>
#include <mqueue.h>
#include <condition_variable>
#include <cstdint>

typedef const void (*callback_t)(uint8_t* , size_t, size_t, uint64_t);

static const std::string PATH_DELIM = "_";
static const std::string MESSAGE_DELIM = " ";
static const std::string NAMESPACE = "momentum";
static const std::string DEBUG_PREFIX = "[" + NAMESPACE + "]: ";
static const std::string PROTOCOL = NAMESPACE + "://";
static const std::string SHM_PATH_BASE = "/dev/shm";
static const std::string DEBUG_ENV = "DEBUG";
static const size_t PAGE_SIZE = getpagesize();
static const size_t MAX_STREAM_SIZE = 128; // 32 chars for stream name, and 11 for protocol (i.e. "momentum://)
static const size_t MAX_PATH_SIZE = 256; // 255 maximum linux file name + 1 for the leading slash
static const uint64_t NANOS_PER_SECOND = 1000000000;
static const mode_t MQ_MODE = 0644;

static const size_t MAX_MESSAGE_SIZE = 1024;

static const char MESSAGE_TYPE_SUBSCRIBE = 1;
static const char MESSAGE_TYPE_UNSUBSCRIBE = 2;
static const char MESSAGE_TYPE_BUFFER = 3;
static const char MESSAGE_TYPE_ACK = 4;
static const char MESSAGE_TYPE_TERM = 5;

static const struct timespec MESSAGE_TIMEOUT {
    0,
    1000
};

struct Buffer {
    uint8_t id;
    char shm_path[MAX_PATH_SIZE];
    int fd;
    size_t length;
    uint8_t* address;
};

class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool is_terminated() const;
    bool term();
    bool is_stream_available(std::string stream);
    bool is_subscribed(std::string stream, callback_t callback);
    bool subscribe(std::string stream, callback_t callback);
    bool unsubscribe(std::string stream, callback_t callback, bool notify=true);
    bool send_string(std::string stream, const char* data, size_t length, uint64_t ts=0);
    Buffer* acquire_buffer(std::string stream, size_t length);
    bool release_buffer(Buffer* buffer, size_t length, uint64_t ts=0);
    
    bool get_debug();
    void set_debug(bool value);
    size_t get_min_buffers();
    void set_min_buffers(size_t value);
    size_t get_max_buffers();
    void set_max_buffers(size_t value);
    bool get_blocking();
    void set_blocking(bool value);
    

private:

    // State variables
    pid_t _pid;

    std::atomic<bool> _debug{false};
    std::atomic<bool> _blocking{false};
    std::atomic<bool>  _terminated{false};    
    std::atomic<bool>  _terminating{false};
    std::atomic<size_t> _min_buffers{1};
    std::atomic<size_t> _max_buffers{std::numeric_limits<size_t>::max()};
    std::atomic<long long int> _last_message_id{-1};
    std::atomic<long long int> _message_id{0};

    std::set<std::string> _producer_streams;
    std::set<std::string> _consumer_streams;

    std::map<std::string, mqd_t> _producer_mq_by_stream;
    std::map<std::string, std::vector<mqd_t>> _consumer_mqs_by_stream;
    std::map<mqd_t, pid_t> _pid_by_consumer_mq;
    std::map<std::string, mqd_t> _mq_by_mq_name;

    std::map<std::string, std::vector<callback_t>> _callbacks_by_stream;

    std::map<std::string, Buffer*> _first_buffer_by_stream;
    std::map<std::string, std::queue<Buffer*>> _buffers_by_stream;
    std::map<std::string, Buffer*> _buffer_by_shm_path;
    std::map<std::string, Buffer*> _last_acquired_buffer_by_stream;

    std::map<pid_t, std::set<long long int>> _message_ids_pending_by_pid;
    std::mutex _producer_mutex, _consumer_mutex, _ack_mutex, _buffer_mutex;
    std::condition_variable _consumer_availability, _acks;

    std::thread _message_handler;

    // Buffer / SHM functions
    Buffer* allocate_buffer(const std::string& shm_path, size_t length, int flags);
    void resize_buffer(Buffer* buffer, size_t length, bool truncate=false);
    void deallocate_buffer(Buffer* buffer);
    std::string to_shm_path(pid_t pid, const std::string& stream, uint64_t id) const;
    void shm_iter(const std::function<void(std::string)> callback);
    void get_shm_time(const std::string& shm_path, uint64_t* read_ts, uint64_t* write_ts);
    void set_shm_time(const std::string& shm_path, uint64_t read_ts, uint64_t write_ts);
    std::string stream_from_shm_path(std::string shm_path) const;

    // Message / MQ functions
    std::string to_mq_name(pid_t pid, const std::string& stream);

    // Stream functions    
    bool is_valid_stream(const std::string& stream) const;
    void normalize_stream(std::string& stream);

    // Utility functions
    uint64_t now() const;    
    bool send(mqd_t mq, const std::string& message, int priority=1);
    bool force_send(mqd_t mq, const std::string& message, int priority=1);
    
};

extern "C" {

    MomentumContext* momentum_context();

    bool momentum_get_debug(MomentumContext* ctx);
    void momentum_set_debug(MomentumContext* ctx, bool value);
    
    size_t momentum_get_min_buffers(MomentumContext* ctx);
    void momentum_set_min_buffers(MomentumContext* ctx, size_t value);
    
    size_t momentum_get_max_buffers(MomentumContext* ctx);
    void momentum_set_max_buffers(MomentumContext* ctx, size_t value);
    
    bool momentum_get_blocking(MomentumContext* ctx);
    void momentum_set_blocking(MomentumContext* ctx, bool value);
    
    bool momentum_term(MomentumContext* ctx);
    bool momentum_is_terminated(MomentumContext* ctx);
    bool momentum_destroy(MomentumContext* ctx);

    bool momentum_is_stream_available(MomentumContext* ctx, const char* stream);

    bool momentum_is_subscribed(MomentumContext* ctx, const char* stream, callback_t callback);
    bool momentum_subscribe(MomentumContext* ctx, const char* stream, callback_t callback);
    bool momentum_unsubscribe(MomentumContext* ctx, const char* stream, callback_t callback);
    
    bool momentum_send_string(MomentumContext* ctx, const char* stream, const char* data, size_t length, uint64_t ts);
    
    Buffer* momentum_acquire_buffer(MomentumContext* ctx, const char* stream, size_t length);
    bool momentum_release_buffer(MomentumContext* ctx, Buffer* buffer, size_t data_length, uint64_t ts);
    uint8_t* momentum_get_buffer_address(Buffer* buffer);
    size_t momentum_get_buffer_length(Buffer* buffer);

}

#endif