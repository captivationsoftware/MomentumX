#ifndef MOMENTUM_H
#define MOMENTUM_H

#include <set>
#include <functional>
#include <string>
#include <vector>
#include <map>
#include <list>
#include <queue>
#include <mutex>
#include <atomic>
#include <sys/stat.h>
#include <sys/types.h>
#include <mqueue.h>
#include <condition_variable>
#include <cstdint>

typedef const void (*callback_t)(uint8_t*, size_t, uint64_t, long long int);

static const std::string PATH_DELIM = "_";
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

struct Message {
    enum{SUBSCRIBE_MESSAGE, UNSUBSCRIBE_MESSAGE, BUFFER_MESSAGE, ACK_MESSAGE, TERM_MESSAGE} type;
    union { 
        struct {
            char stream[MAX_STREAM_SIZE];
            pid_t pid;
        } subscription_message;
        struct {
            char stream[MAX_STREAM_SIZE];
        } term_message;
        struct {
            char stream[MAX_STREAM_SIZE];
            long long int id;
            char buffer_shm_path[MAX_PATH_SIZE];
            size_t buffer_length;
            size_t data_length;
            uint64_t ts;
            bool sync;
        } buffer_message;
        struct {
            long long int id;
            pid_t pid;
        } ack_message;
    };
};

class MomentumContext {

public:
    MomentumContext();
    ~MomentumContext();
    bool is_terminated() const;
    bool term();
    bool is_stream_available(std::string stream);
    bool is_subscribed(std::string stream);
    bool subscribe(std::string stream);
    bool unsubscribe(std::string stream, bool notify=true);
    Buffer* next_buffer(std::string stream, size_t length);
    bool receive_buffer(std::string stream, callback_t callback);
    bool send_buffer(Buffer* buffer, size_t length, uint64_t ts=0);

    // getter / setters for options    
    bool get_debug();
    void set_debug(bool value);
    size_t get_min_buffers();
    void set_min_buffers(size_t value);
    size_t get_max_buffers();
    void set_max_buffers(size_t value);
    bool get_sync();
    void set_sync(bool value);
    

private:

    // State variables
    pid_t _pid;

    std::atomic<bool> _debug{false};
    std::atomic<bool> _sync{false};
    std::atomic<bool>  _terminated{false};    
    std::atomic<bool>  _terminating{false};
    std::atomic<size_t> _min_buffers{1};
    std::atomic<size_t> _max_buffers{std::numeric_limits<size_t>::max()};

    std::set<std::string> _producer_streams;
    std::set<std::string> _consumer_streams;

    std::map<std::string, mqd_t> _producer_mq_by_stream;
    std::map<std::string, std::vector<mqd_t>> _consumer_mqs_by_stream;
    std::map<mqd_t, pid_t> _pid_by_consumer_mq;
    std::map<std::string, mqd_t> _mq_by_mq_name;

    std::map<std::string, std::list<Buffer*>> _buffers_by_stream;
    std::map<std::string, Buffer*> _buffer_by_shm_path;
    std::map<std::string, Buffer*> _last_acquired_buffer_by_stream;

    std::map<std::string, long long int> _message_id_by_stream;
    std::map<std::string, long long int> _last_message_id_by_stream;
    std::map<std::string, Message*> _producer_message_by_shm_path;
    std::map<std::string, std::list<Message>> _consumer_messages_by_stream;
    std::map<pid_t, std::set<long long int>> _message_ids_pending_by_pid;
    std::mutex _message_mutex, _producer_mutex, _consumer_mutex, _ack_mutex, _buffer_mutex;
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
    bool notify(mqd_t mq, Message* message, size_t length, int priority=1);
    bool force_notify(mqd_t mq, Message* message, size_t length, int priority=1);
    
};

extern "C" {

    MomentumContext* momentum_context();

    bool momentum_get_debug(MomentumContext* ctx);
    void momentum_set_debug(MomentumContext* ctx, bool value);
    
    size_t momentum_get_min_buffers(MomentumContext* ctx);
    void momentum_set_min_buffers(MomentumContext* ctx, size_t value);
    
    size_t momentum_get_max_buffers(MomentumContext* ctx);
    void momentum_set_max_buffers(MomentumContext* ctx, size_t value);
    
    bool momentum_get_sync(MomentumContext* ctx);
    void momentum_set_sync(MomentumContext* ctx, bool value);
    
    bool momentum_term(MomentumContext* ctx);
    bool momentum_is_terminated(MomentumContext* ctx);
    bool momentum_destroy(MomentumContext* ctx);

    bool momentum_is_stream_available(MomentumContext* ctx, const char* stream);

    bool momentum_is_subscribed(MomentumContext* ctx, const char* stream);
    bool momentum_subscribe(MomentumContext* ctx, const char* stream);
    bool momentum_unsubscribe(MomentumContext* ctx, const char* stream);
    
    Buffer* momentum_next_buffer(MomentumContext* ctx, const char* stream, size_t length);
    bool momentum_send_buffer(MomentumContext* ctx, Buffer* buffer, size_t data_length, uint64_t ts);
    bool momentum_receive_buffer(MomentumContext* ctx, const char* stream, callback_t callback);

    uint8_t* momentum_get_buffer_address(Buffer* buffer);
    size_t momentum_get_buffer_length(Buffer* buffer);
}


#endif