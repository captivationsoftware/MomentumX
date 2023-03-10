#ifndef MOMENTUMX_STREAM_H
#define MOMENTUMX_STREAM_H

#include <errno.h>
#include <sys/file.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <condition_variable>
#include <cstring>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <tuple>
#include <type_traits>
#include <vector>

#include "buffer.h"
#include "utils.h"

namespace MomentumX {

    struct PendingAcknowledgement;
    struct ControlBlock;

    class Stream {
       public:
        using BufferState = ::MomentumX::BufferState;
        using Mutex = Utils::OmniMutex;
        using Lock = Utils::OmniWriteLock;

        enum Role { CONSUMER, PRODUCER };

        Stream(const Utils::PathConfig& paths, size_t buffer_size = 0, size_t buffer_count = 0, bool sync = false, Role role = Role::CONSUMER);

        ~Stream();

        const std::string& name(const Utils::OmniWriteLock& control_lock);

        Utils::PathConfig paths(const Utils::OmniWriteLock& control_lock);

        int fd(const Utils::OmniWriteLock& control_lock);

        bool sync(const Utils::OmniWriteLock& control_lock);

        size_t buffer_size(const Utils::OmniWriteLock& control_lock);

        size_t buffer_count(const Utils::OmniWriteLock& control_lock);

        std::list<BufferState> buffer_states(const Utils::OmniWriteLock& control_lock, bool sort = false, uint64_t minimum_timestamp = 0);

        void update_buffer_state(const Utils::OmniWriteLock& control_lock, const Stream::BufferState& buffer_state);

        size_t subscriber_count(const Utils::OmniWriteLock& control_lock);

        bool is_ended(const Utils::OmniWriteLock& control_lock);

        void end(const Utils::OmniWriteLock& control_lock);

        void unsubscribe();

        Lock get_control_lock();

       private:
        friend class StreamManager;

        Utils::PathConfig _paths;
        Role _role{};
        int _fd{};
        char* _data{};
        size_t _last_index{};
        size_t _last_iteration{};
        ControlBlock* _control{};
        bool _sync{};
        bool _subscribed{};
    };

    class StreamManager {
       public:
        struct Mutex : std::mutex {};
        using Lock = std::lock_guard<Mutex>;

        StreamManager(Context* context, BufferManager* buffer_manager);
        ~StreamManager();

        Stream* find_or_create(const Lock& lock, std::string name);
        Stream* create(const Lock& lock,
                       const BufferManager::Lock& buffer_manager_lock,
                       std::string name,
                       size_t buffer_size,
                       size_t buffer_count = 0,
                       bool sync = false,
                       Stream::Role role = Stream::Role::CONSUMER);

        void destroy(const Lock& lock, Stream* stream);
        Stream* subscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, std::string name);
        void unsubscribe(const Lock& lock, const BufferManager::Lock& buffer_manager_lock, Stream* stream);

        std::shared_ptr<Stream::BufferState> next_buffer_state(const Lock& lock,
                                                               const BufferManager::Lock& buffer_manager_lock,
                                                               Stream::Lock& control_lock,
                                                               Stream* stream);
        bool send_buffer_state(const Lock& lock,
                               const BufferManager::Lock& buffer_manager_lock,
                               Stream::Lock& control_lock,
                               Stream* stream,
                               Stream::BufferState buffer_state);
        std::shared_ptr<Stream::BufferState> receive_buffer_state(const Lock& lock,
                                                                  const BufferManager::Lock& buffer_manager_lock,
                                                                  Stream::Lock& control_lock,
                                                                  Stream* stream,
                                                                  uint64_t minimum_timestamp = 1);
        bool has_next_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, uint64_t minimum_timestamp = 1);
        void release_buffer_state(const Lock& lock, const Stream::Lock& control_lock, Stream* stream, const Stream::BufferState& buffer_state);
        size_t subscriber_count(const Lock& lock, const Stream::Lock& control_lock, Stream* stream);

        // synchronization stuff
        inline BufferManager::Lock get_buffer_manager_lock() { return _buffer_manager->get_buffer_manager_lock(); }
        inline Stream::Lock get_control_lock(Stream& stream) { return stream.get_control_lock(); }
        inline Lock get_stream_manager_lock() { return Lock(_stream_manager_mutex); }

       private:
        Context* _context;
        BufferManager* _buffer_manager;
        std::map<std::string, Stream*> _stream_by_name;
        std::map<Stream*, std::list<std::shared_ptr<Buffer>>> _buffers_by_stream;
        Mutex _stream_manager_mutex;
    };
};  // namespace MomentumX

#endif