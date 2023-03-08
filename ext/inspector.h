#ifndef MOMENTUMX_INSPECTOR_H
#define MOMENTUMX_INSPECTOR_H

#include <iomanip>
#include <ios>
#include <limits>
#include <sstream>
#include "control.h"
#include "utils.h"
namespace MomentumX {

    struct ControlBlockHandle {
        int fd;
        ControlBlock* control_block;

        ControlBlockHandle(const Utils::PathConfig& paths) {
            fd = open(paths.stream_path.c_str(), O_RDWR, S_IRWXU);

            control_block = reinterpret_cast<ControlBlock*>(mmap(NULL, sizeof(ControlBlock), PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0));
            if (control_block == MAP_FAILED) {
                throw std::runtime_error("Failed to mmap shared memory stream file [errno: " + std::to_string(errno) + "]");
            }
        }

        ~ControlBlockHandle() { close(fd); }
        ControlBlockHandle(ControlBlockHandle&&) = delete;
        ControlBlockHandle(const ControlBlockHandle&) = delete;
        ControlBlockHandle& operator=(ControlBlockHandle&&) = delete;
        ControlBlockHandle& operator=(const ControlBlockHandle&) = delete;
    };

    class Inspector {
       public:
        Inspector(const std::string& stream_name, const std::string& context_path = "/dev/shm") : paths(context_path, stream_name) {}

        std::shared_ptr<ControlBlockHandle> view_control_block() const { return std::make_shared<ControlBlockHandle>(paths); }

        std::string control_snapshot(bool require_read_lock = true) const {
            const auto control_block_handle = view_control_block();
            Utils::OmniWriteLock control_lock(control_block_handle->control_block->control_mutex);
            const std::string s = control_block_handle->control_block->to_string();

            return s;
        }

        void check_locks() const {
            const int name_width = paths.buffer_mutex_name(std::numeric_limits<uint16_t>::max()).size();

            auto check_single = [&](const std::string& id, Utils::OmniMutex& m) {
                // const bool can_rlock = [&] { return Utils::OmniReadLock(m, boost::interprocess::defer_lock).try_lock(); }();
                // const bool can_wlock = [&] { return Utils::OmniWriteLock(m, boost::interprocess::defer_lock).try_lock(); }();
                // Utils::OmniWriteLock write_lock(m, boost::interprocess::defer_lock);

                // std::stringstream ss;
                // ss << "id:" << std::setw(name_width) << id;
                // ss << ", r:" << std::setw(5) << std::boolalpha << can_rlock;
                // ss << ", w:" << std::setw(5) << std::boolalpha << can_wlock;
                // std::cout << ss.str() << std::endl;
            };
            std::cout << "check_locks temporarily disabled" << std::endl;

            // const auto control = control_snapshot();
            // check_single(paths.stream_mutex, control->control_mutex);

            // const auto beg = control.buffers.begin();
            // const auto end = control.buffers.end();
            // for (auto it = beg; it != end; ++it) {
            //     const int16_t buffer_id = it->buffer_id;
            //     const auto mutex_name = paths.buffer_mutex_name(it->buffer_id);
            //     Utils::OmniMutex mutex(boost::interprocess::open_only, mutex_name.c_str());
            //     check_single(mutex_name, mutex);
            // }
        }

       private:
        Utils::PathConfig paths;
    };
}  // namespace MomentumX
#endif  // MOMENTUMX_INSPECTOR_H