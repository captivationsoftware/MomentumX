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

        std::string control_snapshot(float timeout_seconds = 0.5) const {
            const auto control_block_handle = view_control_block();

            Utils::OmniWriteLock control_lock(control_block_handle->control_block->control_mutex, Utils::defer_lock);
            struct timespec ptimeout = Utils::timespec_now();
            const int64_t offset_nsec = static_cast<int64_t>(timeout_seconds * 1e9);
            ptimeout.tv_sec += offset_nsec / 1000000000;
            ptimeout.tv_nsec += offset_nsec % 1000000000;
            if (ptimeout.tv_nsec >= 1000000000) {
                ptimeout.tv_sec += 1;
                ptimeout.tv_nsec -= 1000000000;
            }
            if (!control_lock.timed_lock(ptimeout)) {
                return "error: unable to lock control block";
            }

            return control_block_handle->control_block->to_string();
        }

       private:
        Utils::PathConfig paths;
    };
}  // namespace MomentumX
#endif  // MOMENTUMX_INSPECTOR_H