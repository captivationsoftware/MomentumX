#ifndef MOMENTUMX_CONTROL_H
#define MOMENTUMX_CONTROL_H

#include <ostream>
#include <sstream>
#include <string>
#include "buffer.h"
#include "utils.h"

namespace MomentumX {
    struct PendingAcknowledgement {
        size_t buffer_id = 0;
        Context* context = nullptr;

        PendingAcknowledgement() = default;

        PendingAcknowledgement(size_t _buffer_id, Context* _context) : buffer_id(_buffer_id), context(_context) {}

        inline friend std::ostream& operator<<(std::ostream& os, const PendingAcknowledgement& pa) {
            os << "PendingAcknowledgement: { buffer_id: " << pa.buffer_id;
            os << ", context: " << pa.context;
            os << "}";
            return os;
        }

        inline std::string to_string() const {
            std::stringstream ss;
            ss << *this;
            return ss.str();
        }
    };

    struct ControlBlock {
        static constexpr size_t MAX_BUFFERS = 512;
        static constexpr size_t MAX_SUBSCRIPTIONS = 256;

        bool sync{};
        bool is_ended{};
        size_t buffer_size{};
        size_t buffer_count{};
        Utils::StaticVector<BufferState, MAX_BUFFERS> buffers{};
        Utils::StaticVector<Context*, MAX_SUBSCRIPTIONS> subscribers{};
        Utils::StaticVector<PendingAcknowledgement, MAX_SUBSCRIPTIONS * MAX_BUFFERS> pending_acknowledgements{};

        inline friend std::ostream& operator<<(std::ostream& os, const ControlBlock& cb) {
            os << "ControlBlock: { sync: " << std::boolalpha << cb.sync;
            os << ", is_ended: " << std::boolalpha << cb.is_ended;
            os << ", buffer_size: " << cb.buffer_size;
            os << ", buffer_count: " << cb.buffer_count;
            os << ", buffers: " << cb.buffers;
            os << ", subscribers: " << cb.subscribers;
            os << ", pending_acknowledgements: " << cb.pending_acknowledgements;
            os << "}";
            return os;
        }

        inline std::string to_string() const {
            std::stringstream ss;
            ss << *this;
            return ss.str();
        }
    };

};      // namespace MomentumX
#endif  // MOMENTUMX_CONTROL_H