#ifndef MOMENTUMX_CONTROL_H
#define MOMENTUMX_CONTROL_H

#include <boost/container/static_vector.hpp>
#include <boost/date_time/posix_time/posix_time_duration.hpp>
#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/interprocess/interprocess_fwd.hpp>
#include <boost/interprocess/sync/interprocess_condition.hpp>
#include <boost/interprocess/sync/interprocess_sharable_mutex.hpp>
#include <chrono>
#include <ios>
#include <optional>
#include <ostream>
#include <sstream>
#include <string>
#include "buffer.h"
#include "utils.h"

namespace MomentumX {

    class BufferSync {
        /// This class is used for buffer read/write checkout/checkin synchronization.
        ///
        /// This originally was planned to be handled by a series of mutexes. However,
        /// interprocess mutexes (like usual mutexes) need to be unlocked by the same thread
        /// by which they are originally locked. Since buffer locking is intended to
        /// manage the lifetime of a buffer, we instead will take an approach of manual
        /// bookkeeping, protected by an interprocess mutex.
        ///
        /// For all synchronization, we will rely on the interprocess mutex of the
        /// ControlBlock to ensure all data manipulation is synchronized. Despite not
        /// having a unique mutex, each BufferSync does internally maintain separate
        /// condition variables, to prevent waking unintended threads.

        enum class State : uint8_t { WRITE_START, WRITE_DONE, READ_START, READ_DONE };

       public:
        BufferSync() = default;
        ~BufferSync() = default;

        BufferSync(BufferSync&&) = delete;
        BufferSync(const BufferSync&) = delete;
        BufferSync operator=(BufferSync&&) = delete;
        BufferSync operator=(const BufferSync&) = delete;

        bool checkout_read(Utils::OmniWriteLock& control_lock, std::chrono::microseconds timeout = std::chrono::milliseconds(10)) {
            assert_owns(control_lock);

            const bool can_checkout = _checkin_cond.timed_wait(control_lock, ptimeout(timeout), [&] { return _is_sent && _active_writers == 0; });

            if (can_checkout) {
                ++_active_readers;
                return true;
            }

            return false;
        }

        bool checkout_write(Utils::OmniWriteLock& control_lock,
                            size_t required_acknowledgements,
                            std::chrono::microseconds timeout = std::chrono::milliseconds(10)) {
            assert_owns(control_lock);

            const bool can_checkout = _checkin_cond.timed_wait(control_lock, ptimeout(timeout), [&] {
                const bool ok_writers = (_active_writers == 0);
                const bool ok_readers = (_active_readers == 0);
                const bool ok_acks = (_done_readers >= _required_readers);

                return ok_writers && ok_readers && ok_acks;
            });

            if (can_checkout) {
                _is_sent = false;
                ++_active_writers;
                _done_readers = 0;
                _done_writers = 0;
                _required_readers = required_acknowledgements;
                return true;
            }

            return false;
        }

        void mark_sent(Utils::OmniWriteLock& control_lock) {
            assert_owns(control_lock);
            _is_sent = true;
        }

        void checkin_read(Utils::OmniWriteLock& control_lock) {
            assert_owns(control_lock);

            if (_active_readers == 0) {
                throw std::logic_error("Attempting to check in read buffer where there are none checked out");
            }
            if (_active_writers != 0) {
                throw std::logic_error("Attempting to check in read buffer when writer controls buffer");
            }

            --_active_readers;
            ++_done_readers;

            _checkin_cond.notify_one();  // notify writer
        }

        void checkin_write(Utils::OmniWriteLock& control_lock) {
            assert_owns(control_lock);

            if (_active_writers != 1) {
                throw std::logic_error("Attempting to check in write buffer where there are none checked out");
            }
            if (_active_readers != 0) {
                throw std::logic_error("Attempting to check in write buffer when readers control buffer");
            }
            --_active_writers;
            ++_done_writers;

            _checkin_cond.notify_all();  // notify all readers
        }

        void inc_required(Utils::OmniWriteLock& control_lock) {
            assert_owns(control_lock);

            // Don't decrement if already zero (such as streaming case)
            if (_required_readers != 0) {
                ++_required_readers;
            }
        }

        void dec_required(Utils::OmniWriteLock& control_lock) {
            assert_owns(control_lock);

            // Don't decrement if already zero (such as streaming case)
            if (_required_readers != 0) {
                --_required_readers;
                _checkin_cond.notify_all();  // notify awaiting writers
            }
        }

        void closeout(Utils::OmniWriteLock& control_lock, const std::chrono::microseconds& timeout = std::chrono::seconds(1)) {
            assert_owns(control_lock);
            const bool can_checkout = _checkin_cond.timed_wait(control_lock, ptimeout(timeout), [&] {
                const bool ok_writers = (_active_writers == 0);
                const bool ok_readers = (_active_readers == 0);
                const bool ok_acks = (_done_readers >= _required_readers);

                return ok_writers && ok_readers && ok_acks;
            });
        }

        inline friend std::ostream& operator<<(std::ostream& os, const BufferSync& x) {
            os << "BufferSync: ";
            os << "{ is_sent: " << std::boolalpha << x._is_sent;
            os << ", active_readers: " << x._active_readers;
            os << ", active_writers: " << x._active_writers;
            os << ", done_readers: " << x._done_readers;
            os << ", done_writers: " << x._done_writers;
            os << ", required_readers: " << x._required_readers;
            os << "}";
            return os;
        }

       private:
        inline static void assert_owns(Utils::OmniWriteLock& control_lock) {
            if (!control_lock.owns()) {
                throw std::logic_error("Lock must already own ControlBlock mutext");
            }
        }

        inline static boost::posix_time::ptime ptimeout(const std::chrono::microseconds& timeout) {
            const auto now = boost::posix_time::microsec_clock::universal_time();
            const auto offs = boost::posix_time::microseconds(timeout.count());
            return now + offs;
        }

        Utils::OmniCondition _checkin_cond{};  ///< Conditional for awaiting buffer availability (in non-polling case)
        State _state{State::WRITE_START};      ///< FSM state for tracking write-read transitions
        size_t _is_sent{};                     ///< Marks a buffer as ready to be read or written
        size_t _active_readers{};              ///< Number of readers to have active claim of buffer, zero or more
        size_t _active_writers{};              ///< Number of writers to have active claim of buffer, zero or One
        size_t _done_readers{};                ///< Number of readers to have read the buffer, zero or more
        size_t _done_writers{};                ///< Number of writers to have written to the buffer, zero or One
        size_t _required_readers{};            ///< Number of readers required to complete buffer, nonzero in sync mode
    };

    struct LockableBufferState {
        explicit LockableBufferState(const BufferState& state) : buffer_state(state) {}
        ~LockableBufferState() = default;
        LockableBufferState(LockableBufferState&&) = delete;
        LockableBufferState(const LockableBufferState&) = delete;
        LockableBufferState& operator=(LockableBufferState&&) = delete;
        LockableBufferState& operator=(const LockableBufferState&) = delete;

        BufferState buffer_state{};
        BufferSync buffer_sync{};

        inline friend std::ostream& operator<<(std::ostream& os, const LockableBufferState& x) {
            os << "LockableBufferState: ";
            os << "{ buffer_state: " << x.buffer_state;
            os << ", buffer_sync: " << x.buffer_sync;
            os << "}";
            return os;
        }
    };

    struct ControlBlock {
        static constexpr size_t MAX_BUFFERS = 512;

        bool sync{};
        bool is_ended{};
        size_t buffer_size{};
        size_t buffer_count{};
        size_t subscriber_count{};
        size_t last_sent_index{};  // DELME (?)
        mutable Utils::OmniMutex control_mutex{};
        Utils::StaticVector<LockableBufferState, MAX_BUFFERS> buffers{};

        inline friend std::ostream& operator<<(std::ostream& os, const ControlBlock& cb) {
            os << "ControlBlock: ";
            os << "{ sync: " << std::boolalpha << cb.sync;
            os << ", is_ended: " << std::boolalpha << cb.is_ended;
            os << ", buffer_size: " << cb.buffer_size;
            os << ", buffer_count: " << cb.buffer_count;
            os << ", subscribers: " << cb.subscriber_count;
            os << ", last_sent_index: " << cb.last_sent_index;
            os << ", buffers: " << cb.buffers;
            os << "}";
            return os;
        }

        inline static size_t wrapping_increment(size_t idx, size_t wrap) { return (idx + 1) % wrap; }
        inline static size_t wrapping_decrement(size_t idx, size_t wrap) { return (idx + wrap - 1) % wrap; }

        inline std::string to_string() const {
            std::stringstream ss;
            ss << *this;
            return ss.str();
        }
    };

};      // namespace MomentumX
#endif  // MOMENTUMX_CONTROL_H