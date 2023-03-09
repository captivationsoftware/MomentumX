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
#include <nlohmann/json_fwd.hpp>
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

        enum class State : uint8_t { INITIAL, WRITE_CLAIMED, READ_CLAIMED, SENT, SKIPPED, ACKNOWLEDGED };

       public:
        enum class CheckoutResult : uint8_t { SUCCESS, TIMEOUT, SKIPPED };

        BufferSync() = default;
        ~BufferSync() = default;

        BufferSync(BufferSync&&) = delete;
        BufferSync(const BufferSync&) = delete;
        BufferSync operator=(BufferSync&&) = delete;
        BufferSync operator=(const BufferSync&) = delete;

        bool can_checkout_read(Utils::OmniWriteLock& control_lock, std::chrono::microseconds timeout = std::chrono::milliseconds(200)) {
            assert_owns(control_lock);

            State determined_state = get_state();  // store outside of lambda
            switch (determined_state) {
                case State::READ_CLAIMED:  // fall-through - at least one othe reader
                case State::SENT:          // fall-through - no readers yet
                case State::ACKNOWLEDGED:  // fall-through - always acknowledged for streaming case
                    return true;
                default:
                    return false;
            }
        }

        CheckoutResult checkout_read(Utils::OmniWriteLock& control_lock, std::chrono::microseconds timeout = std::chrono::milliseconds(200)) {
            assert_owns(control_lock);

            State determined_state = State::INITIAL;
            const bool has_result = _checkin_cond.timed_wait(control_lock, ptimeout(timeout), [&] {
                determined_state = get_state();  // store outside of lambda
                switch (determined_state) {
                    case State::READ_CLAIMED:  // fall-through - at least one other reader
                    case State::SENT:          // fall-through - no readers yet
                    case State::SKIPPED:       // fall-through - writer skipped buffer entirely
                    case State::ACKNOWLEDGED:  // fall-through - always acknowledged for streaming case
                        return true;
                    default:
                        return false;
                }
            });

            // Not yet ready to ready
            if (!has_result) {
                return CheckoutResult::TIMEOUT;
            }

            // Will never be ready to read
            if (determined_state == State::SKIPPED) {
                return CheckoutResult::SKIPPED;
            }

            // Ready to read
            ++_active_readers;
            return CheckoutResult::SUCCESS;
        }

        bool checkout_write(Utils::OmniWriteLock& control_lock, std::chrono::microseconds timeout = std::chrono::milliseconds(200)) {
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
                return true;
            }

            return false;
        }

        void mark_sent(Utils::OmniWriteLock& control_lock, size_t required_acknowledgements) {
            assert_owns(control_lock);
            _is_sent = true;
            _required_readers = required_acknowledgements;
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

        // This is a leaky abstraction that should be updated, perhaps by moving the iteration into the sync, since it's being
        // used as an input for checkout control flow (O_o)
        inline bool is_skipped(Utils::OmniWriteLock& control_lock) const {
            assert_owns(control_lock);
            return get_state() == State::SKIPPED;
        };

        void closeout(Utils::OmniWriteLock& control_lock, const std::chrono::microseconds& timeout = std::chrono::seconds(1)) {
            assert_owns(control_lock);
            const bool can_checkout = _checkin_cond.timed_wait(control_lock, ptimeout(timeout), [&] {
                const bool ok_writers = (_active_writers == 0);
                const bool ok_readers = (_active_readers == 0);
                const bool ok_acks = (_done_readers >= _required_readers);

                return ok_writers && ok_readers && ok_acks;
            });
        }

        std::string dumps(int64_t indent = 2) const;
        friend void to_json(nlohmann::json& j, const BufferSync& bs);
        friend std::ostream& operator<<(std::ostream& os, const BufferSync& x);
        inline friend std::ostream& operator<<(std::ostream& os, const BufferSync& x) {
            os << "BufferSync: ";
            os << "{ is_sent: " << std::boolalpha << x._is_sent;
            os << ", state: " << to_str(x.get_state());
            os << ", active_readers: " << x._active_readers;
            os << ", active_writers: " << x._active_writers;
            os << ", done_readers: " << x._done_readers;
            os << ", done_writers: " << x._done_writers;
            os << ", required_readers: " << x._required_readers;
            os << "}";
            return os;
        }

       private:
        inline State get_state() const {
            if (_active_readers != 0 && _active_writers != 0) {
                throw std::logic_error("Cannot have active readers and active writers in BufferSync");
            }

            if (_active_writers != 0) {
                return State::WRITE_CLAIMED;  // write buffer checked out
            }

            if (_active_readers != 0) {
                return State::READ_CLAIMED;  // read buffer(s) checked out
            }

            if (_done_writers > 0 && !_is_sent) {
                return State::SKIPPED;  // write buffer checked out and released, but never sent
            }

            if (_done_readers >= _required_readers) {
                return State::ACKNOWLEDGED;  // all acknowledgements received (sync) or not needed (stream)
            }

            if (_done_writers > 0 && _is_sent) {
                return State::SENT;  // buffer has data to be read (by one or more readers)
            }

            return State::INITIAL;  // initial state
        }

        inline static const char* to_str(State s) {
            switch (s) {
                case State::INITIAL:
                    return "INITIAL";
                case State::WRITE_CLAIMED:
                    return "WRITE_CLAIMED";
                case State::READ_CLAIMED:
                    return "READ_CLAIMED";
                case State::SENT:
                    return "SENT";
                case State::SKIPPED:
                    return "SKIPPED";
                case State::ACKNOWLEDGED:
                    return "ACKNOWLEDGED";
            }
            throw std::logic_error("Invalid enumeration for BufferSync::State");
        }

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
        // State _state{State::WRITE_START};      ///< FSM state for tracking write-read transitions
        bool _is_sent{};             ///< Marks a buffer as ready to be read or written
        size_t _active_readers{};    ///< Number of readers to have active claim of buffer, zero or more
        size_t _active_writers{};    ///< Number of writers to have active claim of buffer, zero or One
        size_t _done_readers{};      ///< Number of readers to have read the buffer, zero or more
        size_t _done_writers{};      ///< Number of writers to have written to the buffer, zero or One
        size_t _required_readers{};  ///< Number of readers required to complete buffer, nonzero in sync mode
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

        std::string dumps(int64_t indent = 2) const;
        friend void to_json(nlohmann::json& j, const LockableBufferState& lbs);
        friend std::ostream& operator<<(std::ostream& os, const LockableBufferState& lbs);
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

        inline const uint64_t& last_sent_iteration() const { return buffers.at(last_sent_index).buffer_state.iteration; }
        inline uint64_t& last_sent_iteration() { return buffers.at(last_sent_index).buffer_state.iteration; }

        std::string dumps(int64_t indent = 2) const;
        friend void to_json(nlohmann::json& j, const ControlBlock& cb);
        inline friend std::ostream& operator<<(std::ostream& os, const ControlBlock& cb) {
            os << "ControlBlock: ";
            os << "{ sync: " << std::boolalpha << cb.sync;
            os << ", is_ended: " << std::boolalpha << cb.is_ended;
            os << ", buffer_size: " << cb.buffer_size;
            os << ", buffer_count: " << cb.buffer_count;
            os << ", subscribers: " << cb.subscriber_count;
            os << ", last_sent_index: " << cb.last_sent_index;
            os << ", last_sent_iteration: " << cb.last_sent_iteration();
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