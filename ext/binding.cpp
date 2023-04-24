#include "control.h"
#define PYBIND11_DETAILED_ERROR_MESSAGES

#include <pybind11/attr.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <stack>
#include <thread>

#include "buffer.h"
#include "context.h"
#include "inspector.h"
#include "stream.h"

namespace py = pybind11;
using namespace py::literals;  // provides `_a` literal for parameter names
using namespace std::string_literals;

using BufferState = MomentumX::Stream::BufferState;
using Logger = MomentumX::Utils::Logger;
using LogLevel = Logger::Level;
using MomentumX::Context;
using MomentumX::Stream;

struct BufferShim;
struct BufferShim;
struct ReadBufferShim;
struct StreamShim;
struct WriteBufferShim;

struct StreamExistsException : public std::exception {
    const char* what() const noexcept override { return "Stream already exists"; }
};

struct StreamUnavailableException : public std::exception {
    const char* what() const noexcept override { return "Failed to create stream subscription"; }
};

struct DataOverflowException : public std::exception {
    const char* what() const noexcept override { return "Data size exceeds allocated buffer size"; }
};

struct AlreadySentException : public std::exception {
    const char* what() const noexcept override { return "Buffer has already been sent and can no longer be modified and/or sent again"; }
};

struct ReleasedBufferException : public std::exception {
    const char* what() const noexcept override { return "Buffer has been released and can not longer be accessed"; }
};

struct ThreadingEventWrapper {
    py::object evt;
    ThreadingEventWrapper(py::object evt) : evt(evt) {}

    bool is_none() {
        py::gil_scoped_acquire lock;  // acquire before calling into python code
        return evt.is_none();
    }

    bool is_set() {
        if (is_none()) {
            return false;
        } else {
            py::gil_scoped_acquire lock;  // acquire before calling into python code
            return evt.attr("is_set")().cast<bool>();
        }
    }

    bool wait(double timeout) {
        if (is_none()) {
            std::this_thread::sleep_for(std::chrono::nanoseconds(int(timeout * 1e9)));
            return false;
        } else {
            py::gil_scoped_acquire lock;  // acquire before calling into python code
            return evt.attr("wait")(timeout).cast<bool>();
        }
    }
};

struct BufferShim {
    std::shared_ptr<Context> ctx{nullptr};
    std::shared_ptr<Stream> stream{nullptr};
    std::shared_ptr<BufferState> _unchecked_buffer_state{nullptr};
    size_t cursor_index, max_cursor_index;
    bool is_sent;

    BufferShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const std::shared_ptr<BufferState>& buffer_state)
        : ctx(ctx), stream(stream), _unchecked_buffer_state(buffer_state), cursor_index(0), max_cursor_index(0), is_sent(false) {}
    ~BufferShim() = default;

    inline std::shared_ptr<BufferState> get_state() const {
        const std::shared_ptr<BufferState> copy = _unchecked_buffer_state;  // copy to maintain ownership during subsequent call
        if (!copy) {
            throw ReleasedBufferException();
        }
        return copy;
    }

    const uint8_t* data() const {
        try {
            const auto buffer_state = get_state();
            return ctx->data_address(stream.get(), buffer_state->buffer_id);
        } catch (std::exception& ex) {
            Logger::get_logger().error(ex.what());
            return NULL;
        }
    }

    uint8_t* data() {
        try {
            const auto buffer_state = get_state();
            return ctx->data_address(stream.get(), buffer_state->buffer_id);
        } catch (std::exception& ex) {
            Logger::get_logger().error(ex.what());
            return NULL;
        }
    }

    uint8_t get_byte(size_t index) const {
        if (index >= buffer_size()) {
            throw DataOverflowException();
        }
        return data()[index];
    }

    py::bytes get_bytes(py::slice slice) const {
        size_t start, stop, step, slicelength;
        if (!slice.compute(buffer_size(), &start, &stop, &step, &slicelength)) {
            throw py::error_already_set();
        }

        std::string bytes;
        for (size_t i = start; i < stop; i += step) {
            bytes.append(std::string(reinterpret_cast<const char*>(data()) + i * sizeof(char), 1));
        }

        return bytes;
    }

    py::bytes read_all() {
        seek(0);
        return read(data_size());
    }

    py::bytes read(size_t count) {
        size_t from_index = tell();
        size_t to_index = from_index + count;

        auto bytes = get_bytes(py::slice(from_index, to_index, 1));
        seek(to_index);
        return bytes;
    }

    void set_byte(size_t index, const uint8_t value) {
        seek(index);
        write(std::string(reinterpret_cast<const char*>(&value), 1));        
    }

    void set_bytes(py::slice slice, const py::bytes value) {
        size_t start, stop, step, slicelength;
        if (!slice.compute(buffer_size(), &start, &stop, &step, &slicelength))
            throw py::error_already_set();

        if (slicelength > py::len(value)) {
            throw std::out_of_range("Incompatible sequence assignment");
        }

        seek(start);
        write(value);
    }

    void write(const py::bytes& value) {
        if (is_sent) {
            throw AlreadySentException();
        }

        size_t value_size = py::len(value);
        if (cursor_index + value_size > buffer_size()) {
            throw DataOverflowException();
        }

        uint8_t* data_pointer = data() + cursor_index * sizeof(uint8_t);
        py::buffer_info info(py::buffer(value).request());
        for (size_t i = 0; i < value_size; i++) {
            *(data_pointer + i * sizeof(uint8_t)) = *(reinterpret_cast<uint8_t*>(info.ptr) + i * sizeof(uint8_t));
        }

        cursor_index += value_size;
        if (cursor_index > max_cursor_index) {
            max_cursor_index = cursor_index;
        }
    }

    const size_t tell() const { return cursor_index; }

    const size_t seek(size_t index) {
        cursor_index = index;
        return tell();
    }

    const size_t truncate_to_current() {
        size_t current = tell();
        for (size_t i = cursor_index; i < max_cursor_index; i++) {
            write("\x00"s);
        }
        max_cursor_index = current;
        return max_cursor_index;
    }

    const size_t truncate(size_t index) {
        if (index >= buffer_size()) {
            throw DataOverflowException();
        }
        size_t current = tell();
        seek(index);
        size_t return_val = truncate_to_current();
        seek(current);
        return return_val;
    }

    uint16_t buffer_id() const { return get_state()->buffer_id; }
    size_t buffer_size() const { return get_state()->buffer_size; }
    size_t buffer_count() const { return get_state()->buffer_count; }
    size_t data_size() const { return std::max(get_state()->data_size, max_cursor_index); }
    uint64_t data_timestamp() const { return get_state()->data_timestamp; }
    uint64_t iteration() const { return get_state()->iteration; }

    py::buffer_info read_buffer_info() const { return py::buffer_info(data(), data_size()); }
    py::buffer_info write_buffer_info() { return py::buffer_info(data(), buffer_size(), false); }

    auto send_from_current(bool release) -> bool { return send(max_cursor_index, release); }

    auto send(size_t data_size, bool release) -> bool {
        if (is_sent) {
            throw AlreadySentException();
        }

        auto buffer_state = get_state();

        if (data_size > buffer_state->buffer_size) {
            throw DataOverflowException();
        }

        buffer_state->data_size = data_size;
        try {
            is_sent = ctx->send(stream.get(), *buffer_state);
            if (release) {
                _unchecked_buffer_state.reset();
            }
            return is_sent;
        } catch (std::exception& ex) {
            Logger::get_logger().error(ex.what());
            return false;
        }
    }
};

struct ReadBufferShim : BufferShim {
    ReadBufferShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const std::shared_ptr<BufferState>& buffer_state)
        : BufferShim(ctx, stream, buffer_state) {}

    ~ReadBufferShim() {}

    void release() {
        const auto copy = _unchecked_buffer_state;  // copy (could be none if previously released)
        if (copy) {
            ctx->release(stream.get(), *copy);
        }
        _unchecked_buffer_state.reset();
    }
};

struct WriteBufferShim : BufferShim {
    WriteBufferShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const std::shared_ptr<BufferState>& buffer_state)
        : BufferShim(ctx, stream, buffer_state) {}
};

struct StreamShim {
    std::shared_ptr<Context> ctx;
    std::shared_ptr<Stream> stream;
    ThreadingEventWrapper evt;
    float polling_interval;

    StreamShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const ThreadingEventWrapper& evt, double polling_interval)
        : ctx(ctx), stream(stream), evt(evt), polling_interval(polling_interval) {}
    ~StreamShim() = default;

    std::string name() { return stream->name(stream->get_control_lock()); }
    size_t fd() { return stream->fd(stream->get_control_lock()); }
    bool sync() { return stream->sync(stream->get_control_lock()); }
    size_t buffer_size() { return stream->buffer_size(stream->get_control_lock()); }
    size_t buffer_count() { return stream->buffer_count(stream->get_control_lock()); }
    size_t subscriber_count() { return stream->subscriber_count(stream->get_control_lock()); }
    bool is_ended() { return stream->is_ended(stream->get_control_lock()); }
    bool has_next() { return this->ctx->can_receive(stream.get()); }
    void end() { stream->end(stream->get_control_lock()); }

    auto next_to_send(bool blocking) -> std::shared_ptr<WriteBufferShim> {
        py::gil_scoped_release nogil;
        while (!evt.is_set()) {
            std::shared_ptr<BufferState> buffer_info = nullptr;

            try {
                buffer_info = ctx->next(stream.get());
            } catch (std::exception& ex) {
                Logger::get_logger().error(ex.what());
            }

            if (buffer_info == nullptr) {
                if (blocking) {
                    evt.wait(polling_interval);
                } else {
                    return {};
                }
            } else {
                return std::shared_ptr<WriteBufferShim>(
                    // instance
                    new WriteBufferShim(ctx, stream, buffer_info),
                    // deleter
                    [](WriteBufferShim* wbs) { delete wbs; });
            }
        }

        return {};
    }

    auto receive(uint64_t minimum_ts, bool blocking) -> std::shared_ptr<ReadBufferShim> {
        py::gil_scoped_release nogil;
        while (has_next() && !evt.is_set()) {
            std::shared_ptr<BufferState> buffer_info = nullptr;

            try {
                buffer_info = ctx->receive(stream.get(), minimum_ts);
            } catch (std::exception& ex) {
                Logger::get_logger().error(ex.what());
            }

            if (buffer_info == nullptr) {
                if (blocking) {
                    evt.wait(polling_interval);
                } else {
                    return {};
                }
            } else {
                return std::shared_ptr<ReadBufferShim>(
                    // instance
                    new ReadBufferShim(ctx, stream, buffer_info),
                    // deleter
                    [](ReadBufferShim* rbs) {
                        rbs->release();
                        delete rbs;
                    });
            }
        }

        return {};
    }

    auto send_string(const std::string& str, bool blocking, bool release) -> bool {
        std::shared_ptr<BufferShim> buffer = next_to_send(blocking);
        if (!buffer) {
            return false;
        }

        if (str.size() > this->buffer_size()) {
            throw DataOverflowException();
        }

        char* data = reinterpret_cast<char*>(buffer->data());
        std::copy(str.begin(), str.end(), data);

        return buffer->send(str.size(), release);
    }

    auto receive_string(uint64_t minimum_ts, bool blocking) -> std::optional<std::string> {
        std::shared_ptr<ReadBufferShim> buffer = receive(minimum_ts, blocking);
        if (!buffer) {
            return {};
        }

        char* data = reinterpret_cast<char*>(buffer->data());
        std::string str(data, buffer->data_size());
        buffer->release();
        return str;
    }
};

struct ConsumerStreamShim : StreamShim {
    ConsumerStreamShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const ThreadingEventWrapper& evt, double polling_interval)
        : StreamShim(ctx, stream, evt, polling_interval) {}
};
struct ProducerStreamShim : StreamShim {
    ProducerStreamShim(const std::shared_ptr<Context>& ctx, const std::shared_ptr<Stream>& stream, const ThreadingEventWrapper& evt, double polling_interval)
        : StreamShim(ctx, stream, evt, polling_interval) {}
};

static ProducerStreamShim producer_stream(const std::string& stream_name,
                                          size_t buffer_size,
                                          size_t buffer_count,
                                          bool sync,
                                          const py::object& evt,
                                          double polling_interval,
                                          const std::string& context) {
    auto c = std::make_shared<Context>(context);
    auto wrapped_evt = ThreadingEventWrapper(evt);

    Stream* stream = nullptr;

    try {
        stream = c->stream(stream_name, buffer_size, buffer_count, sync);
    } catch (std::exception& ex) {
        Logger::get_logger().error(ex.what());
    }

    if (!stream) {
        throw StreamExistsException();
    } else {
        ProducerStreamShim shim(c, std::shared_ptr<Stream>(stream, [c](Stream* stream) { /* cleaned up via Context destructor */ }), wrapped_evt,
                                polling_interval);
        return shim;
    }
}

static ConsumerStreamShim consumer_stream(const std::string& stream_name, const py::object& evt, double polling_interval, const std::string& context) {
    py::gil_scoped_release nogil;  // release gil since the constructor blocks

    auto ctx = std::make_shared<Context>(context);
    auto wrapped_evt = ThreadingEventWrapper(evt);
    Stream* stream = nullptr;

    try {
        stream = ctx->subscribe(std::string(stream_name));
    } catch (std::exception& ex) {
        Logger::get_logger().error(ex.what());
    }

    if (stream) {
        return ConsumerStreamShim(ctx,
                                  std::shared_ptr<Stream>(stream,
                                                          [ctx](Stream* stream) {
                                                              try {
                                                                  ctx->unsubscribe(stream);
                                                              } catch (std::exception& ex) {
                                                                  Logger::get_logger().error(ex.what());
                                                              }
                                                          }),
                                  wrapped_evt, polling_interval);
    }

    throw StreamUnavailableException();
}

inline LogLevel get_log_level() {
    return Logger::get_logger().get_level();
}
inline void set_log_level(LogLevel level) {
    return Logger::get_logger().set_level(level);
}

PYBIND11_MODULE(_mx, m) {
    py::register_exception<DataOverflowException>(m, "DataOverflow", PyExc_IndexError);
    py::register_exception<StreamUnavailableException>(m, "StreamUnavailable", PyExc_RuntimeError);
    py::register_exception<AlreadySentException>(m, "AlreadySent", PyExc_RuntimeError);
    py::register_exception<ReleasedBufferException>(m, "ReleasedBuffer", PyExc_RuntimeError);
    py::register_exception<StreamExistsException>(m, "StreamExists", PyExc_RuntimeError);

    py::enum_<LogLevel>(m, "LogLevel")
        .value("DEBUG", LogLevel::DEBUG)
        .value("INFO", LogLevel::INFO)
        .value("WARNING", LogLevel::WARNING)
        .value("ERROR", LogLevel::ERROR);

    m.def("get_log_level", &get_log_level);
    m.def("set_log_level", &set_log_level, "level"_a);

    py::class_<ThreadingEventWrapper>(m, "ThreadingEventWrapper").def(py::init<py::object>(), "cancel_event"_a).def("is_set", &ThreadingEventWrapper::is_set);

    py::class_<ReadBufferShim, std::shared_ptr<ReadBufferShim>>(m, "ReadBuffer", py::buffer_protocol())
        .def_buffer(&ReadBufferShim::read_buffer_info)
        .def("__getitem__", &ReadBufferShim::get_byte, "index"_a)
        .def("__getitem__", &ReadBufferShim::get_bytes, "slice"_a)
        .def("read", &ReadBufferShim::read_all)
        .def("read", &ReadBufferShim::read, "count"_a)
        .def("seek", &ReadBufferShim::seek, "index"_a)
        .def("tell", &ReadBufferShim::tell)
        .def("release", &ReadBufferShim::release)
        .def_property_readonly("buffer_id", &ReadBufferShim::buffer_id)
        .def_property_readonly("buffer_size", &ReadBufferShim::buffer_size)
        .def_property_readonly("buffer_count", &ReadBufferShim::buffer_count)
        .def_property_readonly("data_size", &ReadBufferShim::data_size)
        .def_property_readonly("data_timestamp", &ReadBufferShim::data_timestamp)
        .def_property_readonly("iteration", &ReadBufferShim::iteration);

    py::class_<WriteBufferShim, std::shared_ptr<WriteBufferShim>>(m, "WriteBuffer", py::buffer_protocol())
        .def_buffer(&WriteBufferShim::write_buffer_info)
        .def("__getitem__", &WriteBufferShim::get_byte, "index"_a)
        .def("__getitem__", &WriteBufferShim::get_bytes, "slice"_a)
        .def("__setitem__", &WriteBufferShim::set_byte, "index"_a, "value"_a)
        .def("__setitem__", &WriteBufferShim::set_bytes, "slice"_a, "value"_a)
        .def("read", &WriteBufferShim::read_all)
        .def("read", &WriteBufferShim::read, "count"_a)
        .def("seek", &WriteBufferShim::seek, "index"_a)
        .def("tell", &WriteBufferShim::tell)
        .def("write", &WriteBufferShim::write, "value"_a)
        .def("truncate", &WriteBufferShim::truncate_to_current)
        .def("truncate", &WriteBufferShim::truncate, "index"_a)
        .def("send", &WriteBufferShim::send, "data_size"_a, py::kw_only(), "release"_a = true)
        .def("send", &WriteBufferShim::send_from_current, py::kw_only(), "release"_a = true)
        .def_property_readonly("buffer_id", &WriteBufferShim::buffer_id)
        .def_property_readonly("buffer_size", &WriteBufferShim::buffer_size)
        .def_property_readonly("buffer_count", &WriteBufferShim::buffer_count)
        .def_property_readonly("data_size", &WriteBufferShim::data_size)
        .def_property_readonly("data_timestamp", &WriteBufferShim::data_timestamp)
        .def_property_readonly("iteration", &WriteBufferShim::iteration);

    py::class_<ProducerStreamShim>(m, "Producer")
        .def(py::init(&producer_stream), "stream_name"_a, "buffer_size"_a, "buffer_count"_a, "sync"_a = false, "cancel_event"_a = std::optional<py::object>(),
             "polling_interval"_a = 0.010, "context"_a = "/dev/shm")
        .def("next_to_send", &ProducerStreamShim::next_to_send, "blocking"_a = true)
        .def("send_string", &ProducerStreamShim::send_string, "message"_a, py::kw_only(), "blocking"_a = true, "release"_a = true)
        .def("end", &ProducerStreamShim::end)
        .def_property_readonly("subscriber_count", &ProducerStreamShim::subscriber_count)
        .def_property_readonly("buffer_count", &ProducerStreamShim::buffer_count)
        .def_property_readonly("buffer_size", &ProducerStreamShim::buffer_size)
        .def_property_readonly("fd", &ProducerStreamShim::fd)
        .def_property_readonly("is_sync", &ProducerStreamShim::sync)
        .def_property_readonly("is_ended", &ProducerStreamShim::is_ended)
        .def_property_readonly("name", &ProducerStreamShim::name);

    py::class_<ConsumerStreamShim>(m, "Consumer")
        .def(py::init(&consumer_stream), "stream_name"_a, "cancel_event"_a = std::optional<py::object>(), "polling_interval"_a = 0.010,
             "context"_a = "/dev/shm")
        .def("receive", &ConsumerStreamShim::receive, "minimum_ts"_a = 1, "blocking"_a = true)
        .def("receive_string", &ConsumerStreamShim::receive_string, "minimum_ts"_a = 1, "blocking"_a = true)
        .def_property_readonly("buffer_count", &ConsumerStreamShim::buffer_count)
        .def_property_readonly("buffer_size", &ConsumerStreamShim::buffer_size)
        .def_property_readonly("is_sync", &ConsumerStreamShim::sync)
        .def_property_readonly("is_ended", &ConsumerStreamShim::is_ended)
        .def_property_readonly("has_next", &ConsumerStreamShim::has_next)
        .def_property_readonly("name", &ConsumerStreamShim::name)
        .def_property_readonly("fd", &ConsumerStreamShim::fd);

    // TODO: nest within `inspect` or `debug` submodule maybe. Shouldn't be part of public API, probably.
    py::class_<MomentumX::ControlBlockHandle>(m, "Control")  // TODO: put in submodule
        .def("__repr__", [](const MomentumX::ControlBlockHandle& c) { return c.control_block->to_string(); });
    py::class_<MomentumX::ControlBlock>(m, "ControlSnapshot")  // TODO: put in submodule
        .def("__repr__", &MomentumX::ControlBlock::to_string);
    py::class_<MomentumX::Inspector>(m, "Inspector")  // TODO: put in submodule
        .def(py::init<const std::string&>(), "stream_name")
        .def("control_snapshot", &MomentumX::Inspector::control_snapshot, "timeout"_a = 0.5);
}
