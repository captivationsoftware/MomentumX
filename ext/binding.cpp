#include "buffer.h"
#include "context.h"
#include "momentumx.h"
#include "stream.h"
#include <condition_variable>
#include <cstddef>
#include <list>
#include <memory>
#include <pybind11/attr.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <stack>
#include <thread>

namespace py = pybind11;
using namespace py::literals; // provides `_a` literal for parameter names

using BufferState = MomentumX::Stream::BufferState;
using Logger = MomentumX::Utils::Logger;
using LogLevel = Logger::Level;
using MomentumX::Buffer;
using MomentumX::Context;
using MomentumX::Stream;

struct BufferShim;
struct BufferStateShim;
struct ReadBufferShim;
struct StreamShim;
struct WriteBufferShim;

template <typename T>
class ReusableStack
{
public:
    void emplace(std::unique_ptr<T> &&ptr)
    {
        _ctx->holder.emplace_back(std::move(ptr));
        _ctx->avail.push_back(ptr.get());
        _ctx->cv.notify_one();
    }

    bool checkout(std::shared_ptr<T> &out)
    {
        auto ctx = _ctx; // copy to maintain ownership throughout checkout
        std::unique_lock<std::mutex> lock(ctx->m);

        if (ctx->holder.empty())
        {
            return false;
        }

        ctx->cv.wait(lock, [&ctx]
                     { return !ctx->avail.empty(); });

        auto checkin_deleter = [ctx](T *t)
        {
            std::unique_lock<std::mutex> lock(ctx->m);
            ctx->avail.push(t);
            ctx->cv.notify_one();
        };

        out = std::shared_ptr<T>(ctx->avail.top(), checkin_deleter);
        ctx->avail.pop();
        return true;
    }

private:
    struct Context
    {
        std::mutex m;
        std::condition_variable cv;
        std::list<std::unique_ptr<T>> holder;
        std::stack<T *> avail;
    };

    std::shared_ptr<Context> _ctx{std::make_shared<Context>()};
};

struct ThreadingEventWrapper
{
    py::object evt;
    ThreadingEventWrapper(py::object evt) : evt(evt) {}

    bool is_set()
    {
        if (evt.is_none())
        {
            return false;
        }

        py::gil_scoped_acquire lock; // acquire before calling into python code
        return evt.attr("is_set")().cast<bool>();
    }

    bool wait(double timeout)
    {
        py::gil_scoped_acquire lock; // acquire before calling into python code
        if (evt.is_none())
        {
            std::this_thread::sleep_for(std::chrono::nanoseconds(int(timeout * 1e9)));
            return false;
        }
        return evt.attr("wait")(timeout).cast<bool>();
    }
};

// TODO: BufferState -> BufferInfo

struct BufferStateShim
{
    std::shared_ptr<Context> ctx{nullptr};
    std::shared_ptr<Stream> stream{nullptr};
    std::shared_ptr<BufferState> buffer_state{nullptr};

    BufferStateShim(const std::shared_ptr<Context> &ctx,
                    const std::shared_ptr<Stream> &stream,
                    const std::shared_ptr<BufferState> &buffer_state)
        : ctx(ctx), stream(stream), buffer_state(buffer_state) {}
    ~BufferStateShim() = default;

    const uint8_t *data() const { return mx_data_address(ctx.get(), stream.get(), buffer_state->buffer_id); }
    uint8_t *data() { return mx_data_address(ctx.get(), stream.get(), buffer_state->buffer_id); }

    uint16_t buffer_id() const { return buffer_state->buffer_id; }
    size_t buffer_size() const { return buffer_state->buffer_size; }
    size_t buffer_count() const { return buffer_state->buffer_count; }
    size_t data_size() const { return buffer_state->data_size; }
    uint64_t data_timestamp() const { return buffer_state->data_timestamp; }
    uint64_t iteration() const { return buffer_state->iteration; }

    py::buffer_info read_buffer_info() const { return py::buffer_info(data(), data_size()); }
    py::buffer_info write_buffer_info() { return py::buffer_info(data(), buffer_size(), false); }

    auto send(size_t data_size) -> bool
    {
        BufferState *copy = new BufferState(*buffer_state); // mx_stream_send takes ownership and deletes
        copy->data_size = data_size;
        return mx_stream_send(ctx.get(), stream.get(), copy);
    }
};

struct ReadBufferShim : BufferStateShim
{
    ReadBufferShim(const std::shared_ptr<Context> &ctx, const std::shared_ptr<Stream> &stream, const std::shared_ptr<BufferState> &buffer_state) : BufferStateShim(ctx, stream, buffer_state) {}
};
struct WriteBufferShim : BufferStateShim
{
    WriteBufferShim(const std::shared_ptr<Context> &ctx, const std::shared_ptr<Stream> &stream, const std::shared_ptr<BufferState> &buffer_state) : BufferStateShim(ctx, stream, buffer_state) {}
};

struct StreamShim
{
    std::shared_ptr<Context> ctx;
    std::shared_ptr<Stream> stream;
    ThreadingEventWrapper evt;
    float polling_interval;

    StreamShim(const std::shared_ptr<Context> &ctx,
               const std::shared_ptr<Stream> &stream,
               const ThreadingEventWrapper &evt,
               double polling_interval)
        : ctx(ctx), stream(stream), evt(evt), polling_interval(polling_interval) {}
    ~StreamShim() = default;

    bool is_alive() { return this->stream->is_alive(); }
    std::string name() { return this->stream->name(); }
    size_t fd() { return this->stream->fd(); }
    bool sync() { return this->stream->sync(); }
    size_t buffer_size() { return this->stream->buffer_size(); }
    size_t buffer_count() { return this->stream->buffer_count(); }
    size_t subscriber_count() { return this->stream->subscribers().size(); }

    auto next_to_send(bool blocking) -> std::optional<WriteBufferShim>
    {
        py::gil_scoped_release nogil;
        while (!evt.is_set())
        {
            MomentumX::Stream::BufferState *buffer_info = mx_stream_next(ctx.get(), stream.get());
            if (buffer_info == nullptr)
            {
                if (blocking)
                {
                    evt.wait(polling_interval);
                }
                else
                {
                    return {};
                }
            }
            else
            {
                return WriteBufferShim(
                    ctx,
                    stream,
                    std::shared_ptr<BufferState>(buffer_info));
            }
        }

        return {};
    }

    auto receive(uint64_t minimum_ts, bool blocking) -> std::optional<ReadBufferShim>
    {
        py::gil_scoped_release nogil;
        while (!evt.is_set())
        {
            if (!stream->is_alive())
            {
                Logger::get_logger().info("cannot receive: stream not alive");
                return {};
            }

            BufferState *buffer_info = mx_stream_receive(ctx.get(), stream.get(), minimum_ts);
            if (buffer_info == nullptr)
            {
                if (blocking)
                {
                    evt.wait(polling_interval);
                }
                else
                {
                    return {};
                }
            }
            else
            {
                auto ctx_cp = ctx;       // copy for lambda
                auto stream_cp = stream; // copy for lambda
                return ReadBufferShim(
                    ctx,
                    stream,
                    std::shared_ptr<BufferState>(buffer_info,
                                                 [ctx_cp, stream_cp](BufferState *buffer_state)
                                                 { mx_stream_release(ctx_cp.get(), stream_cp.get(), buffer_state); }));
            }
        }

        return {};
    }

    auto flush() -> void { mx_stream_flush(ctx.get(), stream.get()); }

    auto send_string(const std::string &str, bool blocking) -> bool
    {
        std::optional<BufferStateShim> buffer = next_to_send(blocking);
        if (!buffer)
        {
            return false;
        }

        if (str.size() > buffer->buffer_state->buffer_size)
        {
            throw std::runtime_error("Cannot send string: larger than buffer");
        }

        const size_t str_size = str.size();
        const size_t buf_size = buffer->buffer_state->buffer_size;
        char *data = reinterpret_cast<char *>(buffer->data());

        std::copy(str.begin(), str.end(), data);
        return buffer->send(str_size);
    }

    auto receive_string(uint64_t minimum_ts, bool blocking) -> std::string
    {
        std::optional<BufferStateShim> buffer = receive(minimum_ts, blocking);
        if (!buffer)
        {
            return "";
        }

        char *data = reinterpret_cast<char *>(buffer->data());
        return std::string(data, buffer->data_size());
    }
};

struct ConsumerStreamShim : StreamShim
{
    ConsumerStreamShim(const std::shared_ptr<Context> &ctx,
                       const std::shared_ptr<Stream> &stream,
                       const ThreadingEventWrapper &evt,
                       double polling_interval)
        : StreamShim(ctx, stream, evt, polling_interval) {}
};
struct ProducerStreamShim : StreamShim
{
    ProducerStreamShim(const std::shared_ptr<Context> &ctx,
                       const std::shared_ptr<Stream> &stream,
                       const ThreadingEventWrapper &evt,
                       double polling_interval)
        : StreamShim(ctx, stream, evt, polling_interval) {}
};

static ProducerStreamShim producer_stream(const std::string &stream_name,
                                          size_t buffer_size,
                                          size_t buffer_count,
                                          bool sync,
                                          const py::object &evt,
                                          double polling_interval)
{
    auto c = Context::scoped_producer_singleton();
    auto wrapped_evt = ThreadingEventWrapper(evt);
    ProducerStreamShim shim(
        c,
        std::shared_ptr<Stream>(mx_stream(c.get(), stream_name.c_str(), buffer_size, buffer_count, sync), [c](Stream *stream) { /* cleaned up via Context destructor */ }),
        wrapped_evt,
        polling_interval);
    return shim;
}

static auto consumer_stream(const std::string &stream_name,
                            const py::object &evt,
                            double polling_interval) -> ConsumerStreamShim
{
    namespace sc = std::chrono;
    py::gil_scoped_release nogil; // release gil since the constructor blocks

    auto c = Context::scoped_consumer_singleton();
    auto wrapped_evt = ThreadingEventWrapper(evt);
    Stream *stream = nullptr;

    do
    {
        stream = mx_subscribe(c.get(), stream_name.c_str());
        if (stream)
        {
            return ConsumerStreamShim(
                c,
                std::shared_ptr<Stream>(stream, [c](Stream *stream) { /* mx_unsubscribe(c.get(), stream); */ }),
                wrapped_evt,
                polling_interval);
        }
    } while (!wrapped_evt.wait(0.1));

    throw std::runtime_error("Unable to create consumer stream: subscription timed out");
}

inline LogLevel get_log_level() { return Logger::get_logger().get_level(); }
inline void set_log_level(LogLevel level) { return Logger::get_logger().set_level(level); }

PYBIND11_MODULE(_mx, m)
{
    py::enum_<LogLevel>(m, "LogLevel")
        .value("DEBUG", LogLevel::DEBUG)
        .value("INFO", LogLevel::INFO)
        .value("WARNING", LogLevel::WARNING)
        .value("ERROR", LogLevel::ERROR);

    m.def("get_log_level", &get_log_level);
    m.def("set_log_level", &set_log_level, "level"_a);

    py::class_<ThreadingEventWrapper>(m, "ThreadingEventWrapper")
        .def(py::init<py::object>(), "cancel_event"_a)
        .def("is_set", &ThreadingEventWrapper::is_set);

    py::class_<ReadBufferShim, std::shared_ptr<ReadBufferShim>>(m, "ReadBuffer", py::buffer_protocol())
        .def_buffer(&ReadBufferShim::read_buffer_info)
        .def_property_readonly("buffer_id", &ReadBufferShim::buffer_id)
        .def_property_readonly("buffer_size", &ReadBufferShim::buffer_size)
        .def_property_readonly("buffer_count", &ReadBufferShim::buffer_count)
        .def_property_readonly("data_size", &ReadBufferShim::data_size)
        .def_property_readonly("data_timestamp", &ReadBufferShim::data_timestamp)
        .def_property_readonly("iteration", &ReadBufferShim::iteration);

    py::class_<WriteBufferShim, std::shared_ptr<WriteBufferShim>>(m, "WriteBuffer", py::buffer_protocol())
        .def_buffer(&WriteBufferShim::write_buffer_info)
        .def("send", &WriteBufferShim::send, "data_size"_a)
        .def_property_readonly("buffer_id", &WriteBufferShim::buffer_id)
        .def_property_readonly("buffer_size", &WriteBufferShim::buffer_size)
        .def_property_readonly("buffer_count", &WriteBufferShim::buffer_count)
        .def_property_readonly("data_size", &WriteBufferShim::data_size)
        .def_property_readonly("data_timestamp", &WriteBufferShim::data_timestamp)
        .def_property_readonly("iteration", &WriteBufferShim::iteration);

    py::class_<ProducerStreamShim>(m, "Producer")
        .def(py::init(&producer_stream), "stream_name"_a, "buffer_size"_a, "buffer_count"_a, "sync"_a = false, "cancel_event"_a = std::optional<py::object>(), "polling_interval"_a = 0.010)
        .def("next_to_send", &ProducerStreamShim::next_to_send, "blocking"_a = true)
        .def("flush", &ProducerStreamShim::flush)
        .def("send_string", &ProducerStreamShim::send_string, "message"_a, "blocking"_a = true)
        .def_property_readonly("subscriber_count", &ProducerStreamShim::subscriber_count)
        .def_property_readonly("buffer_count", &ProducerStreamShim::buffer_count)
        .def_property_readonly("buffer_size", &ProducerStreamShim::buffer_size)
        .def_property_readonly("fd", &ProducerStreamShim::fd)
        .def_property_readonly("is_alive", &ProducerStreamShim::is_alive)
        .def_property_readonly("is_sync", &ProducerStreamShim::sync)
        .def_property_readonly("name", &ProducerStreamShim::name);

    py::class_<ConsumerStreamShim>(m, "Consumer")
        .def(py::init(&consumer_stream), "stream_name"_a, "cancel_event"_a = std::optional<py::object>(), "polling_interval"_a = 0.010)
        .def("receive", &ConsumerStreamShim::receive, "minimum_ts"_a = 1, "blocking"_a = true)
        .def("flush", &ConsumerStreamShim::flush)
        .def("receive_string", &ConsumerStreamShim::receive_string, "minimum_ts"_a = 1, "blocking"_a = true)
        .def_property_readonly("buffer_count", &ConsumerStreamShim::buffer_count)
        .def_property_readonly("buffer_size", &ConsumerStreamShim::buffer_size)
        .def_property_readonly("fd", &ConsumerStreamShim::fd)
        .def_property_readonly("is_alive", &ConsumerStreamShim::is_alive)
        .def_property_readonly("is_sync", &ConsumerStreamShim::sync)
        .def_property_readonly("name", &ConsumerStreamShim::name);
}
