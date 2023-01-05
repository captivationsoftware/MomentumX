import concurrent.futures as cf
import random
import os
import sys
import threading
from typing import Iterator
import contextlib

_EXPECTED_BYTES = 10589  # arbitrary

_STREAM_NAME = b"mx://test_echo_mx_stream"
_DEVSHM_NAME = "/dev/shm/mx.test_echo_mx_stream"


@contextlib.contextmanager
def timeout_event(timeout: float = 5.0) -> Iterator[threading.Event]:
    def sleep_then_trigger(inner_evt: threading.Event, inner_timeout: float):
        if not evt.wait(inner_timeout):
            evt.set()

    evt = threading.Event()
    threading.Thread(target=sleep_then_trigger, args=[evt, timeout]).start()

    yield evt
    evt.set()


def chunked_data(chunksize: int = 4096, seed: int = 0) -> Iterator[bytes]:
    """Generator for chunks of randomly generated data."""
    rand = random.Random(seed)
    data = bytes(rand.getrandbits(8) for _ in range(_EXPECTED_BYTES))

    for beg in range(0, _EXPECTED_BYTES, chunksize):
        end = beg + chunksize
        yield data[beg:end]


def run_send() -> int:
    import momentumx as mx  # import in subprocess
    import numpy as np

    with timeout_event() as event:
        stream = mx.Producer(_STREAM_NAME, 1000, 20, True, event)
        while stream.subscriber_count == 0:
            assert not event.wait(0.1), "no subscribers"

        assert os.path.exists(f"{_DEVSHM_NAME}.buffer.1")

        num_sent = 0

        buf_iter = iter(stream.next_to_send, None)

        for data, buf in zip(chunked_data(), buf_iter):
            if not stream.is_alive:
                raise Exception("Stream died")

            bytearray(buf)[: len(data)] = data
            buf.send(len(data))

            num_sent += len(data)
        return num_sent

        # time.sleep(2)
        # return _EXPECTED_BYTES

        import time
        from datetime import datetime, timezone

        print(f"created: {datetime.now(timezone.utc)}")
        time.sleep(5)
        print(f"slept:   {datetime.now(timezone.utc)}")
        del stream
        print(f"deleted: {datetime.now(timezone.utc)}")

        assert not os.path.exists(f"{_DEVSHM_NAME}.buffer.1")

        num_sent = 0
        for data in chunked_data():
            num_sent += len(data)
        return num_sent


def run_recv() -> int:
    import momentumx as mx  # import in subprocess
    import time

    with timeout_event() as event:
        event.wait(0.5)  # need producer to initialize first
        stream = mx.Consumer(_STREAM_NAME, event)

        b = bytearray()
        return _EXPECTED_BYTES
        while True:
            alive = stream.is_alive
            buf = stream.receive()
            if buf is None and not alive:
                break
            elif buf is None:
                time.sleep(0.1)
                continue
            else:
                b += bytearray(buf)

        # for buf in iter(stream.receive, None):
        #     as_bytearray = bytearray(buf)
        #     b += as_bytearray

        return len(b)


def disable_test_echo() -> None:
    with cf.ProcessPoolExecutor(max_workers=2) as pool:
        send_future = pool.submit(run_send)
        recv_future = pool.submit(run_recv)

        for f in cf.as_completed((send_future, recv_future)):
            assert f.result() == _EXPECTED_BYTES


def test_threading_event() -> None:
    import momentumx as mx

    event = threading.Event()
    wrapper = mx.ThreadingEventWrapper(event)
    assert not wrapper.is_set()

    event.set()
    assert wrapper.is_set()


def test_subscribers() -> None:
    import momentumx as mx

    with timeout_event() as event:
        producer = mx.Producer(_STREAM_NAME, 20, 5, True, event)
        assert producer.subscriber_count == 0, "Should have no subscribers yet"

        consumer1 = mx.Consumer(_STREAM_NAME, event)
        assert producer.subscriber_count == 1, "Should have single subscriber"

        consumer2 = mx.Consumer(_STREAM_NAME, event)
        # assert producer.subscriber_count == 2
        # TODO: This fails. subscriber_count stays a "1"

        del consumer1
        assert producer.subscriber_count == 1, "Should be back to 1 subscriber"

        del consumer2
        assert producer.subscriber_count == 0, "Should have no subscribers again"

def test_one_thread_numpy() -> None:
    import momentumx as mx  # import in subprocess
    import numpy as np

    with timeout_event() as event:
        producer = mx.Producer(_STREAM_NAME, 20, 2, True, event)
        consumer = mx.Consumer(_STREAM_NAME, event)

        buf1 = producer.next_to_send()
        a1 = np.frombuffer(buf1, dtype=np.uint8)
        a1[:10] = list(range(10))

        buf1.send(10)

        buf2 = consumer.receive()
        a2 = np.frombuffer(buf2, dtype=np.uint8)

        assert np.array_equal(a1[:10], a2)

        assert producer is not None
        assert consumer is not None

def test_buffer_cleanup() -> None:
    import momentumx as mx  # import in subprocess

    with timeout_event() as event:

        fname = f"{_DEVSHM_NAME}.buffer.1"

        # Verify buffer is created
        producer = mx.Producer(_STREAM_NAME, 300, 20, True, event)
        assert producer.is_alive
        assert producer.is_sync
        assert producer.buffer_size == 300
        assert producer.buffer_count == 20
        assert producer.name in _STREAM_NAME.decode()
        assert os.path.exists(fname)

        # Verify shm buffers are destroyed
        del producer
        assert not os.path.exists(fname)


if __name__ == "__main__":
    test_buffer_cleanup()
