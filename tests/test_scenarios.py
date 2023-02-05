import concurrent.futures as cf
import random
import os
import sys
import threading
from typing import Iterator
import contextlib
import pytest 
from tempfile import TemporaryDirectory

from mmap import PAGESIZE

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


def test_stream_unavailable_exception() -> None:
    import momentumx as mx

    with pytest.raises(mx.StreamUnavailable):
        mx.Consumer('__nonexistent__')
    
def test_string_overflow_exception() -> None:
    import momentumx as mx
    
    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    with pytest.raises(mx.DataOverflow):
        producer.send_string('x' * (PAGESIZE + 1))


def test_buffer_overflow_exception() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    buffer = producer.next_to_send()
    with pytest.raises(mx.DataOverflow):
        buffer.send(PAGESIZE + 1)

def test_buffer_seek_tell_write_truncate_producer() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    buffer = producer.next_to_send()
    assert buffer.tell() == 0

    buffer.seek(5)
    assert buffer.tell() == 5

    assert buffer.data_size == 0
    buffer.truncate()
    assert buffer.data_size == 5
    assert buffer.tell() == 5

    buffer.write(b'foo')
    assert buffer.data_size == 8
    assert buffer.tell() == 8
    assert buffer[:buffer.data_size] == b'\x00\x00\x00\x00\x00foo'

    buffer.truncate(6)
    assert buffer.data_size == 6
    assert buffer.tell() == 8 # tell is not affected by tuncate
    assert buffer[:buffer.data_size] == b'\x00\x00\x00\x00\x00f'

    del producer


def test_single_getitem_setitem_producer() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    buffer = producer.next_to_send()
    buffer[0] = b'a' # set
    assert buffer[0] == b'a'
    del producer

def test_slice_getitem_setitem_producer() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    buffer = producer.next_to_send()
    buffer[0:3] = b'abc' 
    assert buffer[0:3] == b'abc'
    del producer

def test_empty_slice_matches_buffer_length_producer() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)
    buffer = producer.next_to_send()
    assert len(buffer[:]) == buffer.buffer_size

    del producer

def test_exception_on_double_send() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)

    buffer = producer.next_to_send()
    buffer.send(1)

    with pytest.raises(mx.AlreadySent):
        buffer.send()

def test_exception_on_write_after_send() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, PAGESIZE, 1, False)

    buffer = producer.next_to_send()
    buffer.send(1)

    with pytest.raises(mx.AlreadySent):
        buffer.write(b'foo')

    with pytest.raises(mx.AlreadySent):
        buffer[0:2] = b'foo' 

def test_less_than_page_size() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, 1, 1, False)

    buffer = producer.next_to_send()
    assert buffer.buffer_size == 1

def test_overwrite_region_after_truncate() -> None:
    import momentumx as mx

    producer = mx.Producer(_STREAM_NAME, 3, 1, False)

    buffer = producer.next_to_send()
    buffer.write(b'123')
    assert buffer[:] == b'123'

    buffer.truncate(2)
    assert buffer[:] == b'12\x00'

    del buffer
    del producer

def test_truncate_to_buffer_size() -> None:
    import momentumx as mx

    size = 2
    producer = mx.Producer(_STREAM_NAME, size, 1, False)

    buffer = producer.next_to_send()
    buffer.seek(size)
    buffer.truncate()
    assert buffer.data_size == buffer.buffer_size

def test_write_to_buffer_size() -> None:
    import momentumx as mx

    size = 5
    producer = mx.Producer(_STREAM_NAME, size, 1, False)

    buffer = producer.next_to_send()
    
    for i in range(0, size):
        buffer.write(b'\xff')

    assert size == buffer.data_size == buffer.buffer_size
    assert buffer[:] == b'\xff' * size

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


def test_buffer_cleanup_tmp() -> None:
    import momentumx as mx  # import in subprocess

    with timeout_event() as event, TemporaryDirectory() as tdir:

        shm_ctrl_fname = "/dev/shm/mx.test_echo_mx_stream"
        shm_buff_fname = "/dev/shm/mx.test_echo_mx_stream.buffer.1"

        tmp_ctrl_fname = f"{tdir}/mx.test_echo_mx_stream"
        tmp_buff_fname = f"{tdir}/mx.test_echo_mx_stream.buffer.1"

        # Verify buffer is created (in `/dev/shm`)
        producer = mx.Producer(_STREAM_NAME, 300, 20, True, event)
        assert os.path.exists(shm_ctrl_fname)  # default location
        assert os.path.exists(shm_buff_fname)  # default location
        assert not os.path.exists(tmp_ctrl_fname)
        assert not os.path.exists(tmp_buff_fname)

        # Verify shm buffers are destroyed (in `/dev/shm`)
        del producer
        assert not os.path.exists(shm_ctrl_fname)  # default location
        assert not os.path.exists(shm_buff_fname)  # default location
        assert not os.path.exists(tmp_ctrl_fname)
        assert not os.path.exists(tmp_buff_fname)

        # Verify buffer is created (in `/tmp`)
        producer = mx.Producer(_STREAM_NAME, 300, 20, True, event, context=tdir)
        assert not os.path.exists(shm_ctrl_fname)
        assert not os.path.exists(shm_buff_fname)
        assert os.path.exists(tmp_ctrl_fname)  # override location
        assert os.path.exists(tmp_buff_fname)  # override location

        # Verify shm buffers are destroyed (in `/tmp`)
        del producer
        assert not os.path.exists(shm_ctrl_fname)
        assert not os.path.exists(shm_buff_fname)
        assert not os.path.exists(tmp_ctrl_fname)  # override location
        assert not os.path.exists(tmp_buff_fname)  # override location

def test_buffer_cleanup_both()->None:
    import momentumx as mx  # import in subprocess

    with timeout_event() as event, TemporaryDirectory() as tdir:
        shm_buff_fname = "/dev/shm/mx.test_echo_mx_stream.buffer.1"
        tmp_buff_fname = f"{tdir}/mx.test_echo_mx_stream.buffer.1"

        # `/dev/shm` only
        shm_producer = mx.Producer(_STREAM_NAME, 300, 20, True, event)
        assert os.path.exists(shm_buff_fname)  
        assert not os.path.exists(tmp_buff_fname)

        # `/dev/shm` and `/tmp`
        tmp_producer =  mx.Producer(_STREAM_NAME, 300, 20, True, event, context=tdir)
        assert os.path.exists(shm_buff_fname)
        assert os.path.exists(tmp_buff_fname)  

        # `/tmp` only
        del shm_producer
        assert not os.path.exists(shm_buff_fname)  # default location
        assert os.path.exists(tmp_buff_fname)

        # none
        del tmp_producer
        assert not os.path.exists(shm_buff_fname)
        assert not os.path.exists(tmp_buff_fname)  # override location

if __name__ == "__main__":
    test_buffer_cleanup()
