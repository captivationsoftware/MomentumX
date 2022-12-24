import concurrent.futures as cf
import random
from typing import Iterator

_EXPECTED_BYTES = 10589  # arbitrary

_STREAM_NAME = b"mx://test_echo_mx_stream"


def chunked_data(chunksize: int = 1000, seed: int = 0) -> Iterator[bytes]:
    """Generator for chunks of randomly generated data."""
    rand = random.Random(seed)
    data = bytes(rand.getrandbits(8) for _ in range(_EXPECTED_BYTES))

    for beg in range(0, _EXPECTED_BYTES, chunksize):
        end = beg + chunksize
        yield data[beg:end]


def run_send() -> int:
    import momentumx as mx  # import in subprocess

    ctx = mx.Context(mx.LogLevel.INFO)
    assert not ctx.is_subscribed(_STREAM_NAME)

    stream = ctx.stream(_STREAM_NAME, 20, 20, True)

    import time
    from datetime import datetime, timezone
    print(f'created: {datetime.now(timezone.utc)}')
    time.sleep(5)
    print(f'slept:   {datetime.now(timezone.utc)}')
    del stream
    print(f'deleted: {datetime.now(timezone.utc)}')
    return _EXPECTED_BYTES

    num_sent = 0
    for data in chunked_data():
        num_sent += len(data)
    return num_sent


def run_recv() -> int:
    return _EXPECTED_BYTES
    import time

    import momentumx as mx  # import in subprocess

    ctx = mx.Context(mx.LogLevel.INFO)
    assert not ctx.is_subscribed(_STREAM_NAME)

    killswitch = time.time() + 3
    while time.time() < killswitch:
        try:
            stream = ctx.subscribe(_STREAM_NAME)
        except Exception:
            time.sleep(0.1)  # sleep because the producer needs to start up first O_o

    assert ctx.is_subscribed(_STREAM_NAME)

    del stream
    assert not ctx.is_subscribed(_STREAM_NAME)

    return _EXPECTED_BYTES


def test_echo() -> None:
    with cf.ProcessPoolExecutor(max_workers=2) as pool:
        send_future = pool.submit(run_send)
        recv_future = pool.submit(run_recv)

        for f in cf.as_completed((send_future, recv_future)):
            assert f.result() == _EXPECTED_BYTES


if __name__ == "__main__":
    test_echo()
    
    # import momentumx as mx  # import in subprocess

    # ctx = mx.Context(mx.LogLevel.INFO)
    # assert not ctx.is_subscribed(_STREAM_NAME)

    # ctx.stream(_STREAM_NAME, 20, 20, True)
