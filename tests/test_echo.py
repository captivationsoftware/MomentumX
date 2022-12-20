import concurrent.futures as cf
import random
from typing import Iterator

_EXPECTED_BYTES = 10589  # arbitrary


def chunked_data(chunksize: int = 1000, seed: int = 0) -> Iterator[bytes]:
    """Generator for chunks of randomly generated data."""
    rand = random.Random(seed)
    data = bytes(rand.getrandbits(8) for _ in range(_EXPECTED_BYTES))

    for beg in range(0, _EXPECTED_BYTES, chunksize):
        end = beg + chunksize
        yield data[beg:end]


def run_send() -> int:
    import momentumx
    print(momentumx)
    num_sent = 0
    for data in chunked_data():
        num_sent += len(data)
    return num_sent


def run_recv() -> int:
    import momentumx
    print(momentumx)
    return _EXPECTED_BYTES


def test_echo() -> None:
    with cf.ProcessPoolExecutor(max_workers=2) as pool:
        send_future = pool.submit(run_send)
        recv_future = pool.submit(run_recv)

        for f in cf.as_completed((send_future, recv_future)):
            assert f.result() == _EXPECTED_BYTES
