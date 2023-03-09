import threading
import time
import sys
import signal
import concurrent.futures as cf

import momentumx as mx

STREAM = "mx://threaded"

start_time = 0
stop_time = 0
send_counter = 0
recv_counter = 0


def consumer(cancel: threading.Event) -> None:
    global recv_counter
    time.sleep(0.1)
    stream = mx.Consumer(STREAM, cancel)

    assert stream.has_next

    while stream.has_next and not cancel.is_set():
        string = stream.receive_string()
        if string:
            recv_counter += 1
    cancel.set()


def producer(cancel: threading.Event) -> None:
    global send_counter
    stream = mx.Producer(STREAM, 100, 10, True, cancel)
    while not cancel.is_set():
        if stream.send_string(str(send_counter)):
            send_counter += 1

    time.sleep(1)


def printer(cancel: threading.Event) -> None:
    global start_time
    global stop_time
    global send_counter
    global recv_counter
    start_time = time.time()
    while not cancel.wait(1.0):
        delta = time.time() - start_time
        print(f"{send_counter} send, {send_counter/delta:.01f} msg/sec")
        print(f"{recv_counter} recv, {recv_counter/delta:.01f} msg/sec")

    stop_time = time.time()


def cancel_in(cancel: threading.Event, timeout: float) -> None:
    cancel.wait(timeout)
    cancel.set()


cancel = threading.Event()
signal.signal(signal.SIGINT, lambda *_: cancel.set())
signal.signal(signal.SIGTERM, lambda *_: cancel.set())

with cf.ThreadPoolExecutor(max_workers=sys.maxsize) as pool:
    fs = set(
        (
            pool.submit(consumer, cancel),
            pool.submit(producer, cancel),
            pool.submit(printer, cancel),
            pool.submit(cancel_in, cancel, 10.0),
        ),
    )

    for f in cf.as_completed(fs):
        f.result()


delta = stop_time - start_time
print(f"{send_counter} send, {send_counter/delta:.01f} msg/sec (final)")
print(f"{recv_counter} recv, {recv_counter/delta:.01f} msg/sec (final)")
