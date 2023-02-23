import threading
import time

import momentumx as mx

STREAM = "mx://threaded"

send_counter = 0
recv_counter = 0


def consumer(cancel: threading.Event):
    global recv_counter
    time.sleep(0.1)
    stream = mx.Consumer(STREAM, cancel)

    while stream.is_alive and not cancel.is_set():
        string = stream.receive_string()
        if string:
            recv_counter += 1
    cancel.set()


def producer(cancel: threading.Event):
    global send_counter
    stream = mx.Producer(STREAM, 100, 10, True, cancel)
    while not cancel.is_set():
        if stream.send_string(str(send_counter)):
            send_counter += 1

    time.sleep(1)


cancel = threading.Event()
t1 = threading.Thread(target=consumer, args=(cancel,))
t2 = threading.Thread(target=producer, args=(cancel,))

t1.start()
t2.start()

try:
    start_time = time.time()
    scheduled_end_time = start_time + 10
    while not cancel.wait(1.0) and time.time() < scheduled_end_time:
        delta = time.time() - start_time
        print(f"{send_counter} send, {send_counter/delta} msg/sec")
        print(f"{recv_counter} recv, {recv_counter/delta} msg/sec")

except KeyboardInterrupt:
    print("received ctrl-c")
finally:
    cancel.set()
    t1.join()
    t2.join()
