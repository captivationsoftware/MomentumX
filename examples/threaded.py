import threading
import time

import momentumx as mx

STREAM = "mx://threaded"


def consumer(cancel):
    context = mx.Context()
    time.sleep(1)
    stream = context.subscribe(STREAM)

    while context.is_subscribed(STREAM) and not cancel.is_set():
        string = stream.receive_string()
        if string:
            print("Received:", string)
    cancel.set()


def producer(cancel):
    context = mx.Context()
    stream = context.stream(STREAM, 100, 10, True)
    i = 0
    while i < 1000 and not cancel.is_set():
        if stream.send_string(str(i)):
            print("Sent: ", i)
            i += 1
    
    time.sleep(1)

cancel = threading.Event()
t1 = threading.Thread(target=consumer, args=(cancel,))
t2 = threading.Thread(target=producer, args=(cancel,))

t1.start()
t2.start()

try:
    while not cancel.wait(0.5):
        pass
finally:
    cancel.set()
    t1.join()
    t2.join()

