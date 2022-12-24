import threading
import time
from ctypes import *

import momentumx as mx

STREAM = b'mx://threaded'


def consumer():
    context = mx.Context(0)
    time.sleep(1)
    stream = context.subscribe(STREAM)

    while context.is_subscribed(STREAM)  :
        string = context.receive_string(stream)
        if string is not None:
            print('Received:', string)

    context.term()

def producer():
    context = mx.Context()
    stream = context.stream(STREAM, 100, 10, True)
    i = 0
    try:
        while i < 1000:
            if context.send_string(stream, str(i)):
                print("Sent: ", i)
                i += 1
    except KeyboardInterrupt:
        context.term()
        context.destroy()
        return


t1 = threading.Thread(
    target=consumer
)

t2 = threading.Thread(
    target=producer
)

t1.start()
t2.start()

t1.join()
t2.join()

