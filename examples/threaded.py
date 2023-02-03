import threading
import time

import momentumx as mx

STREAM = "mx://threaded"

def consumer(cancel: threading.Event):
    time.sleep(1)
    stream = mx.Consumer(STREAM, cancel, context='/tmp')

    while stream.is_alive and not cancel.is_set():
        string = stream.receive_string()
        if string:
            print("Received:", string)
    cancel.set()


def producer(cancel: threading.Event):
    stream = mx.Producer(STREAM, 100, 10, True, cancel, context='/tmp')
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
except KeyboardInterrupt:
    print("received ctrl-c")
finally:
    cancel.set()
    t1.join()
    t2.join()
