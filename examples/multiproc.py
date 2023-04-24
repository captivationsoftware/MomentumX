import multiprocessing
import time

import momentumx as mx

STREAM = "mx://threaded"

def consumer(cancel: multiprocessing.Event):
    time.sleep(1)
    stream = mx.Consumer(STREAM, cancel)

    for string in iter(stream.receive_string, None):
        print("Received:", string)
        if cancel.is_set():
            break
    cancel.set()


def producer(cancel: multiprocessing.Event):
    stream = mx.Producer(STREAM, 100, 10, True, cancel)
    while stream.subscriber_count == 0:
        if cancel.wait(0.1):
            raise Exception("Canceled")
    i = 0
    while i < 1000 and not cancel.is_set():
        if stream.send_string(str(i)):
            print("Sent: ", i)
            i += 1
    
    time.sleep(1)


cancel = multiprocessing.Event()
t1 = multiprocessing.Process(target=consumer, args=(cancel,))
t2 = multiprocessing.Process(target=producer, args=(cancel,))

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
