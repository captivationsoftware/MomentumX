from concurrent.futures import thread
from ctypes import *
import time

from momentum import Context

now = time.time()
bytes_received = 0
messages_received = 0

last_iteration = 0
skip_count = 0

def on_read(data, data_length, ts, iteration):
    global now
    global bytes_received
    global messages_received
    global last_iteration
    global skip_count

    messages_received += 1
    bytes_received += data_length

    step = iteration - last_iteration
    if last_iteration > 0 and step > 1:
        skip_count = skip_count + step

    last_iteration = iteration

    threshold = 10000

    if (messages_received % threshold == 0):
        elapsed = time.time() - now
        print("Received {:.2f} msgs/sec".format(messages_received / elapsed))
        print("Received {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))

        print(f"Missed: {skip_count}")
        skip_count = 0


STREAM = b'momentum://streamer'

context = Context()

context.subscribe(STREAM)

try:
    while True:
        context.read(STREAM, on_read)
except KeyboardInterrupt:
    context.term()
