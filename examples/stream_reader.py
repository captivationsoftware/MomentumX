import time

import momentumx as mx
import threading
import signal

STREAM = "mx://streamer"
ts_old = round(time.time())


now = time.time()
bytes_received = 0
messages_received = 0
last_iteration = 0
skip_count = 0

stream = mx.Consumer(STREAM)

while stream.has_next:
    buffer = stream.receive(blocking=False)

    if buffer:
        messages_received += 1
        bytes_received += buffer.data_size

        step = buffer.iteration - last_iteration
        if last_iteration > 0 and step > 1:
            skip_count = skip_count + step

        last_iteration = buffer.iteration

        ts_new = round(time.time())
        if ts_new != ts_old:
            ts_old = ts_new
            elapsed = time.time() - now
            print("Received {:.2f} msgs/sec".format(messages_received / elapsed))
            print("Received {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))
            print(f"Missed: {skip_count}")
            
            now = time.time()
            bytes_received = 0
            messages_received = 0
            last_iteration = 0
            skip_count = 0

        buffer.release()
