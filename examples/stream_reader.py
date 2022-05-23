from concurrent.futures import thread
from ctypes import *
import time

from momentum import Context

now = time.time()
bytes_received = 0
messages_received = 0

last_iteration = 0
skip_count = 0

STREAM = b'momentum://streamer'
THRESHOLD = 10000

context = Context()

context.subscribe(STREAM)

try:
    while context.subscribe(STREAM):
        buffer_data = context.receive_buffer(STREAM)
        if buffer_data is not None:
            messages_received += 1
            bytes_received += buffer_data.data_length

            step = buffer_data.iteration - last_iteration
            if last_iteration > 0 and step > 1:
                skip_count = skip_count + step

            last_iteration = buffer_data.iteration


            if (messages_received % THRESHOLD == 0):
                elapsed = time.time() - now
                print("Received {:.2f} msgs/sec".format(messages_received / elapsed))
                print("Received {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))

                print(f"Missed: {skip_count}")
                skip_count = 0

            context.release_buffer(STREAM, buffer_data)


except KeyboardInterrupt:
    context.term()
