from ctypes import *
import time

from momentum import Context

data_length = int(100e6)

STREAM = b'momentum://streamer'
THRESHOLD = 10000

context = Context()
context.min_buffers = 30

now = time.time()
messages_sent = 0
bytes_sent = 0

try:
    while True:
        buffer = context.next_buffer(STREAM, data_length)

        if (buffer):
            context.send_buffer(buffer, data_length, 0)
            messages_sent += 1
            bytes_sent += data_length

            if (messages_sent % THRESHOLD == 0):
                elapsed = time.time() - now
                print("Sent {:.2f} msgs/sec".format(messages_sent / elapsed))
                print("Sent {:.2f} GB/sec".format(bytes_sent / elapsed / 1.0e9))

                messages_sent = 0
                bytes_sent = 0

                now = time.time()



except KeyboardInterrupt:
    context.term()


