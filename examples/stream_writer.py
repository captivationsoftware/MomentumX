from ctypes import *
import time

from momentum import Context

data_length = int(100e6)

STREAM = b'momentum://streamer'
THRESHOLD = 10000

context = Context()

now = time.time()
messages_sent = 0
bytes_sent = 0

try:
    stream = context.stream(STREAM, data_length, 30)
    while True:
        buffer_state = context.next(stream)

        if buffer_state:
            buffer_state.data_size = data_length

            context.send(stream, buffer_state)
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


