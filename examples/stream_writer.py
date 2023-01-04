import time

import momentumx as mx
import threading
import signal


STREAM = "mx://streamer"
THRESHOLD = 10000

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

now = time.time()
messages_sent = 0
bytes_sent = 0
data_length = int(100e6)

stream = mx.Producer(cancel, STREAM, data_length, 30, True)

while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    if cancel.wait(0.5):
        break

buf_iter = iter(stream.next_to_send, None)

while True:
    buffer = next(buf_iter)

    if buffer:
        buffer.send(data_length)

        messages_sent += 1
        bytes_sent += data_length

        if messages_sent % THRESHOLD == 0:
            elapsed = time.time() - now
            print("Sent {:.2f} msgs/sec".format(messages_sent / elapsed))
            print("Sent {:.2f} GB/sec".format(bytes_sent / elapsed / 1.0e9))

            messages_sent = 0
            bytes_sent = 0

            now = time.time()
