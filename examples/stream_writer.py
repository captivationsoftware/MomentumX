import time

import momentumx as mx
import threading
import signal


STREAM = "mx://streamer"
ts_old = round(time.time())

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

now = time.time()
messages_sent = 0
bytes_sent = 0
data_length = int(100e6)

stream = mx.Producer(STREAM, data_length, 30, True, cancel, polling_interval=0.0)

while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    if cancel.wait(0.5):
        break


for buffer in iter(stream.next_to_send, None):
    buffer.send(data_length)

    messages_sent += 1
    bytes_sent += data_length

    ts_new = round(time.time())
    if ts_new != ts_old:
        ts_old = ts_new
        elapsed = time.time() - now
        print("Sent {:.2f} msgs/sec".format(messages_sent / elapsed))
        print("Sent {:.2f} GB/sec".format(bytes_sent / elapsed / 1.0e9))

        messages_sent = 0
        bytes_sent = 0

        now = time.time()
