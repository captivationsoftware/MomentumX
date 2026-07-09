import time

import momentumx as mx
import threading
import signal

STREAM = "mx://streamer"
ts_old = round(time.time())


now = time.time()
bytes_received = 0
messages_received = 0
skip_count = 0

stream = mx.Consumer(STREAM)

while stream.has_next:
    buffer = stream.receive(blocking=False)

    if buffer:
        messages_received += 1
        bytes_received += buffer.data_size

        # buffer.dropped_count is how many iterations were skipped to reach
        # *this* buffer (nonzero if we fell behind and the producer overwrote
        # buffers before we could read them). No manual iteration-diffing needed
        # -- previously this required tracking last_iteration by hand.
        skip_count += buffer.dropped_count

        ts_new = round(time.time())
        if ts_new != ts_old:
            ts_old = ts_new
            elapsed = time.time() - now
            print("Received {:.2f} msgs/sec".format(messages_received / elapsed))
            print("Received {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))
            print(f"Missed: {skip_count}")
            # stream.dropped_count is the running cumulative total across this
            # consumer's whole lifetime, if you'd rather not track it yourself:
            print(f"Missed (cumulative): {stream.dropped_count}")

            now = time.time()
            bytes_received = 0
            messages_received = 0
            skip_count = 0

        buffer.release()
