import time

import momentumx as mx


STREAM = "mx://streamer"
ts_old = round(time.time())

now = time.time()
messages_sent = 0
bytes_sent = 0
data_length = int(100e6)

stream = mx.Producer(STREAM, data_length, 30, False)

while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    time.sleep(0.5)

while True:
    buffer = stream.next_to_send(blocking=False)

    if buffer:
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
