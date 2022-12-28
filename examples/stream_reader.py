import time

import momentumx as mx


STREAM = "mx://streamer"
THRESHOLD = 10000

now = time.time()
bytes_received = 0
messages_received = 0
last_iteration = 0
skip_count = 0

context = mx.Context()
stream = context.subscribe(STREAM)
while context.is_subscribed(STREAM):
    buffer_state = stream.receive()

    if buffer_state is not None:
        messages_received += 1
        bytes_received += buffer_state.data_size

        step = buffer_state.iteration - last_iteration
        if last_iteration > 0 and step > 1:
            skip_count = skip_count + step

        last_iteration = buffer_state.iteration

        if messages_received % THRESHOLD == 0:
            elapsed = time.time() - now
            print("Received {:.2f} msgs/sec".format(messages_received / elapsed))
            print("Received {:.2f} GB/sec".format(bytes_received / elapsed / 1.0e9))
            print(f"Missed: {skip_count}")
            skip_count = 0

        stream.release(buffer_state)
