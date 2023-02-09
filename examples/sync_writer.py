import momentumx as mx
import threading
import signal
import time

STREAM = "mx://incrementer"

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

stream = mx.Producer(STREAM, 100, 10, True, cancel)
while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    if cancel.wait(0.5):
        break

for n in range(1, 500000):
    assert stream.is_alive

    if stream.subscriber_count == 0:
        cancel.wait(0.5)
    elif stream.send_string(str(n)):
        print(f"Sent: {n}")
