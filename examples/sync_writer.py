import momentumx as mx
import threading
import signal

STREAM = "mx://incrementer"

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

stream = mx.Producer(cancel, STREAM, 100, 10, True)
while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    if cancel.wait(0.5):
        break

for n in range(500000):
    assert stream.is_alive
    if stream.send_string(str(n)):
        print(f"Sent: {n}")
