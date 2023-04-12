import momentumx as mx
import threading
import signal
import time

STREAM = "mx://incrementer"

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

mx.set_log_level(mx.LogLevel.INFO)

stream = mx.Producer(STREAM, 100, 10, True, cancel)
while stream.subscriber_count == 0:
    print("waiting for subscriber(s)")
    if cancel.wait(0.5):
        break


n = 0
while not cancel.is_set() and  n < 500000:
    if stream.subscriber_count == 0:
        cancel.wait(0.5)
    elif stream.send_string(str(n)):
        print(f"Sent: {n}")
        n += 1
