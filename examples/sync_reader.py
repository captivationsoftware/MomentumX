import momentumx as mx
import threading
import signal

STREAM = "mx://incrementer"

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

stream = mx.Consumer(STREAM, cancel)

while stream.has_next:
    string = stream.receive_string()
    if string:
        print("Received:", string)
    else:
        if cancel.wait(0.5):
            break
        print("Waiting for data")
print(stream.has_next)
    