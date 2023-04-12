from typing import Optional
import momentumx as mx
import threading
import signal

STREAM = "mx://incrementer"

cancel = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel.set()))

stream = mx.Consumer(STREAM, cancel)

mx.set_log_level(mx.LogLevel.INFO)

prev_val: Optional[int] = None

while stream.has_next:
    string = stream.receive_string()
    if string:
        if prev_val is None:
            prev_val = int(string)
        else:
            trial = int(string)
            assert trial == prev_val + 1, f"Counter error: {trial} != {prev_val+1}"
            prev_val = trial
        print("Received:", string)
    else:
        if cancel.wait(0.5):
            break
        print("Waiting for data")
print(stream.has_next)
    
