# MomentumX

<p align="center">
    <img src="https://github.com/captivationsoftware/MomentumX/blob/main/Logo.png?raw=true" title="MomentumX Logo" />
    <br/>
    <span>
        <strong>MomentumX</strong> is a <strong>zero-copy shared memory IPC</strong> library for building complex <strong>streaming data pipelines</strong> capable of processing <strong>large datasets</strong> using <strong>Python</strong>. 
    </span>
</p>

<br />

### Key Features:
- High-Throughput, Low Latency
- Supports **streaming and synchronous** modes for use within a wide variety of use cases. 
- Bring your own encoding, or use **raw binary** data.
- Small footprint with **zero dependencies**.
- Sane **data protections** to ensure **reliability of data** in a cooperative computing environment. 
- Pairs with other high-performance libraries, such as **numpy** and **scipy**, to support parallel processing of memory-intensive scientific data.
- Works on most modern versions of **Linux** using shared memory (via `/dev/shm`).
- Seamlessly integrates into a **Docker** environment with minimal configuration, and readily enables lightweight container-to-container data sharing. 

### Examples:
Below are some simplified use cases for common MomentumX workflows. Consult the examples in the `examples/` directory for additional details and implementation guidance.

#### Streaming Mode (e.g. lossy)
```python
# Producer Process
import momentumx as mx

# Create a stream with a total capacity of 10MB
stream = mx.Producer('my_stream', buffer_size=int(1e6), buffer_count=10, sync=False)

# Write the series 0-9 repeatedly to a buffer 1000 times
for i in range(0, 1000):
    buffer = stream.next_to_send()
    buffer.write(f'{i % 10}'.encode('utf8')) # Note: writing to buffer via [<index>] and [<start_index>:<stop_index>] is also possible
    buffer.send() # Note: call with .send(<num bytes>) if you want to explicitly control the data_size parameter, otherwise internal cursor will be used
```

```python
# Consumer Process(es)
import momentumx as mx

stream = mx.Consumer('my_stream')

while stream.is_alive:
    # Receive from the stream as long as the stream is available 
    buffer = stream.receive()
    print(buffer[:buffer.data_size])
```

#### Syncronous Mode (e.g. lossless)
```python
# Producer Process
import momentumx as mx
import threading
import signal

cancel_event = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel_event.set()))

# Create a stream with a total capacity of 10MB
stream = mx.Producer('my_stream', buffer_size=int(1e6), buffer_count=10, sync=True) # NOTE: sync set to True

min_subscribers = 1

while stream.subscriber_count < min_subscribers:
    print("waiting for subscriber(s)")
    if cancel_event.wait(0.5):
        break

print("All expected subscribers are ready")

# Write the series 0-999 to a consumer 
for n in range(0, 1000):
    if stream.subscriber_count == 0:
        cancel_event.wait(0.5)

    # Note: sending strings directly is possible via the send_string call
    elif stream.send_string(str(n)):
        print(f"Sent: {n}")

```

```python
# Consumer Process(es)
import momentumx as mx

stream = mx.Consumer('my_stream')

while stream.is_alive:
    # Note: receiving strings is possible as well via the receive_string call
    print(f"Received: {stream.receive_string()}")

```

#### Numpy Integration
```python
import momentumx as mx
import numpy as np

# Create a stream
stream = mx.Consumer('numpy_stream')

# Receive the next buffer (or if a producer, obtain the next_to_send buffer)
buffer = stream.receive()

# Create a numpy array directly from the memory without any copying
np_buff = np.frombuffer(buffer, dtype=uint8)

```


#### Isolated Contexts
MomentumX allows for the usage of streams outside of `/dev/shm` (the default location). Pass the `context` kwarg pointing to a directory on the filesystem for both the `Producer` and all `Consumer` instances to create isolated contexts.

This option is useful if access to `/dev/shm` is unsuitable.

```python
import momentumx as mx

# Create a producer attached to the context path /my/path
stream = mx.Producer('my_stream', ..., context='/my/path/')
...

# Create Consumer elsewhere attached to the same context of /my/path
stream = mx.Consumer('my_stream', context='/my/path/')

```

### License
Captivation Software, LLC offers **MomentumX** under an **Unlimited Use License to the United States Government**, with **all other parties subject to the GPL-3.0 License**.

### Inquiries / Requests
All inquiries and requests may be sent to <a href="mailto:opensource@captivation.us">opensource@captivation.us</a>.


<sub><sup>
    Copyright &copy; 2022-2023 - <a href="https://captivation.us" target="_blank">Captivation Software, LLC</a>.
</sup></sub>