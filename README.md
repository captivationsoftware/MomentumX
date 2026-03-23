# MomentumX

<p align="center">
    <img src="https://github.com/captivationsoftware/MomentumX/blob/main/Logo.png?raw=true" title="MomentumX Logo" />
    <br/>
    <span>
        <strong>MomentumX</strong> is a <strong>zero-copy shared memory IPC</strong> library for building complex <strong>streaming data pipelines</strong> capable of processing <strong>large datasets</strong>.
        Bindings are available for <strong>Python</strong>, <strong>Rust</strong>, and <strong>C</strong>, and streams are fully interoperable across languages.
    </span>
</p>

<br />

### Key Features:
- High-Throughput, Low Latency
- **Multi-language**: Python, Rust, and C bindings share the same wire format — a Rust producer and a Python consumer work out of the box.
- Supports **streaming and synchronous** modes for use within a wide variety of use cases.
- Bring your own encoding, or use **raw binary** data.
- Sane **data protections** to ensure **reliability of data** in a cooperative computing environment.
- Pairs with other high-performance libraries, such as **numpy** and **scipy**, to support parallel processing of memory-intensive scientific data.
- Works on most modern versions of **Linux** using shared memory (via `/dev/shm`).
- Seamlessly integrates into a **Docker** environment with minimal configuration, and readily enables lightweight container-to-container data sharing.

---

## Python Bindings

Install from PyPI:
```sh
pip install momentumx
```

#### Stream Mode
```python
# Producer Process
import momentumx as mx

# Create a stream with a total capacity of 10MB (1MB x 10)
stream = mx.Producer('my_stream', buffer_size=int(1e6), buffer_count=10, sync=False)

# Obtain the next available buffer for writing
buffer = stream.next_to_send()
buffer.write(b'1')

buffer.send()
# NOTE: buffer.send() can also be passed an explicit number of bytes.
# Otherwise an internally managed cursor will be used.
```

```python
# Consumer Process(es)
import momentumx as mx

stream = mx.Consumer('my_stream')

# Receive from my_stream as long as the stream has not ended OR there are unread buffers
while stream.has_next:

    # Block while waiting to receive buffer
    # NOTE: Non-blocking receive is possible using blocking=False keyword argument
    buffer = stream.receive()

    # If we are here, either the stream ended OR we have a buffer, so check...
    if buffer is not None:

        # We have buffer containing data, so print the entire contents
        print(buffer.read(buffer.data_size))

        # See also "Implicit versus Explicit Buffer Release" section below.
```

#### Sync Mode
```python
# Producer Process
import momentumx as mx
import threading
import signal

cancel_event = threading.Event()
signal.signal(signal.SIGINT, (lambda _sig, _frm: cancel_event.set()))

# Create a stream with a total capacity of 10MB
stream = mx.Producer(
    'my_stream',
    buffer_size=int(1e6),
    buffer_count=10,
    sync=True  # NOTE: sync set to True
)

min_subscribers = 1

while stream.subscriber_count < min_subscribers:
    print("waiting for subscriber(s)")
    if cancel_event.wait(0.5):
        break

print("All expected subscribers are ready")

# Write the series 0-999 to consumers
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

while stream.has_next:
    data = stream.receive_string()

    if data is not None:
        print(f"Received: {data}")
```

#### Iterator Syntax
```python
import momentumx as mx

stream = mx.Consumer(STREAM)

# Iterate over buffers until stream.receive() returns None
for buffer in iter(stream.receive, None):
    print(buffer[0])                    # print first byte
    print(buffer[1:buffer.data_size])   # print remaining bytes
```

#### Numpy Integration
```python
import momentumx as mx
import numpy as np

stream = mx.Consumer('numpy_stream')
buffer = stream.receive()

# Create a numpy array directly from the memory without any copying
np_buff = np.frombuffer(buffer, dtype=np.uint8)
```

#### Implicit versus Explicit Buffer Release
MomentumX Consumers automatically release a buffer once all references are destroyed. To control release explicitly:

```python
stream = mx.Consumer('my_stream')
buffer = stream.receive()

# Access to buffer is safe!
buffer.read(10)

# Buffer is returned back to the available buffer pool.
# Be sure you are truly done with your data!
buffer.release()

# DANGER: DO NOT DO THIS!
# All operations on a buffer after calling `release` are unsafe!
buffer.read(10)
```

#### Isolated Contexts
```python
import momentumx as mx

# Create a producer attached to the context path /my/path
stream = mx.Producer('my_stream', ..., context='/my/path/')
...

# Create Consumer elsewhere attached to the same context
stream = mx.Consumer('my_stream', context='/my/path/')
```

---

## Rust Bindings

Add to `Cargo.toml`:
```toml
[dependencies]
momentumx = "0.1"
```

The crate bundles the C++ source and builds it automatically via cmake — no manual steps required.

#### Producer
```rust
use momentumx::{Context, LogLevel};
use std::time::Duration;

let ctx = Context::new("/dev/shm").expect("failed to create context");
ctx.set_log_level(LogLevel::Warning);

let stream = ctx
    .producer("my_stream", 1024 * 1024, 10, false)
    .expect("failed to create producer stream");

// Wait for at least one subscriber
while stream.subscriber_count() == 0 {
    std::thread::sleep(Duration::from_millis(100));
}

if let Some(mut buf) = stream.next() {
    let len = {
        let data = buf.data_mut().unwrap();
        data[..5].copy_from_slice(b"hello");
        5
    };
    buf.send(len).unwrap();
}

stream.end();
```

#### Consumer
```rust
use momentumx::Context;

let ctx = Context::new("/dev/shm").expect("failed to create context");
let stream = ctx.subscribe("my_stream").expect("failed to subscribe");

while stream.has_next() {
    if let Some(buf) = stream.receive(1) {
        let data = buf.data().unwrap();
        println!("received {} bytes: {:?}", data.len(), data);
        // buf.release() is called automatically on drop
    }
}
```

#### Sync mode with multiple consumers
```rust
// sync=true — producer blocks on each send until all subscribers acknowledge
let stream = ctx
    .producer("my_stream", 1024 * 1024, 4, true)
    .expect("create stream");

while stream.subscriber_count() < 2 {
    std::thread::sleep(Duration::from_millis(50));
}

for i in 0..10u8 {
    if let Some(mut buf) = stream.next() {
        buf.data_mut().unwrap()[0] = i;
        buf.send(1).unwrap();
    }
}
stream.end();
```

---

## C API

The C API (`ext/mx_c_api.h`) exposes the full MomentumX feature set for use from C or any language with a C FFI.

```c
#include "mx_c_api.h"

// --- Producer ---
MxContext* ctx = mx_context_create("/dev/shm");
MxStream*  stream = mx_stream_create(ctx, "my_stream", 1024*1024, 10, false);

while (mx_subscriber_count(ctx, stream) == 0) { /* wait */ }

MxBufferState state;
if (mx_next(ctx, stream, &state) == 1) {
    uint8_t* data = mx_data_address(ctx, stream, state.buffer_id);
    memcpy(data, "hello", 5);
    state.data_size = 5;
    mx_send(ctx, stream, &state);
}
mx_end(ctx, stream);
mx_stream_unsubscribe(ctx, stream);
mx_context_destroy(ctx);

// --- Consumer ---
MxContext* ctx = mx_context_create("/dev/shm");
MxStream*  stream = mx_stream_subscribe(ctx, "my_stream");

MxBufferState state;
while (mx_receive(ctx, stream, 1, &state) >= 0) {
    uint8_t* data = mx_data_address(ctx, stream, state.buffer_id);
    // process data[0..state.data_size]
    mx_release(ctx, stream, &state);
    if (mx_is_ended(ctx, stream)) break;
}
mx_stream_unsubscribe(ctx, stream);
mx_context_destroy(ctx);
```

Key rules:
- Each producer and consumer should use its own `MxContext`.
- Always call `mx_release` after consuming a buffer (or use the Rust/Python wrappers which do this automatically).
- `mx_stream_unsubscribe` frees the `MxStream` handle; do not use it afterwards.
- Stream names are limited to **32 characters**.

---

## Cross-Language Interoperability

All three bindings share the same shared-memory wire format. A Python producer can feed Rust consumers, a Rust producer can feed Python consumers, and so on:

```python
# Python producer
import momentumx as mx
producer = mx.Producer('shared', buffer_size=256, buffer_count=4, sync=True)
```

```rust
// Rust consumer — reads from the same stream
let ctx = Context::new("/dev/shm").unwrap();
let stream = ctx.subscribe("shared").unwrap();
while stream.has_next() {
    if let Some(buf) = stream.receive(1) {
        println!("{:?}", buf.data().unwrap());
    }
}
```

Cross-language streams also work across **Docker containers** — use `ipc: shareable` on the producer container and `ipc: service:<producer>` on consumer containers to share `/dev/shm` between them.

---

### License
Captivation Software, LLC offers **MomentumX** under an **Unlimited Use License to the United States Government**, with **all other parties subject to the GPL-3.0 License**.

### Inquiries / Requests
All inquiries and requests may be sent to <a href="mailto:opensource@captivation.us">opensource@captivation.us</a>.


<sub><sup>
    Copyright &copy; 2022-2025 - <a href="https://captivation.us" target="_blank">Captivation Software, LLC</a>.
</sup></sub>
