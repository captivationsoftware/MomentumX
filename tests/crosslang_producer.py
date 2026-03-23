#!/usr/bin/env python3
"""Cross-language validation: Python producer.

Sends MX_MSG_COUNT messages to a MomentumX stream, waits for a subscriber
before sending, then calls end() when done.

Environment variables:
  MX_CONTEXT      — context path  (default: /dev/shm)
  MX_STREAM_NAME  — stream name   (default: crosslang)
  MX_MSG_COUNT    — message count (default: 10)
  MX_SYNC             — sync mode 1/0  (default: 1)
                        1 = block each send until subscriber acknowledges (guaranteed delivery)
                        0 = streaming, non-blocking (consumer may miss messages)
  MX_MIN_SUBSCRIBERS  — minimum subscribers before starting to send (default: 1)
"""
import os
import sys
import time

import momentumx as mx

ctx_path       = os.environ.get("MX_CONTEXT", "/dev/shm")
stream_name    = os.environ.get("MX_STREAM_NAME", "crosslang")
msg_count      = int(os.environ.get("MX_MSG_COUNT", "10"))
sync           = os.environ.get("MX_SYNC", "1") != "0"
min_subscribers = int(os.environ.get("MX_MIN_SUBSCRIBERS", "1"))
timeout        = 30.0

print(f"[python-producer] ctx={ctx_path}  stream={stream_name}  count={msg_count}  sync={sync}  min_subs={min_subscribers}")

producer = mx.Producer(stream_name, 256, 4, sync, context=ctx_path)

print(f"[python-producer] waiting for {min_subscribers} subscriber(s)...")
deadline = time.monotonic() + timeout
while producer.subscriber_count < min_subscribers:
    if time.monotonic() > deadline:
        print(f"[python-producer] ERROR: timed out waiting for subscribers (have {producer.subscriber_count}, need {min_subscribers})", file=sys.stderr)
        sys.exit(1)
    time.sleep(0.05)

print(f"[python-producer] {producer.subscriber_count} subscriber(s) connected, sending {msg_count} messages")

for i in range(msg_count):
    msg = f"hello from python {i}"
    deadline = time.monotonic() + timeout
    while True:
        buf = producer.next_to_send(blocking=False)
        if buf is not None:
            msg_bytes = msg.encode()
            buf[: len(msg_bytes)] = msg_bytes
            buf.send(len(msg_bytes))
            print(f"[python-producer] sent [{i}]: {msg}")
            break
        if time.monotonic() > deadline:
            print("[python-producer] ERROR: timed out waiting for buffer", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.01)

producer.end()
print("[python-producer] done — stream ended")
