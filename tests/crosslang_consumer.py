#!/usr/bin/env python3
"""Cross-language validation: Python consumer.

Subscribes to a MomentumX stream (retrying until available), receives all
messages until the stream ends, then validates the count.
Exits 0 on success, 1 on failure.

Environment variables:
  MX_CONTEXT        — context path      (default: /dev/shm)
  MX_STREAM_NAME    — stream name       (default: crosslang)
  MX_MSG_COUNT      — expected messages (default: 10)
  MX_STRICT_COUNT   — 1/0 (default: 1)
                      0 = non-sync mode; partial delivery is expected, just log the count
"""
import os
import sys
import time

import momentumx as mx

ctx_path     = os.environ.get("MX_CONTEXT", "/dev/shm")
stream_name  = os.environ.get("MX_STREAM_NAME", "crosslang")
expected     = int(os.environ.get("MX_MSG_COUNT", "10"))
strict_count = os.environ.get("MX_STRICT_COUNT", "1") != "0"
timeout      = 30.0

print(f"[python-consumer] ctx={ctx_path}  stream={stream_name}  expected={expected}")

# Retry subscribing until the producer has created the stream.
deadline = time.monotonic() + timeout
consumer = None
while True:
    try:
        consumer = mx.Consumer(stream_name, context=ctx_path)
        break
    except mx.StreamUnavailable:
        if time.monotonic() > deadline:
            print("[python-consumer] ERROR: timed out waiting for stream", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.1)

print("[python-consumer] subscribed — reading messages")

received = 0
while received < expected and consumer.has_next:
    msg = consumer.receive_string(blocking=True)
    if msg is None:
        break
    print(f"[python-consumer] got [{received}]: {msg}")
    received += 1

print(f"[python-consumer] done — received {received}/{expected}")
if received != expected:
    if strict_count:
        print(
            f"[python-consumer] FAIL: expected {expected} messages, got {received}",
            file=sys.stderr,
        )
        sys.exit(1)
    else:
        print(f"[python-consumer] OK (non-sync): received {received} of {expected} messages")
        sys.exit(0)
print("[python-consumer] PASS")
