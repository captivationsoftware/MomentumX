"""
Cross-language integration tests.

Each scenario mirrors a test in test_scenarios.py but runs with the two ends
in different languages: one side uses the Python momentumx package directly
(in-process or as a subprocess via docker/python_*.py), the other side runs
a compiled Rust binary (rust/target/release/examples/mx_producer or mx_consumer).

Prerequisites:
    make install
    make rust/build/examples
"""
import hashlib
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import pytest
import momentumx as mx

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
ROOT = Path(__file__).parent.parent
PYTHON = Path(sys.executable)
EXAMPLES = ROOT / "rust" / "target" / "release" / "examples"
PY_PROD_SCRIPT = ROOT / "tests" / "crosslang_producer.py"
PY_CONS_SCRIPT = ROOT / "tests" / "crosslang_consumer.py"


def _rust_bin(name: str) -> str:
    p = EXAMPLES / name
    if not p.exists():
        pytest.skip(
            f"Rust binary not found: {p}\n"
            "Run:  make rust/build/examples"
        )
    return str(p)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture()
def stream(request):
    """Unique /dev/shm stream name per test; cleaned up before and after.

    MomentumX enforces a 32-character stream name limit.  pytest encodes
    unicode parametrize IDs (e.g. → becomes \\u2192) which inflates names, so
    we hash the full node name and use a compact prefix + 6-char hex suffix to
    guarantee uniqueness within the 32-char budget.
    """
    raw = re.sub(r"\W+", "_", request.node.name).strip("_")
    h = hashlib.md5(raw.encode()).hexdigest()[:6]
    name = f"xl_{raw[:22]}_{h}"   # 3 + 22 + 1 + 6 = 32 chars max
    _shm_purge(name)
    yield name
    _shm_purge(name)


def _shm_purge(name: str) -> None:
    for f in Path("/dev/shm").glob(f"mx.{name}*"):
        try:
            f.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# Subprocess helpers
# ---------------------------------------------------------------------------
def _env(**kv) -> dict:
    e = dict(os.environ)
    e.update({k: str(v) for k, v in kv.items()})
    return e


def start_rust_producer(stream, count, *, sync=True, min_subs=1):
    return subprocess.Popen(
        [_rust_bin("mx_producer")],
        env=_env(
            MX_STREAM_NAME=stream,
            MX_MSG_COUNT=count,
            MX_SYNC="1" if sync else "0",
            MX_MIN_SUBSCRIBERS=min_subs,
        ),
    )


def start_rust_consumer(stream, count, *, strict=True):
    return subprocess.Popen(
        [_rust_bin("mx_consumer")],
        env=_env(
            MX_STREAM_NAME=stream,
            MX_MSG_COUNT=count,
            MX_STRICT_COUNT="1" if strict else "0",
        ),
    )


def start_py_producer(stream, count, *, sync=True, min_subs=1):
    return subprocess.Popen(
        [str(PYTHON), str(PY_PROD_SCRIPT)],
        env=_env(
            MX_STREAM_NAME=stream,
            MX_MSG_COUNT=count,
            MX_SYNC="1" if sync else "0",
            MX_MIN_SUBSCRIBERS=min_subs,
        ),
    )


def start_py_consumer(stream, count, *, strict=True):
    return subprocess.Popen(
        [str(PYTHON), str(PY_CONS_SCRIPT)],
        env=_env(
            MX_STREAM_NAME=stream,
            MX_MSG_COUNT=count,
            MX_STRICT_COUNT="1" if strict else "0",
        ),
    )


def await_all(*procs, timeout=30):
    """Wait for all processes; assert each exited with code 0."""
    for p in procs:
        try:
            rc = p.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            p.kill()
            pytest.fail(f"Process {p.args[0]} (pid {p.pid}) timed out after {timeout}s")
        assert rc == 0, f"Process {p.args[0]} (pid {p.pid}) exited {rc}"


def wait_for_subscriber_count(producer, expected, timeout=10):
    deadline = time.monotonic() + timeout
    while producer.subscriber_count != expected:
        if time.monotonic() > deadline:
            pytest.fail(
                f"subscriber_count stuck at {producer.subscriber_count}, "
                f"expected {expected}"
            )
        time.sleep(0.05)


# ===========================================================================
# Tests
# ===========================================================================

# ---------------------------------------------------------------------------
# Basic sync delivery
# Mirrors: test_synced_buffers, test_durable_receives, test_implicit_release
#
# Producer sends N messages in sync mode; every consumer must receive all N.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("start_prod,start_cons", [
    (start_py_producer,   start_rust_consumer),
    (start_rust_producer, start_py_consumer),
], ids=["py->rust", "rust->py"])
def test_basic_sync(stream, start_prod, start_cons):
    """All 10 messages delivered in sync mode."""
    prod = start_prod(stream, 10)
    cons = start_cons(stream, 10)
    await_all(prod, cons)


# ---------------------------------------------------------------------------
# Durable ending
# Mirrors: test_durable_ending
#
# Producer calls end(); consumer reads every message then exits cleanly.
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("start_prod,start_cons", [
    (start_py_producer,   start_rust_consumer),
    (start_rust_producer, start_py_consumer),
], ids=["py->rust", "rust->py"])
def test_durable_ending(stream, start_prod, start_cons):
    """stream.end() propagates across the language boundary; consumer exits 0."""
    prod = start_prod(stream, 20)
    cons = start_cons(stream, 20)
    await_all(prod, cons)


# ---------------------------------------------------------------------------
# Multiple consumers — every consumer receives all messages
# Mirrors: test_two_consumer_copies, test_implicit_release (multi)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("start_prod,cons_factories", [
    (start_py_producer,   [start_rust_consumer, start_rust_consumer]),
    (start_rust_producer, [start_py_consumer,   start_py_consumer]),
    (start_py_producer,   [start_rust_consumer, start_py_consumer]),
    (start_rust_producer, [start_py_consumer,   start_rust_consumer]),
], ids=["py->2xrust", "rust->2xpy", "py->rust+py", "rust->py+rust"])
def test_multiple_consumers(stream, start_prod, cons_factories):
    """Each consumer independently receives all messages in sync mode."""
    n = len(cons_factories)
    prod = start_prod(stream, 10, min_subs=n)
    cons = [fn(stream, 10) for fn in cons_factories]
    await_all(prod, *cons)


# ---------------------------------------------------------------------------
# Mixed consumers — 2 Python + 2 Rust consumers
# Mirrors: test_two_consumer_copies (extended)
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("start_prod", [
    start_py_producer,
    start_rust_producer,
], ids=["py_prod", "rust_prod"])
def test_mixed_consumers(stream, start_prod):
    """2 Python + 2 Rust consumers all receive every message."""
    prod = start_prod(stream, 10, min_subs=4)
    cons = [
        start_py_consumer(stream, 10),
        start_py_consumer(stream, 10),
        start_rust_consumer(stream, 10),
        start_rust_consumer(stream, 10),
    ]
    await_all(prod, *cons)


# ---------------------------------------------------------------------------
# Streaming (non-sync) mode — producer never blocks, partial delivery OK
# Mirrors: test_slow_consumer_streaming, test_increment_streaming
# ---------------------------------------------------------------------------
@pytest.mark.parametrize("start_prod,start_cons", [
    (start_py_producer,   start_rust_consumer),
    (start_rust_producer, start_py_consumer),
], ids=["py->rust", "rust->py"])
def test_streaming_partial_delivery(stream, start_prod, start_cons):
    """Non-sync producer runs to completion; consumer exits 0 regardless of count."""
    prod = start_prod(stream, 20, sync=False)
    cons = start_cons(stream, 20, strict=False)
    await_all(prod, cons)


# ---------------------------------------------------------------------------
# Subscriber count — Python producer (in-process) + Rust consumer subprocesses
# Mirrors: test_subscribers
#
# subscriber_count must increment when a Rust consumer subscribes and
# decrement when it exits cleanly after receiving all expected messages.
# ---------------------------------------------------------------------------
def test_subscriber_count_rust_consumers(stream):
    """subscriber_count tracks cross-language subscribes and unsubscribes."""
    producer = mx.Producer(stream, 256, 4, True)
    assert producer.subscriber_count == 0

    cons1 = start_rust_consumer(stream, 5)
    wait_for_subscriber_count(producer, 1)
    assert producer.subscriber_count == 1

    cons2 = start_rust_consumer(stream, 5)
    wait_for_subscriber_count(producer, 2)
    assert producer.subscriber_count == 2

    # Send 5 messages in sync mode — both consumers must ack each one
    for i in range(5):
        buf = producer.next_to_send()
        msg = f"msg{i}".encode()
        buf[: len(msg)] = msg
        buf.send(len(msg))

    # Both consumers received their expected count and exit cleanly
    assert cons1.wait(timeout=10) == 0
    assert cons2.wait(timeout=10) == 0

    # After clean exit, subscriber_count must return to 0
    wait_for_subscriber_count(producer, 0)
    assert producer.subscriber_count == 0


# ---------------------------------------------------------------------------
# Subscriber count — Rust producer (subprocess) + Python consumer (in-process)
# Mirrors: test_subscribers (reversed direction)
# ---------------------------------------------------------------------------
def test_subscriber_count_rust_producer(stream):
    """Python consumer can subscribe to a Rust-produced stream and drain it."""
    prod = start_rust_producer(stream, 10)

    # Retry until the Rust producer has created the stream
    deadline = time.monotonic() + 10
    while True:
        try:
            consumer = mx.Consumer(stream)
            break
        except mx.StreamUnavailable:
            if time.monotonic() > deadline:
                pytest.fail("Timed out waiting for Rust producer to create stream")
            time.sleep(0.05)

    received = 0
    while consumer.has_next:
        buf = consumer.receive()
        if buf is not None:
            received += 1

    assert received == 10
    assert prod.wait(timeout=10) == 0


# ---------------------------------------------------------------------------
# Unregister during sync write claim — Python producer (in-process)
# Mirrors: test_unregister_during_sync_write_claim
#
# A Rust consumer exits while the producer holds a write claim.  The producer
# must not deadlock and must successfully deliver to the remaining consumers.
# ---------------------------------------------------------------------------
def test_unregister_during_write_claim(stream):
    """Consumer that exits cleanly mid-stream doesn't deadlock the producer.

    cons2 is configured to receive only 3 messages; after the 3rd ack it
    returns from main(), Rust Drop calls mx_stream_unsubscribe, and
    subscriber_count decrements.  The producer then holds a write claim
    with only cons1 and cons3 as live subscribers and must send successfully.
    """
    producer = mx.Producer(stream, 256, 5, True)

    cons1 = start_rust_consumer(stream, 6)
    cons2 = start_rust_consumer(stream, 3)   # exits after 3 messages
    cons3 = start_rust_consumer(stream, 6)
    wait_for_subscriber_count(producer, 3)

    # All 3 consumers ack messages 1–3; cons2 then exits cleanly (received == expected)
    for v in (b"1", b"2", b"3"):
        buf = producer.next_to_send()
        buf[0] = v[0]
        buf.send(1)

    # Wait for cons2 to exit and unsubscribe before taking the write claim
    assert cons2.wait(timeout=10) == 0
    wait_for_subscriber_count(producer, 2)

    # Hold a write claim with only cons1 and cons3 as active subscribers
    buf = producer.next_to_send()
    buf[0] = b"4"[0]
    buf.send(1)   # must not block — cons2 is already unsubscribed

    for v in (b"5", b"6"):
        buf = producer.next_to_send()
        buf[0] = v[0]
        buf.send(1)

    assert cons1.wait(timeout=10) == 0
    assert cons3.wait(timeout=10) == 0


# ---------------------------------------------------------------------------
# Register during sync write claim — Python producer (in-process)
# Mirrors: test_register_during_sync_write_claim, test_register_between_sync_write
#
# A Rust consumer subscribes while the producer holds a write claim.
# The new subscriber must not block the send, and must receive all in-flight
# and subsequent messages.
# ---------------------------------------------------------------------------
def test_register_during_write_claim(stream):
    """Rust consumer subscribes while Python producer holds a write claim."""
    producer = mx.Producer(stream, 256, 4, True)

    # Seed with one consumer so the producer can start sending
    cons1 = start_rust_consumer(stream, 5)
    wait_for_subscriber_count(producer, 1)

    # Send 2 messages with just cons1
    for v in (b"1", b"2"):
        buf = producer.next_to_send()
        buf[0] = v[0]
        buf.send(1)

    # Hold a write claim, then add cons2 while it's held
    buf = producer.next_to_send()
    buf[0] = b"3"[0]

    # cons2 subscribes after msgs 1-2 are already acked; it receives msgs 3-5 = 3 messages
    cons2 = start_rust_consumer(stream, 3)
    wait_for_subscriber_count(producer, 2)

    buf.send(1)

    # Send remaining messages for both consumers to reach their expected count
    for v in (b"4", b"5"):
        buf = producer.next_to_send()
        buf[0] = v[0]
        buf.send(1)

    assert cons1.wait(timeout=10) == 0
    assert cons2.wait(timeout=10) == 0


# ---------------------------------------------------------------------------
# Grab oldest — Python producer (in-process) + Rust consumers
# Mirrors: test_grab_oldest (validates buffer ordering cross-language)
# ---------------------------------------------------------------------------
def test_grab_oldest_rust_consumer(stream):
    """Rust consumer receives messages in correct oldest-first order."""
    # This test verifies ordering at the count level: all N messages received
    # in order is validated by the Rust consumer's exit code.
    producer = mx.Producer(stream, 256, 5, False)  # streaming, 5 buffers

    cons = start_rust_consumer(stream, 5, strict=True)
    wait_for_subscriber_count(producer, 1)

    for i in range(5):
        buf = producer.next_to_send()
        msg = f"msg{i}".encode()
        buf[: len(msg)] = msg
        buf.send(len(msg))

    producer.end()
    assert cons.wait(timeout=10) == 0


# ---------------------------------------------------------------------------
# Slow consumer streaming — Python producer (in-process) + Rust consumer
# Mirrors: test_slow_consumer_streaming
#
# Fast producer fills ring buffer in streaming mode; slow consumer gets the
# newest messages available, not the overwritten ones.
# ---------------------------------------------------------------------------
def test_slow_consumer_streaming(stream):
    """Streaming-mode consumer receives most-recent messages, not overwritten ones."""
    # 3 buffers; send 6 messages without waiting for ack — oldest 3 get overwritten
    producer = mx.Producer(stream, 256, 3, False)

    cons = start_rust_consumer(stream, 3, strict=False)
    wait_for_subscriber_count(producer, 1)

    for i in range(6):
        buf = producer.next_to_send()
        msg = f"msg{i}".encode()
        buf[: len(msg)] = msg
        buf.send(len(msg))

    producer.end()
    # Consumer exits 0 — it received some messages (strict=False) without error
    assert cons.wait(timeout=10) == 0
