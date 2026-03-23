#!/usr/bin/env bash
# Cross-language IPC tests — runs all 8 producer/consumer scenarios locally.
#
# Prerequisites:
#   make install              (builds + installs Python package)
#   make rust/build/examples  (builds mx_producer and mx_consumer)
#
# Usage:
#   bash tests/test_crosslang.sh
#   make test/crosslang

set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON="$ROOT/.venv/bin/python3"
EXAMPLES="$ROOT/rust/target/release/examples"
PY_PROD="$ROOT/tests/crosslang_producer.py"
PY_CONS="$ROOT/tests/crosslang_consumer.py"
COUNT=10

# ---------------------------------------------------------------------------
# Preflight checks
# ---------------------------------------------------------------------------
missing=0
for f in \
    "$PYTHON" \
    "$EXAMPLES/mx_producer" \
    "$EXAMPLES/mx_consumer" \
    "$PY_PROD" \
    "$PY_CONS"; do
    if [ ! -f "$f" ] && [ ! -x "$f" ]; then
        echo "  [MISSING] $f"
        missing=$((missing + 1))
    fi
done
if [ "$missing" -gt 0 ]; then
    echo ""
    echo "Run:  make install && make rust/build/examples"
    exit 1
fi

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
PASS=0; FAIL=0

result() {
    local name="$1" status="$2"
    if [ "$status" -eq 0 ]; then
        printf "  %-65s PASS\n" "$name"
        PASS=$((PASS + 1))
    else
        printf "  %-65s FAIL\n" "$name"
        FAIL=$((FAIL + 1))
    fi
}

cleanup() { rm -f /dev/shm/mx.xl_* 2>/dev/null || true; }
trap cleanup EXIT
cleanup

# wait_pids <pid> [pid ...] — waits for all given PIDs; returns 1 if any failed
wait_pids() {
    local status=0
    for pid in "$@"; do
        wait "$pid" || status=1
    done
    return $status
}

RUST_PROD="$EXAMPLES/mx_producer"
RUST_CONS="$EXAMPLES/mx_consumer"

# ---------------------------------------------------------------------------
# Scenarios
# ---------------------------------------------------------------------------
echo ""
echo "Cross-language IPC tests"
echo "========================"
echo ""
echo "  Sync scenarios — all consumers must receive exactly $COUNT messages"
echo ""

# 1. Rust producer → Python consumer (sync)
{
    S=xl_r2p; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Rust producer → Python consumer" $r
}

# 2. Python producer → Rust consumer (sync)
{
    S=xl_p2r; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Python producer → Rust consumer" $r
}

# 3. Rust producer → 2 Python consumers (sync)
{
    S=xl_r2pm; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_MIN_SUBSCRIBERS=2 "$RUST_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Rust producer → 2× Python consumer" $r
}

# 4. Python producer → 2 Rust consumers (sync)
{
    S=xl_p2rm; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_MIN_SUBSCRIBERS=2 "$PYTHON" "$PY_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Python producer → 2× Rust consumer" $r
}

# 5. Rust producer → 2 Python + 2 Rust consumers (sync, mixed)
{
    S=xl_r2mx; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_MIN_SUBSCRIBERS=4 "$RUST_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Rust producer → 2× Python + 2× Rust consumer (mixed)" $r
}

# 6. Python producer → 2 Python + 2 Rust consumers (sync, mixed)
{
    S=xl_p2mx; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_MIN_SUBSCRIBERS=4 "$PYTHON" "$PY_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$PYTHON" "$PY_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT "$RUST_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Python producer → 2× Python + 2× Rust consumer (mixed)" $r
}

echo ""
echo "  Non-sync scenarios — producer never blocks; partial delivery is expected"
echo ""

# 7. Rust producer → Python consumer (non-sync)
{
    S=xl_r2p_ns; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_SYNC=0 "$RUST_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_STRICT_COUNT=0 "$PYTHON" "$PY_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Rust producer → Python consumer (non-sync)" $r
}

# 8. Python producer → Rust consumer (non-sync)
{
    S=xl_p2r_ns; cleanup; pids=()
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_SYNC=0 "$PYTHON" "$PY_PROD" &  pids+=($!)
    MX_STREAM_NAME=$S MX_MSG_COUNT=$COUNT MX_STRICT_COUNT=0 "$RUST_CONS" &  pids+=($!)
    wait_pids "${pids[@]}"; r=$?
    result "Python producer → Rust consumer (non-sync)" $r
}

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
echo ""
echo "  Results: $PASS passed, $FAIL failed"
echo ""

[ "$FAIL" -eq 0 ]
