#!/bin/bash
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

PIDS=""

cleanup() {
    for pid in $PIDS; do
        kill "$pid" 2>/dev/null || true
    done
    for pid in $PIDS; do
        wait "$pid" 2>/dev/null || true
    done
    rm -f /tmp/rtmm.in.sock /tmp/rtmm.out.sock
    echo ""
    echo "All processes stopped."
}

trap cleanup EXIT INT TERM

rm -f /tmp/rtmm.in.sock /tmp/rtmm.out.sock

echo "==> Starting validator..."
cargo run --release --bin validator &
PIDS="$PIDS $!"
sleep 1

echo "==> Starting matchmaker..."
cargo run --release --bin matchmaker &
PIDS="$PIDS $!"
sleep 1

echo "==> Starting generator..."
cargo run --release --bin generator &
PIDS="$PIDS $!"
sleep 1

echo "==> All processes running. Press Ctrl+C to stop."
echo "    PIDs: $PIDS"
echo ""

for pid in $PIDS; do
    wait "$pid" || true
done
