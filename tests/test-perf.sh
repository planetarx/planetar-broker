#!/usr/bin/env bash
#
# test-perf.sh — end-to-end performance benchmark for planetar-broker.
#
# Measures producer->broker->consumer latency (Min/Avg/p50/p90/p99/
# Max) and producer throughput for three paths:
#
#   TCP -> TCP      pub-tcp -> broker -> sub-tcp
#   TCP -> SHM      pub-tcp -> broker -> sub-shm
#   UDP -> SHM      pub-udp -> broker -> sub-shm
#
# Methodology (same as the zbroker0 harness that produced the
# proposal's numbers): the producer embeds a CLOCK_MONOTONIC ns
# timestamp in each payload; the consumer subtracts it on receipt.
# CLOCK_MONOTONIC is system-wide on Linux, so producer and consumer
# clocks are directly comparable on one host. Percentiles come from
# a qsort over raw samples — no streaming approximation.
#
# IMPORTANT — how these numbers relate to the proposal's headline
# "SHM p50 80-140 ns": that figure (zbroker0, 2026-04-27) measured a
# direct SHM-producer -> SHM-consumer ring hop. planetar-broker has
# no SHM *producer* path yet, so every measurement here crosses a
# kernel transport into the broker first (TCP or UDP ingest + WAL +
# ring write + eventfd wake). Expect microseconds, not nanoseconds —
# they are different measurements and must not be conflated.
#
# Usage:
#     ./tests/test-perf.sh [count] [payload_bytes]
#
#   count          messages per path (default 100000; use 1000000
#                  for proposal-grade runs)
#   payload_bytes  payload size (default 64)

set -u
cd "$(dirname "$0")/.."

BROKER=./planetar-broker
PUB_TCP=tests/pub-tcp
PUB_UDP=tests/pub-udp
PUB_SHM=tests/pub-shm
SUB_TCP=tests/sub-tcp
SUB_SHM=tests/sub-shm
RING_HOP=tests/ring-hop

N=${1:-100000}
PAYLOAD=${2:-64}
RESULTS=$(mktemp -d /tmp/planetar-perf-XXXXXX)
WAL=$(mktemp -d /tmp/planetar-perf-wal-XXXXXX)
BROKER_PID=""

cleanup() {
    [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

port_open() {
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&-; return 0; }
    return 1
}

for bin in $BROKER $PUB_TCP $PUB_UDP $PUB_SHM $SUB_TCP $SUB_SHM $RING_HOP; do
    [ -x "$bin" ] || { echo "missing $bin — run 'make test' first"; exit 1; }
done
if port_open 12001; then
    echo "ABORT: something is already listening on 12001."
    exit 1
fi

# ---------------- context header ----------------
echo "planetar-broker perf — $(date -Iseconds)"
echo "host:   $(uname -srm)"
echo "cpu:    $(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | sed 's/^ //')"
echo "msgs:   $N x ${PAYLOAD}B payload per path"
echo "wal:    $WAL (on, default config)"
echo "build:  $(cc --version | head -1)"
echo

WAL_DIR="$WAL" $BROKER >"$RESULTS/broker.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 50); do port_open 12001 && break; sleep 0.1; done

run_path() {  # run_path <label> <sub_cmd...> -- <pub_cmd...>
    local label=$1; shift
    local sub=() pub=()
    while [ "$1" != "--" ]; do sub+=("$1"); shift; done
    shift
    pub=("$@")

    echo "=== $label ==="
    timeout 120 "${sub[@]}" >"$RESULTS/$label.sub" 2>&1 &
    local S=$!
    sleep 0.5
    timeout 120 "${pub[@]}" >"$RESULTS/$label.pub" 2>&1
    wait $S

    grep '^SENT'   "$RESULTS/$label.pub"
    grep '^RESULT' "$RESULTS/$label.sub"
    sed -n '/^Latency/,/Max:/p' "$RESULTS/$label.sub"
    echo
}

# Unique per-run topic prefix: consumers filter on it, so live bus
# traffic (pm2 planetar-ais etc.) can't pollute counts or latency.
PFX="p$$r$RANDOM"

# --- Throughput: unpaced burst. The producer reports its raw send
# rate; the SHM consumer reports how much survived end-to-end. A TCP
# consumer would shed here by design (drop-oldest), so SHM is the
# honest delivery check for an all-out burst.
echo "--- THROUGHPUT (unpaced burst) ---"
run_path burst \
    $SUB_SHM "$N" 10 "$PFX." "$PFX.burst" -- \
    $PUB_TCP "$N" "$PFX.burst" "$PAYLOAD"

# --- Latency: paced below saturation (5 us between sends, ~200 k
# msg/s offered). Unpaced "latency" is really queueing delay — the
# whole burst sits in buffers ahead of each sample. Pacing keeps
# queues empty so the numbers measure the per-message path itself.
echo "--- LATENCY (paced, 5 us between sends) ---"

# TCP -> TCP
run_path tcp-tcp \
    $SUB_TCP "$N" 10 0 "$PFX." "$PFX.tcp" -- \
    $PUB_TCP "$N" "$PFX.tcp" "$PAYLOAD" 5

# TCP -> SHM
run_path tcp-shm \
    $SUB_SHM "$N" 10 "$PFX." "$PFX.shm" -- \
    $PUB_TCP "$N" "$PFX.shm" "$PAYLOAD" 5

# UDP -> SHM (also paced to avoid loopback datagram loss; delivered
# count is reported next to sent, so any loss is visible, not hidden).
run_path udp-shm \
    $SUB_SHM 0 5 "$PFX." "$PFX.udp" -- \
    $PUB_UDP "$N" "$PFX.udp" "$PAYLOAD" 5

# --- Ring hop: broker accept stamp -> SHM consumer dequeue, with a
# busy-polling consumer (-b). THIS is the bus-core number to track
# against the proposal's ns-class target: it excludes the kernel
# ingest hop but includes WAL append, topic match, the ring write,
# and cross-process visibility. (The "e2e" block in the same output
# still shows the full producer->consumer path for context.)
echo "--- RING HOP (broker stamp -> busy-poll SHM consumer) ---"
run_path hop-wal-on \
    $SUB_SHM -b "$N" 10 "$PFX." "$PFX.hop1" -- \
    $PUB_TCP "$N" "$PFX.hop1" "$PAYLOAD" 5

# Same with the WAL disabled (an invalid WAL_DIR makes the broker
# run durability-free) — isolates the WAL writev's share of the hop.
kill "$BROKER_PID" 2>/dev/null; wait "$BROKER_PID" 2>/dev/null
WAL_DIR=/dev/null/x $BROKER >"$RESULTS/broker-nowal.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 50); do port_open 12001 && break; sleep 0.1; done

run_path hop-wal-off \
    $SUB_SHM -b "$N" 10 "$PFX." "$PFX.hop0" -- \
    $PUB_TCP "$N" "$PFX.hop0" "$PAYLOAD" 5

# --- SHM ingest end-to-end (phase 5): SHM producer -> broker -> SHM
# consumer, NO kernel transport on either side. This is the full path
# the proposal's flagship demo would use. Two configs:
#   shm-shm-evented   default broker (WAL on, eventfd-driven): the
#                     realistic out-of-the-box latency.
#   shm-shm-spin      BROKER_SPIN=1 + WAL off + CPU pinning: the
#                     best-case bus-core latency the architecture can
#                     deliver through the broker today.
# CPU pinning matters enormously for the spin path (a cross-core
# cache-line handoff dominates), so pin broker/consumer/producer to
# distinct cores when taskset is available.
PIN_BRK=""; PIN_SUB=""; PIN_PUB=""
if command -v taskset >/dev/null 2>&1; then
    PIN_BRK="taskset -c 2"; PIN_SUB="taskset -c 4"; PIN_PUB="taskset -c 6"
    echo "(CPU pinning: broker cpu2, consumer cpu4, producer cpu6)"
fi

echo "--- SHM INGEST e2e (SHM producer -> broker -> SHM consumer) ---"

# shm-shm-evented: restart the broker at defaults (WAL on, evented).
WAL_SHM=$(mktemp -d /tmp/planetar-perf-wal-XXXXXX)
kill "$BROKER_PID" 2>/dev/null; wait "$BROKER_PID" 2>/dev/null
WAL_DIR="$WAL_SHM" $BROKER >"$RESULTS/broker-shm-ev.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 50); do port_open 12001 && break; sleep 0.1; done
run_path shm-shm-evented \
    $SUB_SHM "$N" 10 "$PFX." "$PFX.ssev" -- \
    $PUB_SHM "$N" "$PFX.ssev" "$PAYLOAD" 5

# shm-shm-spin: restart with spin + WAL off, pinned, busy-poll sub.
kill "$BROKER_PID" 2>/dev/null; wait "$BROKER_PID" 2>/dev/null
WAL_DIR=/dev/null/x BROKER_SPIN=1 $PIN_BRK $BROKER \
    >"$RESULTS/broker-shm-spin.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 50); do port_open 12001 && break; sleep 0.1; done
echo "=== shm-shm-spin ==="
timeout 120 $PIN_SUB $SUB_SHM -b "$N" 10 "$PFX." "$PFX.sssp" \
    >"$RESULTS/shm-shm-spin.sub" 2>&1 &
S=$!
sleep 0.5
timeout 120 $PIN_PUB $PUB_SHM "$N" "$PFX.sssp" "$PAYLOAD" 5 \
    >"$RESULTS/shm-shm-spin.pub" 2>&1
wait $S
grep '^SENT'   "$RESULTS/shm-shm-spin.pub"
grep '^RESULT' "$RESULTS/shm-shm-spin.sub"
sed -n '/^Latency (e2e/,/Max:/p' "$RESULTS/shm-shm-spin.sub"
echo

# --- Direct ring hop (no broker): the zbroker0-topology measurement
# on THIS codebase's ring algorithm. One producer process, one
# busy-polling consumer process, distinct pinned cores, ns reported.
# This is the apples-to-apples number against the proposal's headline.
kill "$BROKER_PID" 2>/dev/null; wait "$BROKER_PID" 2>/dev/null
echo "--- RING HOP direct (no broker, zbroker0 topology) ---"
$RING_HOP "$N" "$PAYLOAD" 2 4 1

echo
echo "raw output in $RESULTS"
echo
echo "READING THE NUMBERS (fastest layer first):"
echo "  ring-hop direct   bus primitive, no broker (matches the"
echo "                    proposal's zbroker0 topology): ns class."
echo "  shm-shm-spin      through the broker, all shared memory,"
echo "                    spin + WAL off: sub-microsecond e2e."
echo "  shm-shm-evented   through the broker at defaults (WAL on,"
echo "                    eventfd): the realistic out-of-box path."
echo "  hop-* / e2e *     broker-internal ring hop reached via a"
echo "                    kernel transport (TCP/UDP ingest): us class."
echo
echo "The proposal's p50 80-140 ns (zbroker0, 2026-04-27) is the"
echo "ring-hop-direct row's analogue. The broker-in-path rows answer"
echo "a different question (what a real client sees through the bus)."
