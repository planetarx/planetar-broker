#!/usr/bin/env bash
#
# test-functional.sh — functional + integration tests for
# planetar-broker, run against a real broker process.
#
# Modeled on zbroker0's test-unified.sh, adapted to this broker's
# wire protocol (zmesg envelopes, SUB handshake, ports 12001-12003,
# /tmp/planetar-broker.sock).
#
# LIVE-HOST SAFETY: on a dev box the pm2 fleet (planetar-ais
# publishing live envelopes, planetar-ui's bridge subscribing "**")
# will happily reconnect to OUR test broker the moment it binds the
# well-known ports. The suite is built to coexist with that traffic:
#   - every scenario uses topics under a unique per-run prefix, and
#   - the consumers count ONLY topics under that prefix (their
#     <filter> argument), draining foreign envelopes silently.
# A preflight pass detects foreign traffic and says so, so surprising
# numbers in the logs are explainable.
#
# Scenarios:
#   F1   SUB handshake accepts a good line ("OK")
#   F2   SUB handshake rejects a bad line ("ERR")
#   F3   TCP pub -> TCP sub, exact topic
#   F4   wildcard "<pfx>.pac.*" receives matches, nothing else
#   F5   "**" receives everything (counted under our prefix)
#   F6   two TCP subs both receive the full stream (fan-out)
#   I1   UDP pub -> TCP sub (cross-transport re-framing)
#   I2   TCP pub -> SHM sub
#   I3   UDP pub -> SHM sub
#   I4   TCP + SHM subscribers see the same message count
#   I7   SHM producer -> SHM sub (all-shared-memory path)
#   I8   SHM producer -> TCP sub (ingest crosses to kernel transport)
#   I9   broker logs clean producer accounting at disconnect
#   I5   oversized frame -> publisher dropped, broker survives
#   I6   slow consumer -> drop-oldest backpressure engages
#   W1   WAL: clean restart recovers the sequence (no id reuse)
#
# Each scenario prints PASS/FAIL; exit code 0 iff all pass.

set -u
cd "$(dirname "$0")/.."          # repo root

BROKER=./planetar-broker
PUB_TCP=tests/pub-tcp
PUB_UDP=tests/pub-udp
PUB_SHM=tests/pub-shm
SUB_TCP=tests/sub-tcp
SUB_SHM=tests/sub-shm

RESULTS=$(mktemp -d /tmp/planetar-ftest-XXXXXX)
WAL_A=$(mktemp -d /tmp/planetar-ftest-wal-XXXXXX)
PASS=0
FAIL=0
BROKER_PID=""

# Unique per-run topic namespace. Everything we publish lives under
# "$PFX." and consumers filter on it, so live bus traffic from other
# services can never pollute a count.
PFX="t$$r$RANDOM"

# ---------------- plumbing ----------------

say()  { printf '%s\n' "$*"; }
pass() { PASS=$((PASS+1)); say "  PASS: $1"; }
fail() { FAIL=$((FAIL+1)); say "  FAIL: $1"; }

check() {  # check <description> <command...>
    local desc=$1; shift
    if "$@" >/dev/null 2>&1; then pass "$desc"; else fail "$desc"; fi
}

cleanup() {
    [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null
    wait 2>/dev/null
}
trap cleanup EXIT

port_open() {  # true if something is listening on 127.0.0.1:$1
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && { exec 3>&-; return 0; }
    return 1
}

start_broker() {  # start_broker <wal_dir> <log_file>
    WAL_DIR="$1" $BROKER >"$2" 2>&1 &
    BROKER_PID=$!
    for _ in $(seq 1 50); do
        port_open 12001 && return 0
        sleep 0.1
    done
    say "broker failed to start; log:"; cat "$2"
    exit 1
}

stop_broker() {
    [ -n "$BROKER_PID" ] || return 0
    kill -TERM "$BROKER_PID" 2>/dev/null
    wait "$BROKER_PID" 2>/dev/null
    BROKER_PID=""
}

# ---------------- preflight ----------------

say "planetar-broker functional tests"
say "results dir: $RESULTS    topic prefix: $PFX"

for bin in $BROKER $PUB_TCP $PUB_UDP $PUB_SHM $SUB_TCP $SUB_SHM; do
    [ -x "$bin" ] || { say "missing $bin — run 'make test' to build"; exit 1; }
done

if port_open 12001; then
    say "ABORT: something is already listening on 12001."
    say "Stop any running planetar-broker before testing."
    exit 1
fi

start_broker "$WAL_A" "$RESULTS/broker-a.log"
say "broker up (pid $BROKER_PID, WAL $WAL_A)"

# Detect foreign traffic: watch the whole bus briefly without
# publishing anything ourselves. Purely informational — the prefix
# filtering keeps scenarios correct either way.
timeout 10 $SUB_SHM 0 2 - '**' >"$RESULTS/preflight.out" 2>&1
foreign=$(grep -o 'count=[0-9]*' "$RESULTS/preflight.out" \
          | head -1 | cut -d= -f2)
if [ "${foreign:-0}" -gt 0 ]; then
    say "NOTE: $foreign foreign message(s) seen in 2s — live services"
    say "      (pm2 planetar-ais/ui?) are using this bus. Scenarios"
    say "      are isolated by topic prefix; counts remain exact."
fi

# ---------------- F1/F2: handshake ----------------
say "-- handshake"

timeout 10 $SUB_TCP 0 1 0 - "$PFX.hs" >"$RESULTS/f1.out" 2>&1
check "F1 good SUB line accepted" test $? -eq 0

reply=$(exec 3<>/dev/tcp/127.0.0.1/12002 \
        && printf 'BOGUS nope\n' >&3 \
        && head -c 3 <&3; exec 3>&- 2>/dev/null)
check "F2 bad SUB line rejected" test "$reply" = "ERR"

# ---------------- F3-F6: TCP routing ----------------
say "-- TCP topic routing"

# F3: exact topic.
timeout 20 $SUB_TCP 100 5 0 "$PFX." "$PFX.exact" >"$RESULTS/f3.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 100 "$PFX.exact" >>"$RESULTS/f3.out" 2>&1
wait $S; check "F3 exact topic, 100/100 delivered" test $? -eq 0

# F4: wildcard gets <pfx>.pac.* only — 60 matching + 40 others
# published; the consumer must end at exactly 60 (its filter is the
# whole prefix, so a wrong-topic leak of our own traffic would
# overshoot and fail it).
timeout 20 $SUB_TCP 60 5 0 "$PFX." "$PFX.pac.*" >"$RESULTS/f4.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 60 "$PFX.pac.alpha" >>"$RESULTS/f4.out" 2>&1
$PUB_TCP 40 "$PFX.other"     >>"$RESULTS/f4.out" 2>&1
wait $S; check "F4 wildcard isolation, exactly 60/60" test $? -eq 0

# F5: "**" sees both streams (counted under our prefix only).
timeout 20 $SUB_TCP 70 5 0 "$PFX." '**' >"$RESULTS/f5.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 30 "$PFX.all.a" >>"$RESULTS/f5.out" 2>&1
$PUB_TCP 40 "$PFX.all.b" >>"$RESULTS/f5.out" 2>&1
wait $S; check "F5 match-all sees both topics, 70/70" test $? -eq 0

# F6: two subscribers, one stream, both complete.
timeout 20 $SUB_TCP 50 5 0 "$PFX." "$PFX.fan" >"$RESULTS/f6a.out" 2>&1 &
SA=$!
timeout 20 $SUB_TCP 50 5 0 "$PFX." "$PFX.fan" >"$RESULTS/f6b.out" 2>&1 &
SB=$!; sleep 0.3
$PUB_TCP 50 "$PFX.fan" >>"$RESULTS/f6.out" 2>&1
wait $SA; RA=$?
wait $SB; RB=$?
check "F6 fan-out to two TCP subs" test $RA -eq 0 -a $RB -eq 0

# ---------------- I1-I4: cross-transport ----------------
say "-- cross-transport integration"

# I1: UDP in, TCP out. Paced sends — loopback UDP can drop on bursts.
timeout 20 $SUB_TCP 50 5 0 "$PFX." "$PFX.udp" >"$RESULTS/i1.out" 2>&1 &
S=$!; sleep 0.3
$PUB_UDP 50 "$PFX.udp" 64 200 >>"$RESULTS/i1.out" 2>&1
wait $S; check "I1 UDP -> TCP, 50/50" test $? -eq 0

# I2: TCP in, SHM out.
timeout 20 $SUB_SHM 100 5 "$PFX." "$PFX.shm" >"$RESULTS/i2.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 100 "$PFX.shm" >>"$RESULTS/i2.out" 2>&1
wait $S; check "I2 TCP -> SHM, 100/100" test $? -eq 0

# I3: UDP in, SHM out.
timeout 20 $SUB_SHM 50 5 "$PFX." "$PFX.us" >"$RESULTS/i3.out" 2>&1 &
S=$!; sleep 0.3
$PUB_UDP 50 "$PFX.us" 64 200 >>"$RESULTS/i3.out" 2>&1
wait $S; check "I3 UDP -> SHM, 50/50" test $? -eq 0

# I4: one stream, TCP + SHM subscribers in parallel, equal counts.
timeout 20 $SUB_TCP 60 5 0 "$PFX." "$PFX.both" >"$RESULTS/i4a.out" 2>&1 &
SA=$!
timeout 20 $SUB_SHM 60 5 "$PFX." "$PFX.both" >"$RESULTS/i4b.out" 2>&1 &
SB=$!; sleep 0.3
$PUB_TCP 60 "$PFX.both" >>"$RESULTS/i4.out" 2>&1
wait $SA; RA=$?
wait $SB; RB=$?
check "I4 TCP and SHM both get 60/60" test $RA -eq 0 -a $RB -eq 0

# ---------------- I7-I9: SHM producer ingest (phase 5) -----------
say "-- SHM producer ingest"

# I7: SHM in -> SHM out (the all-shared-memory path).
timeout 20 $SUB_SHM 100 5 "$PFX." "$PFX.sps" >"$RESULTS/i7.out" 2>&1 &
S=$!; sleep 0.3
$PUB_SHM 100 "$PFX.sps" 64 50 >>"$RESULTS/i7.out" 2>&1
wait $S; check "I7 SHM -> SHM, 100/100" test $? -eq 0

# I8: SHM in -> TCP out (ingest crosses to a kernel-transport sub).
timeout 20 $SUB_TCP 100 5 0 "$PFX." "$PFX.spt" >"$RESULTS/i8.out" 2>&1 &
S=$!; sleep 0.3
$PUB_SHM 100 "$PFX.spt" 64 50 >>"$RESULTS/i8.out" 2>&1
wait $S; check "I8 SHM -> TCP, 100/100" test $? -eq 0

# I9: the broker logs clean producer accounting at disconnect
# (msgs delivered, zero bad envelopes, zero flow-control resyncs).
grep -Eq "disconnect SHM PUB .*msgs=100 bad=0 resyncs=0" \
     "$RESULTS/broker-a.log"
check "I9 producer disconnect clean (msgs=100 bad=0 resyncs=0)" \
      test $? -eq 0

# ---------------- I5: protocol violation ----------------
say "-- protocol violations"

# A length prefix over FRAME_MAX_LEN must get the publisher dropped
# without harming the broker. Send 0xFFFFFFFF as the frame length.
(exec 3<>/dev/tcp/127.0.0.1/12001 \
    && printf '\xff\xff\xff\xff' >&3 \
    && sleep 0.3; exec 3>&-) 2>/dev/null
sleep 0.2
grep -q 'bogus length' "$RESULTS/broker-a.log"
check "I5a broker logged the bogus length" test $? -eq 0
# Broker must still be serving: a quick pub/sub roundtrip.
timeout 10 $SUB_TCP 5 3 0 "$PFX." "$PFX.alive" >"$RESULTS/i5.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 5 "$PFX.alive" >>"$RESULTS/i5.out" 2>&1
wait $S; check "I5b broker alive after violation" test $? -eq 0

# ---------------- I6: backpressure ----------------
say "-- backpressure (drop-oldest)"

# A consumer sleeping 2ms per frame against a full-speed 3000-frame
# burst (~12 MB at 4 KiB payloads) must overflow its 1 MiB outq and
# shed frames. The sub prints its local port, so we can find ITS
# disconnect line (and dropped count) in the broker log precisely.
timeout 40 $SUB_TCP 0 2 2000 "$PFX." "$PFX.slow" >"$RESULTS/i6.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 3000 "$PFX.slow" 4096 >>"$RESULTS/i6.out" 2>&1
wait $S
sleep 0.5
slowport=$(grep -o 'LOCAL port=[0-9]*' "$RESULTS/i6.out" | cut -d= -f2)
grep -Eq "disconnect SUB 127.0.0.1:${slowport:-NONE} \(dropped=[1-9]" \
     "$RESULTS/broker-a.log"
check "I6 drop-oldest engaged (dropped>0 for the slow sub)" test $? -eq 0

# ---------------- W1: WAL recovery ----------------
say "-- WAL recovery across restart"

stop_broker
WAL_B=$(mktemp -d /tmp/planetar-ftest-wal-XXXXXX)

# Phase 1: publish 25 messages, clean shutdown. (Foreign traffic may
# add more WAL entries; assertions below use >= and strict ordering,
# not exact counts.)
start_broker "$WAL_B" "$RESULTS/broker-w1.log"
$PUB_TCP 25 "$PFX.wal" >"$RESULTS/w1.out" 2>&1
sleep 0.5
stop_broker

# Phase 2: restart on the same WAL dir. Recovery must see at least
# our 25 entries, and new messages must get sequence ids strictly
# above everything phase 1 logged (no id reuse).
phase1_seq=$(grep -o 'shutting down.*seq=[0-9]*' "$RESULTS/broker-w1.log" \
             | grep -o 'seq=[0-9]*$' | cut -d= -f2)
start_broker "$WAL_B" "$RESULTS/broker-w2.log"
entries=$(grep -o 'WAL recovered: .*entries=[0-9]*' "$RESULTS/broker-w2.log" \
          | grep -o 'entries=[0-9]*' | cut -d= -f2)
check "W1a recovery saw >=25 entries (${entries:-0})" \
      test "${entries:-0}" -ge 25

timeout 20 $SUB_SHM 10 5 "$PFX." '**' >"$RESULTS/w1b.out" 2>&1 &
S=$!; sleep 0.3
$PUB_TCP 10 "$PFX.wal" >>"$RESULTS/w1b.out" 2>&1
wait $S
first=$(grep -o 'first_seq=[0-9]*' "$RESULTS/w1b.out" | cut -d= -f2)
check "W1b post-restart ids (${first:-?}) > phase-1 max (${phase1_seq:-?})" \
      test "${first:-0}" -gt "${phase1_seq:-999999999}"
gaps=$(grep -o 'gaps=[0-9]*' "$RESULTS/w1b.out" | cut -d= -f2)
check "W1c no sequence gaps in the new stream" test "${gaps:-1}" -eq 0

stop_broker

# ---------------- summary ----------------
say ""
say "functional: $PASS passed, $FAIL failed   (logs in $RESULTS)"
[ "$FAIL" -eq 0 ] && rm -rf "$WAL_A" "$WAL_B"
exit $((FAIL > 0))
