# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

`planetar-broker` is a single-file C message bus (`planetar-broker.c`, ~3.3 k
lines as of 2026-10-13, a large share doc comments) plus a ~220-LOC reference SHM consumer
(`shm-consumer.c`). It is the "Bus" component of the planetar / IDEaS CH13
proposal spine. Proposal claims that cite LOC or benchmarks should be verified
against this repo.

On the deploy box (bb) the broker runs under pm2 as `planetar:broker`
(WAL in this repo's `./wal`) — never run a bare `./planetar-broker` to test:
it ignores argv and binds the live ports; use `PORT_BASE`/`SHM_PATH`/`WAL_DIR`
overrides (see `~/data/vaults/docs/DEPLOY-planetar.md`).

It carries zmesg envelopes over four transports simultaneously (TCP pub :12001,
TCP sub :12002, UDP :12003, SHM via `/tmp/planetar-broker.sock`) and durably
logs every message to a WAL before fan-out.

## Docs map

- `README.md` — overview, build/run, wire protocols, smoke test, perf-test
  pointers. Start here.
- `ARCH.md` — architecture deep-dive with annotated code excerpts (reactor,
  transports, WAL format/recovery, SHM ring + memory barriers, backpressure).
- `planetar-broker.c` top-of-file comment — the authoritative wire spec.
  Every function and struct has a doc-comment block; keep it that way (see
  Conventions).

Don't duplicate ARCH.md content here; update ARCH.md when architecture changes.

## Build / run / verify

```bash
make              # builds planetar-broker + shm-consumer (-O2)
make debug        # clean rebuild with -O0 -g3 + ASan/UBSan
make run          # ./planetar-broker (foreground; Ctrl-C for clean shutdown)
WAL_DIR=/tmp/wal ./planetar-broker   # override WAL location (default ./wal)
```

### Tests

```bash
make test          # unit (tests/test-unit) + functional suite
make test-unit     # ~180 unit checks, no broker process needed
make test-functional   # 16 scenarios against a live broker
make perf          # latency/throughput bench (N=1000000 for big runs)
./tests/test-functional.sh   # runnable directly; logs land in /tmp
```

Suite layout: `tests/test-unit.c` `#include`s `planetar-broker.c` with main
renamed, so every static function is directly testable — keep new internals
covered there. `tests/test-functional.sh` drives real broker processes via
the protocol clients (`pub-tcp`, `pub-udp`, `sub-tcp`, `sub-shm`).
`tests/zmesg-test.h` mirrors the zmesg envelope layout (third copy — update
with the other two).

**Live-host coexistence:** the pm2 services (planetar-ais publisher,
planetar-ui `**` subscriber) reconnect to ANY broker on ports 12001–12003,
including test brokers. Scenarios isolate themselves with unique per-run
topic prefixes + consumer-side prefix filters — preserve that pattern when
adding scenarios, and never assert exact counts on unfiltered `**` traffic.

Perf methodology: throughput is an unpaced burst; latency is paced (5 µs)
because unpaced "latency" is just queueing delay. The proposal's
`p50 80–140 ns` (zbroker0, 2026-04-27) measured a direct SHM ring hop —
this suite's e2e-through-broker numbers are microsecond-class and must not
be conflated with it. sales4's latency tooling measures the browser/WS UI
bridge, not this bus.

## Architecture invariants (the load-bearing ones)

Full detail in `ARCH.md` §2–§8; the short list a future instance must not break:

- **Tagged epoll dispatch.** `client_t` and `shm_client_t` both begin with an
  `ev_handle_t ev` read through epoll's `data.ptr`. That member **must stay
  first** in both structs.
- **One identity per message.** `g_sequence++` and `now_ns_real()` are sampled
  once per accepted message, and the same `(seq, ts)` goes to `wal_append`,
  `fanout` (TCP), and `shm_fanout` (SHM). Never re-sample per sink. When HLC
  stamping is on (`RING_ID` set), the single `hlc_tick()`/`hlc_stamp_frame()`
  call is also part of that one-identity step — stamp once, before the sinks,
  and route the stamped bytes (or the originals when it returns 0) to all three.
- **Sink order is WAL → SHM → TCP** in both ingest paths (`handle_pub_data`,
  `udp_drain`). Durability first, then the latency-critical ring, then TCP
  queues — per-TCP-subscriber mallocs must stay out of the SHM hop (measured
  2026-06-11: ring-hop p50 0.3 µs vs 1.2 µs with a `**` TCP sub attached).
- **Hardcoded zmesg offsets.** `ZMESG_FIXED_HDR=66`, `ZMESG_OFF_TOPIC_LEN=48`,
  magic `0x5A4D5347` are copies of `~/github/sness23/zmesg` — duplicated in
  both `.c` files plus `tests/zmesg-test.h`, no shared header. Change in
  lockstep. **Two envelope versions now coexist on the wire:** v1 (fixed
  header 66) and v2 (fixed header 98 — a 32-byte HLC sequencing extension at
  offset 66, strings shifted to 98; see SPEC-hlc-envelope.md in
  planetarx/planetar-disruptor). Every parse site branches on the version
  byte (offset 4) via `zmesg_fixed_hdr()` to locate the topic; an unknown
  version is dropped, not misparsed. `tests/zmesg-test.h` deliberately builds
  v1 (producers stay v1; the broker upgrades to v2 at ingress).
- **HLC stamping is opt-in and identity-recovering.** Off unless `RING_ID` is
  set, so the live single-ring broker is byte-identical to before. When on,
  the broker is the single writer of its ring and owns the v2 tuple
  `(hlc_physical_ns, hlc_logical, ring_id, local_seq, err_bound_ns)`;
  `wal_scan_segment` recovers `(last_physical, last_logical)` from the last v2
  record so post-restart stamps never regress (SPEC §7 identity rule).
  `PORT_BASE`/`SHM_PATH` env overrides let multiple rings run on one box.
- **Duplicated twins.** `topic_matches`/`shm_topic_matches` and
  `parse_sub_line`/`parse_shm_sub_line` are identical logic on two struct
  types. Edit both or neither.
- **SHM ring contract.** Absolute never-wrapping `write_idx`
  (pos = `% RING_DATA_SIZE`), release-store after byte copy paired with the
  consumer's acquire-load, length-0 wrap sentinel, 64-byte/24-byte
  `_Static_assert`ed headers. The reader half lives in `shm-consumer.c` —
  layout changes must be mirrored there.
- **Never block the reactor.** TCP subs get drop-oldest queues (1 MiB cap),
  SHM readers get overwritten ("newest wins"), WAL write errors disable the
  WAL (`w->enabled = 0`) instead of crashing.
- **WAL recovery trusts entries, not headers.** `wal_scan_segment` walks
  records to the first bad magic; a failed scan jumps the sequence forward by
  1,000,000 — gaps are fine, id reuse is not. Sequence gaps after a crash are
  expected, not a bug.

## Conventions

- C11, `-D_GNU_SOURCE`, warnings-clean under `-Wall -Wextra -Wpedantic` and
  under `make debug` (ASan + UBSan).
- **Comment density is a feature.** The maintainer wants liberal documentation:
  every function and struct gets a doc-comment block (what, why, parameters,
  return/error behavior), plus inline comments on non-obvious logic. Keep
  comment prose ≤80 chars/line. Match the existing style.
- All wire structs are `__attribute__((packed))`; the `_Static_assert`s on ring
  header sizes are part of the cross-process contract — fix the struct, never
  the assert.
- Loopback-only by design: TCP binds `INADDR_LOOPBACK`, the Unix socket is
  `chmod 0600`. Don't widen the bind surface without a reason.
- `wal/` is gitignored runtime state; don't commit segments.
