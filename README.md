# planetar-broker

A single-file C message bus that carries [zmesg](https://github.com/sness23/zmesg)
envelopes over **four transports at once** — TCP publish, TCP subscribe, UDP, and
shared memory — while durably appending every message to a write-ahead log.

It is the "bus" layer of the planetar situational-awareness stack: nanosecond-class
fan-out of sensor envelopes (AIS, SAR, EO, hydrophone, RF) to many consumers, with
a Slack-style shell on the far end. The whole broker is ~1.4 k lines of C11 in one
file (`planetar-broker.c`) plus a ~220-line reference consumer (`shm-consumer.c`).

```
 producers                      planetar-broker                       consumers
 ─────────                      ───────────────                       ─────────
 TCP  ─frames─► :12001 ┐                                      ┌ :12002 ─frames─► TCP sub
 UDP  ─dgram──► :12003 ┼──►  single-threaded epoll reactor ──►┤
 SHM  ─ring───► unix  ─┘            │                         └ unix sock ─► SHM sub
       sock (PUB)                   ▼                                    (memfd ring
                                  WAL (./wal/, CRC32)                     + eventfd)
```

Every message that enters on any transport is assigned **one** monotonic sequence
id and **one** timestamp, which then travel with it to the WAL and to every
consumer on every transport. An observer anywhere on the bus sees identical
identifiers for the same message.

## Build

```bash
make            # builds ./planetar-broker and ./shm-consumer (-O2)
make debug      # clean rebuild with -O0 -g3 + AddressSanitizer + UBSan
make run        # build then run in the foreground (Ctrl-C = clean shutdown)
make clean
```

Requirements: a Linux toolchain with `cc`, C11, and `_GNU_SOURCE` features
(`epoll`, `eventfd`, `memfd_create`, `accept4`, `SCM_RIGHTS`). No external
libraries.

## Run

```bash
./planetar-broker                 # WAL in ./wal/
WAL_DIR=/var/lib/planetar ./planetar-broker   # WAL elsewhere

# A second, isolated ring on one box (distributed-disruptor Phase 0):
PORT_BASE=12101 SHM_PATH=/tmp/pb-b.sock WAL_DIR=/tmp/wal-b \
  RING_ID=ring-b ./planetar-broker
```

Environment:

| Var | Default | Effect |
|-----|---------|--------|
| `WAL_DIR` | `./wal` | WAL segment directory |
| `PORT_BASE` | `12001` | TCP pub = base, TCP sub = base+1, UDP = base+2 |
| `SHM_PATH` | `/tmp/planetar-broker.sock` | Unix handshake socket |
| `RING_ID` | *(unset)* | Enables HLC v2 stamping; a decimal id or any string (FNV-1a hashed). Unset = pass envelopes through unstamped, identical to before. See `planetarx/planetar-disruptor/SPEC-hlc-envelope.md`. |
| `BROKER_SPIN` | *(unset)* | Busy-poll inbound SHM rings (benchmark mode) |

Ports and endpoints below assume the default `PORT_BASE` (all loopback-only by design):

| Endpoint                      | Role                                   |
|-------------------------------|----------------------------------------|
| `127.0.0.1:12001` (TCP)       | Publishers stream length-prefixed frames |
| `127.0.0.1:12002` (TCP)       | Subscribers send a `SUB` line, then receive frames |
| `127.0.0.1:12003` (UDP)       | Publishers send one envelope per datagram |
| `/tmp/planetar-broker.sock`   | SHM subscribers handshake, get a ring + eventfd |

### Quick smoke test

In one terminal:

```bash
make run
```

In another, attach the shared-memory reference consumer (subscribe to everything):

```bash
./shm-consumer '**'
```

It connects to the Unix socket, receives the ring `memfd` + `eventfd` over
`SCM_RIGHTS`, maps the ring read-only, and prints each envelope as it arrives.
Publish some traffic with a TCP or UDP producer (see *Wire protocols* below) and
watch it appear.

## Wire protocols

**TCP publish (`:12001`).** A stream of frames, each:

```
[ u32 length, big-endian ][ length bytes: zmesg envelope ]
```

**TCP subscribe (`:12002`).** Send one line, then read frames:

```
SUB <topic> [<topic> ...]\n      ->  "OK\n"   then length-prefixed frames
                                 ->  "ERR <reason>\n"  then close
```

Topics match three ways: exact string, a single trailing `.*` prefix wildcard
(`chat.pac.*`), or the special `**` which matches every message.

**UDP (`:12003`).** One datagram = one zmesg envelope, no length prefix (the
datagram boundary frames it). Re-framed to length-prefixed form for TCP
subscribers; delivered raw to SHM subscribers.

**SHM subscribe (`/tmp/planetar-broker.sock`).** Connect, send the same
`SUB ...\n` line, then receive two file descriptors via `SCM_RIGHTS`: a 16 MiB
ring `memfd` and an `eventfd`. The broker writes entries into the ring and
signals the eventfd; the consumer blocks on the eventfd and drains the ring.
`shm-consumer.c` is the canonical reader — see it for the exact ring-walking
logic.

**SHM publish (`/tmp/planetar-broker.sock`).** Connect and send `PUB\n` instead;
the broker hands back an *inbound* ring `memfd` + `eventfd` the same way, but now
the **producer** writes entries and bumps the eventfd, and the broker drains and
routes them (origin `ORIGIN_SHM`). Inbound rings have flow control — the broker
publishes its read cursor in the ring header so a producer never overwrites
unread bytes (ingest must not lose data). This is the no-kernel-transport
ingest path; `tests/pub-shm.c` is the canonical writer. Run the broker with
`BROKER_SPIN=1` to busy-poll producer rings for lowest ingest latency.

The envelope itself is the external **zmesg** format; the broker reads only its
fixed header (magic, topic offset/length) for routing and never parses payloads.

## Backpressure

The broker never blocks the reactor on a slow consumer:

- **TCP subscribers** have a per-client output queue capped at 1 MiB. When it
  overflows, the *oldest* queued frames are dropped (and counted) so the newest
  data keeps flowing — the right trade for a real-time sensor bus.
- **SHM subscribers** read a fixed-size ring; a reader that falls too far behind
  has its unread bytes overwritten by newer entries ("newest wins").

## Durability (WAL)

Before fan-out, every valid envelope is appended to a 64 MiB segment file under
`WAL_DIR` (default `./wal/`), CRC32-checksummed (IEEE polynomial). On startup the
broker scans the latest segment, walks its entries to recover the next sequence
number, and opens a fresh segment. Segment files (`segment-<base>.wal`) are
gitignored. See `ARCH.md` for the on-disk format.

## Testing

The repo carries a full suite under `tests/`, modeled on the zbroker0 harness
(the predecessor whose tooling produced the proposal's original benchmark
figures):

```bash
make test          # unit tests + functional/integration suite
make test-unit     # unit tests only (fast, no broker process)
make perf          # latency + throughput benchmark (100k msgs/path)
make perf N=1000000   # proposal-grade run
```

| Piece | What it covers |
|---|---|
| `tests/test-unit.c` | ~198 checks against the broker's internals — it `#include`s `planetar-broker.c` (main renamed) so every static function is testable: CRC32 vectors, SUB-line parsing, topic matching, buffer growth, drop-oldest queues, partial-write resume, WAL roundtrip/recovery/torn-tail, SHM ring writes across the wrap sentinel, **SHM producer drain (skip/resync/flow-control)**, EOF-flush + burst-backlog regressions |
| `tests/test-functional.sh` | 19 scenarios against a live broker: handshake accept/reject, exact/wildcard/`**` routing, multi-sub fan-out, all four cross-transport paths **including SHM-producer → SHM/TCP**, protocol-violation survival, backpressure drops, WAL recovery across restart |
| `tests/test-perf.sh` | burst throughput + paced latency (p50/p90/p99) for every path, plus the **`ring-hop` direct primitive bench** and **SHM-producer e2e** (spin / evented) |
| `tests/pub-tcp`, `pub-udp`, `pub-shm`, `sub-tcp`, `sub-shm` | standalone protocol clients the scripts drive; also useful interactively |
| `tests/ring-hop` | standalone two-process SHM ring benchmark (no broker) — the ns-class primitive measurement |
| `tests/zmesg-test.h`, `tests/latency-stats.h` | envelope builder/parser (mirrors `zmesg.h`) and the qsort-percentile stats ported from zbroker0 |

**Live-host coexistence:** on a dev box the pm2 fleet (`planetar-ais`
publishing live envelopes, `planetar-ui`'s bridge subscribing `**`) reconnects
to any broker on the well-known ports — including a test broker. Every scenario
therefore publishes under a unique per-run topic prefix and the consumers count
only that prefix, so live traffic can't corrupt results. The suite aborts only
if another *broker* already holds port 12001.

Representative numbers (i9-9900K, 64 B payloads):

- **Ring primitive — the nanosecond target.** `tests/ring-hop` (1 M msgs,
  pinned cores, the zbroker0 single-hop topology on *this* codebase's ring
  contract, no broker in the path): Min ≈ **47 ns**, **p50 ≈ 95 ns**, p90 ≈
  130 ns. Inside the proposal's **80–140 ns** band.
- **Through the broker, all shared memory** (SHM producer → broker → SHM
  consumer, `make perf` `shm-shm-*`): e2e p50 ≈ **0.6 µs** spin + WAL-off,
  ≈ **7 µs** evented + WAL-on (the realistic out-of-box default).
- **Kernel-transport ingest** (paced, `make perf`): e2e p50 ≈ **34 µs**
  (TCP→TCP), **26 µs** (TCP→SHM), **17 µs** (UDP→SHM); burst throughput
  ≈ **1.2 M msg/s** with 100 % SHM delivery.

> **Benchmark provenance.** The proposal's headline **p50 80–140 ns / p99
> 400–900 ns** (zbroker0, 2026-04-27) measured a *direct SHM-producer → ring →
> consumer hop* with no broker process between. With the SHM producer path
> (phase 5, 2026-06-14) that topology is now reproducible on planetar-broker's
> own ring code via `tests/ring-hop` — the **p50 ≈ 95 ns** above is the
> apples-to-apples figure and it lands in the target band. The
> through-broker rows answer a different question (what a real client sees
> across the bus: a second ring hop, routing, optional WAL/eventfd) and are
> sub-µs to µs by construction — don't conflate the two. Likewise the
> browser/WebSocket tooling in `~/github/sness23/sales4` measures the *UI
> bridge*, not the C bus.

## See also

- `ARCH.md` — architecture deep-dive with annotated code.
- `CLAUDE.md` — orientation for AI agents working in this repo.
- `planetar-broker.c` — the whole broker; the top-of-file comment is the
  authoritative protocol spec, and every function is documented inline.
