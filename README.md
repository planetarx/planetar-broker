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
 (SHM producer: TODO)  ┘            │                         └ unix sock ─► SHM sub
                                    ▼                                    (memfd ring
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
```

Ports and endpoints (all loopback-only by design):

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

**SHM (`/tmp/planetar-broker.sock`).** Connect, send the same `SUB ...\n` line,
then receive two file descriptors via `SCM_RIGHTS`: a 16 MiB ring `memfd` and an
`eventfd`. The broker writes entries into the ring and signals the eventfd; the
consumer blocks on the eventfd and drains the ring. `shm-consumer.c` is the
canonical reader — see it for the exact ring-walking logic.

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
| `tests/test-unit.c` | ~180 checks against the broker's internals — it `#include`s `planetar-broker.c` (main renamed) so every static function is testable: CRC32 vectors, SUB-line parsing, topic matching, buffer growth, drop-oldest queues, partial-write resume, WAL roundtrip/recovery/torn-tail, SHM ring writes across the wrap sentinel, EOF-flush + burst-backlog regressions |
| `tests/test-functional.sh` | 16 scenarios against a live broker: handshake accept/reject, exact/wildcard/`**` routing, multi-sub fan-out, all four cross-transport paths, protocol-violation survival, backpressure drops, WAL recovery across restart |
| `tests/test-perf.sh` | unpaced burst throughput + paced per-message latency (p50/p90/p99) for TCP→TCP, TCP→SHM, UDP→SHM |
| `tests/pub-tcp`, `pub-udp`, `sub-tcp`, `sub-shm` | standalone protocol clients the scripts drive; also useful interactively |
| `tests/zmesg-test.h`, `tests/latency-stats.h` | envelope builder/parser (mirrors `zmesg.h`) and the qsort-percentile stats ported from zbroker0 |

**Live-host coexistence:** on a dev box the pm2 fleet (`planetar-ais`
publishing live envelopes, `planetar-ui`'s bridge subscribing `**`) reconnects
to any broker on the well-known ports — including a test broker. Every scenario
therefore publishes under a unique per-run topic prefix and the consumers count
only that prefix, so live traffic can't corrupt results. The suite aborts only
if another *broker* already holds port 12001.

Representative numbers from this suite (i9-9900K, 64 B payloads, 20 k msgs,
2026-06-11): burst ingest ≈ **1.2 M msg/s** with 100 % SHM delivery; paced
e2e latency p50 ≈ **34 µs** (TCP→TCP), **26 µs** (TCP→SHM), **17 µs**
(UDP→SHM); **broker ring hop** (accept stamp → busy-polling SHM consumer,
the `hop-*` passes in `make perf`) p50 ≈ **1.8–2.0 µs** WAL-on,
**≈ 300 ns** WAL-off (Min ≈ 140–160 ns).

> **Benchmark provenance.** The proposal's headline **p50 80–140 ns / p99
> 400–900 ns** (zbroker0, 2026-04-27) measured a *direct SHM-producer → ring →
> consumer hop* with no broker process in between. planetar-broker's closest
> in-architecture analogue is the ring-hop number above: its WAL-off p50 of
> ~300 ns is the bus core (stamp + topic match + ring write + cross-process
> visibility, including ~50–100 ns of measurement overhead from the two
> `clock_gettime` calls). The full e2e paths cross a kernel transport into
> the broker and are microsecond-class by construction. A true
> apples-to-apples with the zbroker0 figure requires the planned SHM
> *producer* path. Different measurements; don't conflate them. Likewise the
> browser/WebSocket tooling in `~/github/sness23/sales4` measures the *UI
> bridge*, not the C bus.

## See also

- `ARCH.md` — architecture deep-dive with annotated code.
- `CLAUDE.md` — orientation for AI agents working in this repo.
- `planetar-broker.c` — the whole broker; the top-of-file comment is the
  authoritative protocol spec, and every function is documented inline.
