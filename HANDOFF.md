# Session handoff — 2026-06-17

Wrap-up notes for continuing planetar-broker work in another session.
This session's through-line: **document the broker, build a full test
suite, and add the SHM producer path so the bus can hit its
nanosecond latency target.** All of that is done and green. A parallel
HLC-v2 effort has been editing the same file; the two have merged
cleanly.

---

## TL;DR status

| Item | State |
|---|---|
| SHM producer path (phase 5) | **Done, committed, tested** |
| Nanosecond target | **Hit** — `ring-hop` p50 ≈ 95 ns (target 80–140 ns) |
| Unit tests | 198 pass (`make test-unit`) |
| Functional/integration | 19 scenarios pass (`make test-functional`) |
| Build | Clean (`-Wall -Wextra -Wpedantic`; ASan clean in isolation) |
| Docs | README/ARCH/CLAUDE updated; doc edits **uncommitted** (see below) |
| Concurrent HLC-v2 effort | Merged into the same file; coexists; my tests pass against it |

---

## What this session did (in order)

1. **`/init` → wrote `CLAUDE.md`.** Discovered this repo IS the real
   broker (the parent `~/github/planetarx/CLAUDE.md` calls it an "empty
   placeholder" — that's stale). Documented the architecture and
   invariants.

2. **Located the original perf tests.** They are in
   `~/github/sness23/zbroker0` (producer-/consumer-{tcp,udp,ultra}.c +
   `test-unified.sh`), **not** `sales4` — sales4 holds the
   browser/WebSocket UI-bridge latency tooling, a different layer.
   zbroker0 produced the proposal's `p50 80–140 ns` figure.

3. **Heavily documented `planetar-broker.c`.** Doc-comment block on
   every function/struct, a top-of-file reading guide, annotated
   includes/tunables, inline commentary on the subtle parts (ring
   release/acquire barrier, WAL recovery, sink ordering). Also wrote
   `README.md` and `ARCH.md`.

4. **Built the test suite (`tests/`).** Unit (`test-unit.c`, which
   `#include`s the broker with `main` renamed so every static function
   is testable), functional (`test-functional.sh`), perf
   (`test-perf.sh`), protocol clients, and shared headers
   (`zmesg-test.h`, `latency-stats.h`). Writing the suite surfaced and
   fixed two real broker bugs: `client_readable` discarded buffered
   frames on publisher EOF, and killed publishers on >1 MiB
   small-frame bursts.

5. **Built the SHM producer path (phase 5) — the main work.**
   - `PUB\n` handshake → broker hands back an **inbound** ring +
     eventfd (the subscriber handshake reversed).
   - `shm_producer_drain` — third ingest pump, sibling to
     `handle_pub_data`/`udp_drain`; standard WAL→SHM→TCP routing,
     origin `ORIGIN_SHM`.
   - **Flow control** (the key difference from newest-wins outbound
     rings): broker publishes its read cursor in the ring header's
     `reserved[0]`; the producer blocks rather than overwrite unread
     bytes, because ingest loss would bypass the WAL.
   - **Deferred close** — a producer owns two epoll fds (handshake
     socket + ring eventfd), so an inline close mid-batch is a
     use-after-free; the reactor queues producer closes to end-of-batch
     and drains the tail first. **Validated clean under ASan.**
   - `BROKER_SPIN=1` busy-poll mode for benchmark-grade ingest.
   - New tools: `tests/pub-shm.c`, `tests/ring-hop.c` (standalone
     two-process ring bench reproducing the zbroker0 topology on THIS
     codebase's ring contract, no broker in the path).
   - New tests: producer-drain unit coverage (skip/resync/flow-control),
     functional scenarios I7–I9 (SHM→SHM, SHM→TCP, clean accounting),
     perf passes (shm-shm spin/evented, ring-hop direct).

6. **Fixed a documented shutdown bug** — the subscriber cleanup loop
   iterated forward over a swap-compacted array (closed only ~half);
   now iterates backward. (A residual: TCP *publishers* aren't tracked
   in any registry, so they still leak on shutdown — harmless, OS
   reclaims, only visible with foreign clients connected. See Open
   items.)

---

## Measurements (i9-9900K, 64 B payloads)

**The headline — ring primitive, the nanosecond target:**
`tests/ring-hop`, 1 M msgs, pinned cores, zbroker0 single-hop topology
on this codebase's ring, no broker in the path:

| Min | **p50** | p90 | p99 |
|---|---|---|---|
| ~47 ns | **~95 ns** | ~130 ns | ~1 µs |

→ inside the proposal's **80–140 ns** band, now on our own ring code.

**Through the broker (the real client path), `make perf`:**

| Path | e2e p50 |
|---|---|
| SHM→broker→SHM, spin + WAL-off, pinned | ~0.6 µs |
| SHM→broker→SHM, evented + WAL-on (default) | ~7 µs |
| TCP→TCP / TCP→SHM / UDP→SHM (paced) | ~34 / ~26 / ~17 µs |
| burst throughput | ~1.2 M msg/s, 100 % SHM delivery |

**Do not conflate** the 95 ns ring primitive with the µs through-broker
numbers — they answer different questions. The 95 ns is the
apples-to-apples vs zbroker0; the µs figures are what a real client
sees across the bus.

---

## Repo state (commit/uncommit boundary)

- **Committed** (`bef30dd` + `6f18db4`): `planetar-broker.c` (my SHM
  phase-5 work AND the HLC-v2 work, merged), all `tests/*.c` and
  `tests/*.h`.
- **Uncommitted (this session's tail — doc + test-script polish):**
  `ARCH.md`, `README.md`, `tests/test-functional.sh`,
  `tests/test-perf.sh`. These are mine and ready; nobody has asked to
  commit them yet.
- **Build artifacts (not source):** the `planetar-broker` and
  `tests/test-hlc-integ` binaries show as modified/untracked — noise,
  ignore (note: the binary is tracked from the initial commit, which is
  itself a bit odd; consider gitignoring binaries).

---

## The concurrent HLC-v2 effort (read before editing the file)

A parallel session/effort is building **HLC (hybrid logical clock) v2
envelope stamping / "distributed-disruptor"** in the SAME
`planetar-broker.c`, plus README/CLAUDE.md/Makefile. Its commit:
`6f18db4 "Add HLC v2 stamping at the ingest path (opt-in via RING_ID)"`.

What it added (all opt-in, off unless `RING_ID` is set):
- A second envelope version on the wire: v1 (fixed header 66) and v2
  (fixed header 98 — a 32-byte HLC extension at offset 66). Parse sites
  branch on the version byte via `zmesg_fixed_hdr(ver)`.
- `RING_ID` / `PORT_BASE` / `SHM_PATH` env overrides (multiple rings on
  one box).
- `tests/test-hlc-integ.c` (+ `make test-hlc`), and a spec at
  `planetarx/planetar-disruptor/SPEC-hlc-envelope.md`.
- It also extended **my** `shm_producer_drain` to be v2-aware
  (`zmesg_fixed_hdr(ver)`, `ZMESG_OFF_VERSION`) — so the SHM ingest path
  already understands v2 envelopes.

**Coexistence is clean:** my 198 unit tests pass against the merged
file. But there are two writers — **re-read the file before editing**,
and prefer targeted edits (they fail safe rather than clobber).

The next natural feature (SHM-as-relay / the disruptor fan-out pattern)
belongs to THAT effort's lane, not the SHM-producer work — coordinate
before building into it.

---

## Open items / loose ends

1. **Commit the doc/test-script polish** (ARCH.md, README.md,
   test-functional.sh, test-perf.sh) when ready. (Left uncommitted —
   the maintainer drives commits.)
2. **TCP-publisher shutdown leak** — publishers aren't tracked in any
   registry, so they leak on broker shutdown. Harmless (OS reclaims,
   shutdown-only, only appears with foreign clients still connected at
   kill time), and the test suite is ASan-clean in isolation. Fixing it
   properly means a lightweight all-TCP-clients registry touching
   `accept_loop`/`client_close`/`main` — deferred because that's hot
   code the HLC effort is also in. Documented in `main()`'s shutdown
   comment.
3. **Proposal-grade perf run** — the README cites a 20 k validation run
   for e2e and a 1 M run for ring-hop. A quiet-host 1 M run of every
   path would firm up the numbers for the proposal.
4. **Live-host gotcha** — the pm2 fleet (`planetar-ais` publisher,
   `planetar-ui` bridge subscribing `**`) reconnects to ANY broker on
   ports 12001–12003, including test brokers. The suite isolates via
   per-run topic prefixes + consumer-side filters; preserve that when
   adding scenarios, and never assert exact counts on unfiltered `**`.

---

## How to validate / continue

```bash
make test           # 198 unit + 19 functional, all green
make test-hlc       # the concurrent effort's HLC integration test
make perf N=1000000 # proposal-grade latency/throughput
./tests/ring-hop 1000000 64 2 4 1   # the ns-class primitive bench
```

Memory updated this session:
`broker-test-provenance.md` (zbroker0 vs sales4 provenance, the SHM
producer milestone + 95 ns result, the concurrent-effort note).
