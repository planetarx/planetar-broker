# planetar-broker — Architecture

This document is the "why and how" companion to `planetar-broker.c`. The source
file's top comment is the authoritative wire spec; here we walk the design with
annotated excerpts. Read it top to bottom and you should understand every moving
part: the reactor, the four transports, the unified sequencing, the WAL, and the
shared-memory ring.

> All code blocks below are excerpted from `planetar-broker.c` (and
> `shm-consumer.c` for the reader side). Line-for-line they may drift as the code
> evolves; the *shapes* are what matter.

---

## 1. The big picture

One process, one thread, one `epoll` loop. Producers push messages in over TCP or
UDP; the broker stamps each with an identity, writes it to disk, and fans it out
to subscribers over TCP and shared memory. Nothing blocks the loop — slow
consumers lose data rather than stall the bus.

```
                          ┌─────────────────────────────────────────────┐
   TCP pub  :12001 ──────►│                                             │
   UDP      :12003 ──────►│   epoll_wait()  (single thread)             │
   TCP sub  :12002 ──────►│      │                                      │
   unix sock ────────────►│      ▼                                      │
                          │   per-message:                              │
                          │      g_sequence++   ts = now_ns_real()      │
                          │      ├─► wal_append()      (durability)     │
                          │      ├─► fanout()          (TCP subs)       │
                          │      └─► shm_fanout()      (SHM subs)       │
                          └─────────────────────────────────────────────┘
```

The four listeners and every client live in the same epoll set. The loop tells
them apart with a tag at the front of each registered object — see §2.

---

## 2. The reactor and tagged dispatch

`epoll`'s `data.ptr` is an opaque `void*`. To recover the concrete type when an
event fires, **every object we register begins with an `ev_handle_t`** whose
`kind` field says what it really is:

```c
typedef enum {
    EV_LISTEN_PUB,    /* TCP publisher listener  (port 12001) */
    EV_LISTEN_SUB,    /* TCP subscriber listener (port 12002) */
    EV_LISTEN_UDP,    /* UDP datagram socket     (port 12003) */
    EV_LISTEN_SHM,    /* Unix-socket SHM handshake listener   */
    EV_CLIENT,        /* a client_t (TCP pub or sub)          */
    EV_SHM_CLIENT,    /* a shm_client_t (SHM subscriber)      */
} ev_kind_t;

typedef struct ev_handle {
    ev_kind_t kind;
} ev_handle_t;
```

Both client structs embed it **first**:

```c
typedef struct client {
    ev_handle_t   ev;     /* MUST be first — epoll data.ptr looks at this */
    int           fd;
    ...
} client_t;

typedef struct shm_client {
    ev_handle_t  ev;        /* MUST be first; kind = EV_SHM_CLIENT */
    int          sock_fd;
    ...
} shm_client_t;
```

Listeners use small `static` tags. The dispatch core is just a switch on the tag:

```c
for (int i = 0; i < n; i++) {
    ev_handle_t *h = events[i].data.ptr;
    if (h->kind == EV_LISTEN_PUB) { accept_loop(pub_fd, CK_PUB); continue; }
    if (h->kind == EV_LISTEN_SUB) { accept_loop(sub_fd, CK_SUB); continue; }
    if (h->kind == EV_LISTEN_UDP) { udp_drain(udp_fd);          continue; }
    if (h->kind == EV_LISTEN_SHM) { accept_shm(shm_fd);         continue; }
    if (h->kind == EV_SHM_CLIENT) {
        shm_client_t *sc = (shm_client_t *)h;
        ...
        continue;
    }
    client_t *c = (client_t *)h;            /* EV_CLIENT */
    int rc = 0;
    if (events[i].events & (EPOLLERR | EPOLLHUP)) rc = -1;
    if (rc == 0 && (events[i].events & EPOLLIN))  rc = client_readable(c);
    if (rc == 0 && (events[i].events & EPOLLOUT)) rc = client_writable(c);
    if (rc < 0) client_close(c);
}
```

**Why single-threaded?** No locks, no atomics on the shared broker state, no
contention — the entire data path is cache-friendly straight-line code. The one
place we *do* need atomics is the SHM ring, because the *reader* runs in another
process (§7). The 1-second `epoll_wait` timeout exists only so a `SIGINT` that
arrives between events is still noticed promptly:

```c
while (!g_stop) {
    int n = epoll_wait(g_epoll_fd, events, EPOLL_BATCH, 1000);
    ...
}
```

---

## 3. One identity per message

The single most important invariant in the broker: when a message is accepted, it
gets **one** sequence number and **one** timestamp, and those exact values are
handed to all three sinks. This is what makes the WAL, a TCP subscriber, and a SHM
subscriber agree on "which message is which."

From the TCP publish path (`handle_pub_data`):

```c
/* Assign one sequence + timestamp shared by WAL and fan-out
 * so observers across transports see the same identifiers. */
g_sequence++;
uint64_t seq = g_sequence;
uint64_t ts  = now_ns_real();
wal_append(&g_wal, c->rbuf + 4, flen, ORIGIN_TCP, seq, ts);   /* 1. durable  */
shm_fanout(c->rbuf + 4, flen, topic, topic_len, ORIGIN_TCP, seq, ts);
                                                              /* 2. SHM ring */
fanout(c->rbuf, 4 + flen, topic, topic_len);                  /* 3. TCP subs */
```

The **sink order is deliberate** and an invariant: WAL first (durability
before delivery), the SHM ring second (the latency-critical tier), TCP last.
TCP fan-out does a malloc + memcpy per subscriber; keeping that out of the
SHM hop measurably matters — with a `**` TCP subscriber attached, moving SHM
ahead of TCP dropped the ring-hop p50 from ~1.2 µs to ~0.3 µs (2026-06-11,
`make perf`'s `hop-wal-off` pass).

The UDP path (`udp_drain`) does the identical dance with `ORIGIN_UDP`. Because the
broker is single-threaded, `g_sequence++` needs no synchronization — there is
exactly one writer.

`g_sequence` is *recovered* across restarts from the WAL (§6), so ids stay
monotonic even after a crash.

---

## 4. The four transports

### 4.1 TCP publish (`:12001`)

A byte stream of `[u32 BE length][envelope]` frames. The reader pump
(`client_readable`) fills a growable buffer, then `handle_pub_data` peels off
whole frames in a loop:

```c
for (;;) {
    if (c->rlen < 4) return 0;                 /* need the length prefix   */
    uint32_t flen = ntohl(*(uint32_t*)c->rbuf);
    if (flen == 0 || flen > FRAME_MAX_LEN) return -1;   /* bogus -> close  */
    if (c->rlen < 4 + flen) {                  /* frame not all here yet   */
        if (rbuf_grow(c, 4 + flen) < 0) return -1;
        return 0;
    }
    /* ... validate zmesg magic, read topic, sequence+stamp, route ... */
    memmove(c->rbuf, c->rbuf + 4 + flen, c->rlen - (4 + flen));  /* consume */
    c->rlen -= 4 + flen;
}
```

The buffer can only grow to `FRAME_MAX_LEN + 4`, so a hostile publisher can't make
us allocate without bound (`rbuf_grow`).

### 4.2 TCP subscribe (`:12002`)

A subscriber's first line selects topics; everything after is one-way fan-out to
it. The handshake is line-oriented:

```
SUB chat.pac.* ais.victoria.**\n   ->  OK\n
SUB **\n                           ->  OK\n      (match everything)
<malformed>                        ->  ERR bad SUB line\n   (then close)
```

`parse_sub_line` tokenizes on spaces; `handle_sub_handshake` replies and, on
success, flips the client to `SS_RUNNING` and registers it for fan-out.

### 4.3 UDP (`:12003`)

One datagram is exactly one envelope — the datagram boundary *is* the frame. To
deliver to TCP subscribers (who expect length-prefixed framing) we re-frame into a
staging buffer:

```c
/* Re-frame for TCP fan-out: 4-byte BE length + datagram bytes. */
uint32_t net_len = htonl((uint32_t)n);
memcpy(tcp_frame, &net_len, 4);
memcpy(tcp_frame + 4, recv_buf, (size_t)n);
fanout(tcp_frame, 4 + (size_t)n, topic, topic_len);
shm_fanout(recv_buf, (size_t)n, topic, topic_len, ORIGIN_UDP, seq, ts);
```

SHM subscribers get the raw envelope (the ring carries its own framing, §7).

### 4.4 SHM (`/tmp/planetar-broker.sock`)

Covered in depth in §7. The Unix socket is only used to *bootstrap* the shared
memory; after that, delivery is lock-free shared-memory writes plus an eventfd
kick.

---

## 5. Topic matching

Three rules, checked in `topic_matches` (TCP) and its twin `shm_topic_matches`
(SHM). The topic is read straight out of the zmesg envelope header — the broker
never looks at the payload.

```c
static int topic_matches(const client_t *c, const char *topic, size_t topic_len) {
    if (c->match_all) return 1;                       /* subscribed to "**" */
    for (size_t i = 0; i < c->n_topics; i++) {
        const char *t = c->topics[i];
        size_t tl = strlen(t);
        if (tl == topic_len && memcmp(t, topic, topic_len) == 0)
            return 1;                                 /* exact match        */
        if (tl >= 2 && t[tl - 1] == '*' && t[tl - 2] == '.' &&
            topic_len >= tl - 1 && memcmp(t, topic, tl - 1) == 0)
            return 1;                                 /* "prefix.*" wildcard */
    }
    return 0;
}
```

So `chat.pac.*` matches `chat.pac.anything`, `foo` matches only `foo`, and `**`
matches everything. There is no general glob — this keeps matching to a couple of
`memcmp`s per subscriber on the hot path.

> **Maintenance note:** the TCP and SHM versions are byte-identical logic on two
> different struct types. Change one, change the other.

---

## 6. The write-ahead log

Every accepted envelope is appended to disk *before* fan-out, so the log is the
ground truth of what the bus carried.

### 6.1 On-disk format

A segment file is a 64-byte header followed by a run of variable-length entries:

```
segment-000000001234.wal
┌────────────────────────────┐
│ wal_seg_header_t (64 bytes) │  magic "WAL1", base_sequence, clean flag, count
├────────────────────────────┤
│ wal_entry_hdr_t (32 bytes)  │  magic 0xB10C, origin, data_len, sequence,
│ envelope bytes (data_len)   │  timestamp_ns, crc32, total_len
│ zero padding -> 8-byte align │
├────────────────────────────┤
│ wal_entry_hdr_t ...          │
│ ...                          │
└────────────────────────────┘
```

```c
typedef struct {
    uint16_t magic;          /*  0: 0xB10C                  */
    uint16_t origin;         /*  2: msg_origin_t            */
    uint32_t data_len;       /*  4: zmesg envelope length   */
    uint64_t sequence;       /*  8: global monotonic ID     */
    uint64_t timestamp_ns;   /* 16: CLOCK_REALTIME          */
    uint32_t crc32;          /* 24: CRC32 over data         */
    uint32_t total_len;      /* 28: aligned entry size      */
    /* data[] follows */
} __attribute__((packed)) wal_entry_hdr_t;
```

The structs are `packed` so the byte offsets are exact and stable — external
replay tooling can read entries by offset.

### 6.2 The append hot path

One `writev` puts header + envelope + pad on disk in a single syscall, so a record
is always contiguous:

```c
struct iovec iov[3];
iov[0].iov_base = &hdr;  iov[0].iov_len = sizeof hdr;     /* 32-byte header  */
iov[1].iov_base = data;  iov[1].iov_len = len;            /* envelope        */
iov[2].iov_base = pad;   iov[2].iov_len = padded - len;   /* 0..7 zero bytes */
int niov = iov[2].iov_len ? 3 : 2;
ssize_t got = writev(w->fd, iov, niov);
```

The CRC is computed over exactly the envelope bytes. **Availability beats
durability**: any short or failed write flips `w->enabled = 0` and the broker
keeps serving traffic without the WAL rather than dying.

### 6.3 Rotation

When the active segment would exceed 64 MiB, it's finalized and a fresh one opens,
based at "where this segment's sequence ids left off" so ids stay dense across the
boundary:

```c
if (w->bytes_written + total > WAL_SEG_MAX_BYTES) {
    uint64_t next_base = w->base_sequence + w->entry_count;
    wal_finalize_segment(w);                 /* rewrite header: clean + count */
    if (wal_open_new_segment(w, next_base) < 0) { w->enabled = 0; return -1; }
}
```

### 6.4 Recovery

On startup `wal_init` picks the highest-numbered segment (filenames are
zero-padded so lexical == numeric order) and `wal_scan_segment` *walks the entries
themselves* — it does not trust the header's count, so an unclean shutdown
recovers correctly:

```c
for (;;) {
    wal_entry_hdr_t eh;
    ssize_t r = read(fd, &eh, sizeof eh);
    if (r == 0) break;                                 /* clean EOF          */
    if (r != (ssize_t)sizeof eh || eh.magic != WAL_ENTRY_MAGIC) break; /* torn */
    size_t padded = (eh.data_len + (WAL_ALIGN-1)) & ~((size_t)WAL_ALIGN-1);
    if (lseek(fd, (off_t)padded, SEEK_CUR) < 0) break;
    last_seq = eh.sequence;
    count++;
}
*out_next_seq = last_seq + (count ? 1 : 0);
```

The walk stops at the first bad magic — exactly where a crash would have left a
half-written record. If the scan can't even parse the header, recovery jumps the
sequence far forward (`base + 1000000`) rather than risk *reusing* an id a torn
segment might already hold. Gaps are harmless; reuse is not.

Then it adopts the recovered id and opens a brand-new segment (writes never append
to a historical file):

```c
g_sequence = next_seq;
w->enabled = 1;
if (wal_open_new_segment(w, g_sequence) < 0) { w->enabled = 0; return -1; }
```

---

## 7. The shared-memory ring (the fast path)

This is where the nanoseconds live. Instead of copying bytes through a socket, the
broker and consumer share a memory region; the broker writes entries and bumps an
eventfd, and the consumer reads straight out of mapped memory.

### 7.1 Bootstrap over a Unix socket

A SHM consumer connects to the Unix socket and sends the same `SUB ...\n` line. The
broker then (`shm_complete_handshake`):

1. `memfd_create` a 16 MiB anonymous file, `ftruncate` it to size.
2. `mmap` it **read-write** (the broker's write window).
3. Create a non-blocking `eventfd`.
4. Send **both fds** to the consumer with `SCM_RIGHTS`.

The fd-passing requires one byte of normal payload to ride along with the control
message:

```c
static int send_fds_with_byte(int sock_fd, int fds[2]) {
    char dummy = '\0';
    struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };
    char cbuf[CMSG_SPACE(sizeof(int) * 2)];
    struct msghdr msg = {0};
    msg.msg_iov = &iov; msg.msg_iovlen = 1;
    msg.msg_control = cbuf; msg.msg_controllen = sizeof cbuf;

    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    cm->cmsg_level = SOL_SOCKET;
    cm->cmsg_type  = SCM_RIGHTS;
    cm->cmsg_len   = CMSG_LEN(sizeof(int) * 2);
    memcpy(CMSG_DATA(cm), fds, sizeof(int) * 2);   /* the memfd + eventfd */

    return sendmsg(sock_fd, &msg, 0) == 1 ? 0 : -1;
}
```

The consumer's matching `recv_two_fds` (in `shm-consumer.c`) pulls the two fds out
of the ancillary data and `mmap`s the same memfd **read-only**.

### 7.2 Ring layout

```
memfd (16 MiB)
┌──────────────────────────────┐  offset 0
│ ring_header_t (64 bytes)     │  volatile uint64_t write_idx  (+ padding)
├──────────────────────────────┤  offset 64 = RING_HEADER_SIZE
│                              │
│  RING_DATA_SIZE bytes of      │  entries: [hdr][envelope][pad-to-8] ...
│  variable-length entries      │
│                              │
└──────────────────────────────┘
```

```c
typedef struct {
    volatile uint64_t write_idx;   /* ABSOLUTE byte offset, never wraps */
    uint64_t reserved[7];          /* pad to a full 64-byte cache line  */
} ring_header_t;

typedef struct {
    uint32_t length;        /* envelope length; 0 = "skip to ring start" */
    uint32_t origin;
    uint64_t sequence;
    uint64_t timestamp_ns;
    /* envelope bytes follow, padded to 8 */
} __attribute__((packed)) ring_entry_hdr_t;
```

`write_idx` is **absolute and monotonic** — it never wraps. The physical slot is
`write_idx % RING_DATA_SIZE`. Using an absolute counter means the reader knows
exactly how far behind it is (just subtract its own cursor); there's no ambiguity
about whether the ring has lapped.

### 7.3 The publish memory barrier

The entire cross-process correctness argument is the ordering of two operations in
`ring_write_one`: copy the bytes, **then** release-store `write_idx`.

```c
/* 1. copy entry header + envelope into the ring (plain stores) */
memcpy(data + pos, &eh, sizeof eh);
memcpy(data + pos + sizeof eh, envelope, env_len);

/* 2. publish: a RELEASE store so a reader that sees the new write_idx
 *    with an ACQUIRE load is guaranteed to also see the bytes above. */
__atomic_store_n(&hdr->write_idx, cur + entry_total, __ATOMIC_RELEASE);

/* 3. wake a blocked reader; eventfd is a counter, EAGAIN just means
 *    "already signalled, reader will catch up on its own". */
uint64_t one = 1;
write(c->eventfd_fd, &one, sizeof one);
```

The reader pairs it with an acquire load (`shm-consumer.c`):

```c
uint64_t write_idx = __atomic_load_n(&hdr->write_idx, __ATOMIC_ACQUIRE);
if (my_read >= write_idx) break;            /* nothing new */
```

Release/acquire is exactly the guarantee needed: *if* the reader observes the
advanced `write_idx`, it also observes every entry byte written before the release.
The broker's own load of `write_idx` can be relaxed — it's the sole writer, so it
always holds the freshest value.

### 7.4 End-of-ring wrap

When an entry won't fit before the end of the data region, the writer drops a
zero-length sentinel header and advances the absolute cursor past the gap:

```c
if (pos + entry_total > RING_DATA_SIZE) {
    ring_entry_hdr_t sentinel = {0};        /* length == 0 */
    memcpy(data + pos, &sentinel, sizeof sentinel);
    size_t skipped = RING_DATA_SIZE - pos;  /* accounted in the absolute idx */
    cur += skipped;
    pos = 0;
}
```

The reader sees `length == 0` and performs the mirror skip — note it advances its
cursor by `RING_DATA_SIZE - pos`, the same quantity the writer added:

```c
if (eh.length == 0) {                       /* wrap sentinel */
    my_read += (RING_DATA_SIZE - pos);
    continue;
}
```

### 7.5 "Newest wins"

There is no flow control on the ring. A reader too slow to keep up will simply have
its unread bytes overwritten by newer entries. For a real-time sensor bus that's
the desired behavior: a lagging UI should jump to the present, not replay a backlog.
(Oversized envelopes that can't fit in the ring at all are counted as `dropped` and
skipped.)

---

## 8. Backpressure, end to end

| Sink            | Policy                          | Mechanism                       |
|-----------------|---------------------------------|---------------------------------|
| TCP subscriber  | drop **oldest** queued frames   | `outq_push` evicts from head    |
| SHM subscriber  | overwrite **oldest** ring bytes | reader's unread data is clobbered |
| WAL             | disable on write error          | `w->enabled = 0`, keep serving  |

The TCP queue cap is `OUTQ_MAX_BYTES` (1 MiB) per subscriber. `outq_push`:

```c
/* drop-oldest backpressure */
while (c->outq_bytes + total > OUTQ_MAX_BYTES && c->outq_head) {
    outq_entry_t *e = c->outq_head;
    c->outq_head = e->next;
    c->outq_bytes -= (e->total - e->sent);
    c->dropped++;
    free(e);
}
```

Across all three sinks the rule is the same: **the broker never blocks, never
grows without bound, and a slow consumer only hurts itself.**

---

## 9. Lifecycle summary

```
startup ──► wal_init() recovers g_sequence
        ──► listen_tcp ×2, bind_udp, bind_unix
        ──► epoll_create1, register 4 listeners
        ──► LOG "ready"

per event ─► accept_loop / udp_drain / accept_shm   (new work in)
         ─► client_readable ─► handle_pub_data ─► seq+ts ─► wal/fanout/shm
         ─► client_writable ─► try_flush                  (drain TCP queues)
         ─► shm handshake   ─► shm_complete_handshake     (ring + fds)

SIGINT ──► g_stop = 1
shutdown ─► wal_close (finalize segment)
         ─► close every client + listener, unlink the unix socket
```

---

## 10. Tunables

All in the `tunables` block near the top of `planetar-broker.c`:

| Macro                | Default      | Meaning                                  |
|----------------------|--------------|------------------------------------------|
| `PUB/SUB/UDP_PORT`   | 12001/2/3    | TCP/UDP ports (loopback only)            |
| `SHM_PATH`           | `/tmp/...sock` | Unix socket for the SHM handshake      |
| `RING_SIZE`          | 16 MiB       | per-subscriber shared ring               |
| `FRAME_MAX_LEN`      | 1 MiB        | largest single envelope                  |
| `OUTQ_MAX_BYTES`     | 1 MiB        | per-TCP-subscriber queue cap             |
| `MAX_SUBS`           | 256          | TCP subscriber slots                     |
| `MAX_SHM_SUBS`       | 128          | SHM subscriber slots                     |
| `WAL_SEG_MAX_BYTES`  | 64 MiB       | segment size before rotation             |
| `WAL_DIR_DEFAULT`    | `./wal`      | overridable via the `WAL_DIR` env var    |

---

## 11. Cross-file contracts

A few invariants span multiple files; break one and things misparse silently:

- **Ring layout** — `ring_header_t` (64 B) and `ring_entry_hdr_t` (24 B) are
  `_Static_assert`ed identically in `planetar-broker.c` and `shm-consumer.c`. The
  release/acquire pairing on `write_idx` is the writer/reader contract.
- **zmesg offsets** — `ZMESG_FIXED_HDR`, `ZMESG_OFF_TOPIC_LEN`, and the magic are
  hardcoded copies of the external `zmesg` header. If zmesg's layout changes,
  update them here too (there is no shared header).
- **Topic matching** — `topic_matches` and `shm_topic_matches` must stay identical.

See `CLAUDE.md` for the short version of these for AI agents.
