/*
 * test-unit.c — unit tests for planetar-broker.c internals.
 *
 * The broker is a single file of static functions, so the standard
 * single-file-C testing trick applies: rename its main() via the
 * preprocessor and #include the whole source. Every static function
 * and type becomes directly callable from here, no headers needed.
 *
 * What is covered (pure logic + kernel-object logic that doesn't
 * need the live reactor):
 *
 *   1. CRC32 known-answer vectors
 *   2. wal_segment_path formatting
 *   3. parse_sub_line (valid/invalid/wildcard/limits)
 *   4. topic_matches (exact / "prefix.*" / "**" / negatives)
 *   5. rbuf_grow growth + protocol cap
 *   6. outq_push drop-oldest backpressure accounting
 *   7. try_flush partial-write resume over a socketpair
 *   8. WAL append -> scan -> recover roundtrip, torn-tail stop,
 *      and the +1,000,000 anti-collision jump on a bad header
 *   9. SHM ring write + wrap-sentinel walk (a simulated live reader)
 *
 * The fan-out/epoll paths are covered by tests/test-functional.sh
 * against a real broker process.
 *
 * Run via `make test-unit` (or as part of `make test`). Exit code 0
 * iff every check passed. Broker LOG lines appearing in the output
 * are expected — the WAL tests exercise the real logging paths.
 */

#define main broker_main          /* don't let the broker's main win */
#include "../planetar-broker.c"
#undef main

#include <assert.h>
#include <sys/wait.h>

#include "zmesg-test.h"

/* ---------------- tiny check harness ---------------- */

static int t_pass = 0;
static int t_fail = 0;

#define CHECK(cond) do {                                              \
    if (cond) { t_pass++; }                                           \
    else {                                                            \
        t_fail++;                                                     \
        fprintf(stderr, "    FAIL %s:%d  %s\n",                       \
                __FILE__, __LINE__, #cond);                           \
    }                                                                 \
} while (0)

#define SECTION(name) printf("== %s\n", name)

/* ---------------- helpers ---------------- */

/* Free the topic strings a parse_sub_line test left on a client. */
static void free_topics(client_t *c) {
    for (size_t i = 0; i < c->n_topics; i++) free(c->topics[i]);
    c->n_topics = 0;
    c->match_all = 0;
}

/* Build a client_t subscribed to the given pattern list. */
static void make_sub(client_t *c, const char **pats, size_t n) {
    memset(c, 0, sizeof *c);
    for (size_t i = 0; i < n; i++) {
        c->topics[c->n_topics++] = strdup(pats[i]);
        if (strcmp(pats[i], "**") == 0) c->match_all = 1;
    }
}

/* ---------------- 1. CRC32 ---------------- */

static void test_crc32(void) {
    SECTION("crc32");
    /* The universal CRC-32/IEEE check value. */
    CHECK(crc32_calc((const uint8_t *)"123456789", 9) == 0xCBF43926u);
    /* Empty input must give 0 (init ^ final cancel out). */
    CHECK(crc32_calc((const uint8_t *)"", 0) == 0x00000000u);
    /* A flipped bit must change the checksum. */
    uint32_t a = crc32_calc((const uint8_t *)"planetar", 8);
    uint32_t b = crc32_calc((const uint8_t *)"planetas", 8);
    CHECK(a != b);
}

/* ---------------- 2. wal_segment_path ---------------- */

static void test_segment_path(void) {
    SECTION("wal_segment_path");
    char p[512];
    wal_segment_path(p, sizeof p, "/tmp/x", 1234);
    CHECK(strcmp(p, "/tmp/x/segment-000000001234.wal") == 0);
    wal_segment_path(p, sizeof p, ".", 0);
    CHECK(strcmp(p, "./segment-000000000000.wal") == 0);
}

/* ---------------- 3. parse_sub_line ---------------- */

static void test_parse_sub_line(void) {
    SECTION("parse_sub_line");
    client_t c;

    /* Single topic. */
    memset(&c, 0, sizeof c);
    char l1[] = "SUB foo";
    CHECK(parse_sub_line(&c, l1, strlen(l1)) == 0);
    CHECK(c.n_topics == 1);
    CHECK(strcmp(c.topics[0], "foo") == 0);
    CHECK(c.match_all == 0);
    free_topics(&c);

    /* Multiple topics, repeated separators, trailing CR. */
    memset(&c, 0, sizeof c);
    char l2[] = "SUB a  b   c\r";
    CHECK(parse_sub_line(&c, l2, strlen(l2)) == 0);
    CHECK(c.n_topics == 3);
    CHECK(strcmp(c.topics[2], "c") == 0);
    free_topics(&c);

    /* "**" sets match_all. */
    memset(&c, 0, sizeof c);
    char l3[] = "SUB **";
    CHECK(parse_sub_line(&c, l3, strlen(l3)) == 0);
    CHECK(c.match_all == 1);
    free_topics(&c);

    /* Wrong keyword. */
    memset(&c, 0, sizeof c);
    char l4[] = "PUB foo";
    CHECK(parse_sub_line(&c, l4, strlen(l4)) == -1);
    free_topics(&c);

    /* SUB with no topics at all. */
    memset(&c, 0, sizeof c);
    char l5[] = "SUB   ";
    CHECK(parse_sub_line(&c, l5, strlen(l5)) == -1);
    free_topics(&c);

    /* One topic over the limit fails. */
    memset(&c, 0, sizeof c);
    char big[4096] = "SUB";
    size_t off = 3;
    for (int i = 0; i < MAX_TOPICS_PER_SUB + 1; i++)
        off += (size_t)snprintf(big + off, sizeof big - off, " t%d", i);
    CHECK(parse_sub_line(&c, big, off) == -1);
    free_topics(&c);
}

/* ---------------- 4. topic_matches ---------------- */

static void test_topic_matches(void) {
    SECTION("topic_matches");
    client_t c;
    const char *pats[] = { "exact", "chat.pac.*" };
    make_sub(&c, pats, 2);

    CHECK(topic_matches(&c, "exact", 5) == 1);          /* exact     */
    CHECK(topic_matches(&c, "exac", 4) == 0);           /* shorter   */
    CHECK(topic_matches(&c, "exactX", 6) == 0);         /* longer    */
    CHECK(topic_matches(&c, "chat.pac.123", 12) == 1);  /* wildcard  */
    CHECK(topic_matches(&c, "chat.pac.", 9) == 1);      /* bare stem */
    CHECK(topic_matches(&c, "chat.pacX", 9) == 0);      /* bad stem  */
    CHECK(topic_matches(&c, "chat.pa", 7) == 0);        /* too short */
    free_topics(&c);

    /* match_all takes everything, even the empty topic. */
    const char *all[] = { "**" };
    make_sub(&c, all, 1);
    CHECK(topic_matches(&c, "anything.at.all", 15) == 1);
    CHECK(topic_matches(&c, "", 0) == 1);
    free_topics(&c);

    /* And the SHM twin must agree. */
    shm_client_t s;
    memset(&s, 0, sizeof s);
    s.topics[s.n_topics++] = strdup("chat.pac.*");
    CHECK(shm_topic_matches(&s, "chat.pac.9", 10) == 1);
    CHECK(shm_topic_matches(&s, "chat.zzz.9", 10) == 0);
    free(s.topics[0]);
}

/* ---------------- 5. rbuf_grow ---------------- */

static void test_rbuf_grow(void) {
    SECTION("rbuf_grow");
    client_t c;
    memset(&c, 0, sizeof c);

    /* First growth lands on the READ_CHUNK floor. */
    CHECK(rbuf_grow(&c, 100) == 0);
    CHECK(c.rcap == READ_CHUNK);

    /* Doubling growth. */
    CHECK(rbuf_grow(&c, READ_CHUNK + 1) == 0);
    CHECK(c.rcap == READ_CHUNK * 2);

    /* Clamped at exactly the protocol max. */
    CHECK(rbuf_grow(&c, FRAME_MAX_LEN + 4) == 0);
    CHECK(c.rcap == FRAME_MAX_LEN + 4);

    /* One byte past the protocol max is refused. */
    CHECK(rbuf_grow(&c, FRAME_MAX_LEN + 5) == -1);
    free(c.rbuf);
}

/* ---------------- 6. outq drop-oldest ---------------- */

static void test_outq_drop_oldest(void) {
    SECTION("outq_push drop-oldest");
    client_t c;
    memset(&c, 0, sizeof c);
    c.fd = -1;                       /* never flushed in this test  */

    /* Three 400 KiB frames against a 1 MiB cap: pushing the third
     * must evict exactly the first (oldest). */
    enum { F = 400 * 1024 };
    uint8_t *frame = malloc(F);
    memset(frame, 0xAB, F);

    CHECK(outq_push(&c, frame, F) == 0);
    CHECK(outq_push(&c, frame, F) == 0);
    CHECK(c.outq_bytes == 2 * F);
    CHECK(c.dropped == 0);

    CHECK(outq_push(&c, frame, F) == 0);
    CHECK(c.dropped == 1);                   /* head evicted        */
    CHECK(c.outq_bytes == 2 * F);            /* still under the cap */
    CHECK(c.outq_bytes <= OUTQ_MAX_BYTES);

    /* FIFO integrity after eviction: two entries, head != tail. */
    CHECK(c.outq_head != NULL);
    CHECK(c.outq_head->next == c.outq_tail);

    outq_free_all(&c);
    CHECK(c.outq_bytes == 0);
    CHECK(c.outq_head == NULL && c.outq_tail == NULL);
    free(frame);
}

/* ---------------- 7. try_flush over a socketpair ---------------- */

static void test_try_flush(void) {
    SECTION("try_flush partial-write resume");
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) == 0);

    /* Shrink the send buffer so a 200 KiB frame cannot fit in one
     * write — forcing the partial-write path. */
    int sndbuf = 16 * 1024;
    setsockopt(sv[0], SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof sndbuf);

    client_t c;
    memset(&c, 0, sizeof c);
    c.fd = sv[0];

    enum { F = 200 * 1024 };
    uint8_t *frame = malloc(F);
    for (size_t i = 0; i < F; i++) frame[i] = (uint8_t)(i & 0xFF);
    CHECK(outq_push(&c, frame, F) == 0);

    /* First flush: partial. Queue must shrink but not empty, and the
     * head entry's cursor must have advanced. */
    CHECK(try_flush(&c) == 0);
    CHECK(c.outq_head != NULL);
    CHECK(c.outq_head->sent > 0);
    CHECK(c.outq_bytes < F);

    /* Drain the reader side and re-flush until the queue empties,
     * verifying every byte arrives in order. */
    uint8_t rd[8192];
    size_t got = 0;
    int guard = 0;
    while (c.outq_head && guard++ < 10000) {
        ssize_t n = read(sv[1], rd, sizeof rd);
        if (n > 0) {
            for (ssize_t i = 0; i < n; i++)
                if (rd[i] != (uint8_t)((got + (size_t)i) & 0xFF)) {
                    CHECK(!"byte order corrupted");
                    goto out;
                }
            got += (size_t)n;
        }
        CHECK(try_flush(&c) == 0);
    }
    /* Pull whatever the kernel still buffers after the queue closed. */
    for (;;) {
        ssize_t n = read(sv[1], rd, sizeof rd);
        if (n <= 0) break;
        got += (size_t)n;
    }
    CHECK(got == F);
    CHECK(c.outq_bytes == 0);
out:
    outq_free_all(&c);
    free(frame);
    close(sv[0]);
    close(sv[1]);
}

/* ---------------- 8. WAL roundtrip + recovery ---------------- */

static void test_wal(void) {
    SECTION("WAL roundtrip / recovery / corruption");
    char dir[] = "/tmp/planetar-wal-test-XXXXXX";
    CHECK(mkdtemp(dir) != NULL);

    /* Fresh init: sequence recovered as 0, segment 0 created. */
    wal_t w;
    CHECK(wal_init(&w, dir) == 0);
    CHECK(g_sequence == 0);

    /* Append five envelopes the way the broker does: pre-increment
     * the global, then log with that id. */
    uint8_t env[256];
    for (int i = 0; i < 5; i++) {
        int n = zt_build(env, sizeof env, "wal.test",
                         (const uint8_t *)"payload", 7);
        CHECK(n > 0);
        g_sequence++;
        CHECK(wal_append(&w, env, (size_t)n, ORIGIN_TCP,
                         g_sequence, now_ns_real()) == 0);
    }
    char seg0[512];
    snprintf(seg0, sizeof seg0, "%s", w.path);
    wal_close(&w);

    /* Scan agrees: 5 entries, next id is 6. */
    uint64_t next_seq = 0, count = 0;
    CHECK(wal_scan_segment(seg0, &next_seq, &count) == 0);
    CHECK(count == 5);
    CHECK(next_seq == 6);

    /* Re-init in the same dir: recovery adopts next_seq. */
    CHECK(wal_init(&w, dir) == 0);
    CHECK(g_sequence == 6);
    CHECK(w.base_sequence == 6);
    wal_close(&w);

    /* Torn tail, case 1: the cut reaches only the last record's
     * PAYLOAD. The scan validates headers only (the per-record CRC
     * is for replay-time validation, not the scan), so the record
     * still counts and — importantly — its id is still not reused.
     * Each record here is 32 (hdr) + 88 (padded env) = 120 bytes. */
    struct stat st;
    CHECK(stat(seg0, &st) == 0);
    CHECK(truncate(seg0, st.st_size - 10) == 0);
    CHECK(wal_scan_segment(seg0, &next_seq, &count) == 0);
    CHECK(count == 5);
    CHECK(next_seq == 6);

    /* Torn tail, case 2: the cut reaches into the last record's
     * HEADER. Now the scan must stop at the previous whole record. */
    CHECK(truncate(seg0, st.st_size - 100) == 0);
    CHECK(wal_scan_segment(seg0, &next_seq, &count) == 0);
    CHECK(count == 4);
    CHECK(next_seq == 5);

    /* Unreadable segment header: wal_init must take the
     * +1,000,000 anti-collision jump from the segment's base id. */
    char dir2[] = "/tmp/planetar-wal-test-XXXXXX";
    CHECK(mkdtemp(dir2) != NULL);
    char junk[600];
    snprintf(junk, sizeof junk, "%s/segment-000000000005.wal", dir2);
    FILE *f = fopen(junk, "w");
    CHECK(f != NULL);
    fwrite("garbage", 1, 7, f);          /* shorter than the header */
    fclose(f);
    CHECK(wal_init(&w, dir2) == 0);
    CHECK(g_sequence == 5 + 1000000);
    wal_close(&w);

    /* Note: temp dirs are left in /tmp for post-mortem inspection;
     * they are tiny and named planetar-wal-test-*. */
}

/* ---------------- 9. SHM ring write + wrap walk ---------------- */

/*
 * Simulates a live reader against ring_write_one: write a batch,
 * drain it, repeat — crossing the end-of-ring wrap several times.
 * Verifies sequence continuity, payload integrity, sentinel
 * handling, and the writer's absolute-index bookkeeping.
 */
static void test_ring(void) {
    SECTION("SHM ring write + wrap sentinel");

    shm_client_t c;
    memset(&c, 0, sizeof c);
    c.ring = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(c.ring != MAP_FAILED);
    c.ring_size = RING_SIZE;
    c.eventfd_fd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    CHECK(c.eventfd_fd >= 0);

    ring_header_t *hdr = (ring_header_t *)c.ring;
    const uint8_t *data = (const uint8_t *)c.ring + RING_HEADER_SIZE;

    /* ~12k entries x ~3 KiB ≈ 36 MiB through a 16 MiB ring: the
     * write position wraps at least twice. */
    enum { N = 12000, ENV = 3000 };
    static uint8_t env[ENV];

    uint64_t my_read = 0;        /* reader's absolute cursor        */
    uint64_t expect_seq = 1;     /* next sequence we must see       */
    uint64_t seen = 0;
    int payload_ok = 1, order_ok = 1;

    for (uint64_t seq = 1; seq <= N; seq++) {
        /* Payload pattern derived from seq so the reader can verify
         * it wasn't clobbered. */
        memset(env, (int)(seq & 0xFF), sizeof env);
        ring_write_one(&c, env, sizeof env, ORIGIN_TCP, seq, seq * 10);

        /* Drain every 97 writes (odd cadence crosses the wrap at
         * varying offsets). Mirrors shm-consumer.c's walk. */
        if (seq % 97 == 0 || seq == N) {
            uint64_t widx = __atomic_load_n(&hdr->write_idx,
                                            __ATOMIC_ACQUIRE);
            while (my_read < widx) {
                size_t pos = (size_t)(my_read % RING_DATA_SIZE);
                ring_entry_hdr_t eh;
                memcpy(&eh, data + pos, sizeof eh);
                if (eh.length == 0) {            /* wrap sentinel   */
                    my_read += RING_DATA_SIZE - pos;
                    continue;
                }
                if (eh.sequence != expect_seq) order_ok = 0;
                const uint8_t *p = data + pos + sizeof eh;
                if (p[0] != (uint8_t)(eh.sequence & 0xFF) ||
                    p[eh.length - 1] != (uint8_t)(eh.sequence & 0xFF))
                    payload_ok = 0;
                expect_seq++;
                seen++;
                my_read += sizeof eh + ((eh.length + 7u) & ~7u);
            }
        }
    }

    CHECK(seen == N);            /* nothing lost, reader kept up    */
    CHECK(order_ok);             /* strict sequence order           */
    CHECK(payload_ok);           /* bytes intact across wraps       */
    CHECK(c.dropped == 0);
    CHECK(c.delivered == N);
    CHECK(my_read == __atomic_load_n(&hdr->write_idx, __ATOMIC_ACQUIRE));

    /* The eventfd must have accumulated wakeups. */
    uint64_t ev = 0;
    CHECK(read(c.eventfd_fd, &ev, sizeof ev) == (ssize_t)sizeof ev);
    CHECK(ev > 0);

    /* Oversized envelope: counted as dropped, ring untouched. */
    uint64_t before = __atomic_load_n(&hdr->write_idx, __ATOMIC_RELAXED);
    uint8_t *huge = malloc(RING_DATA_SIZE);
    CHECK(huge != NULL);
    ring_write_one(&c, huge, RING_DATA_SIZE, ORIGIN_TCP, N + 1, 0);
    CHECK(c.dropped == 1);
    CHECK(__atomic_load_n(&hdr->write_idx, __ATOMIC_RELAXED) == before);
    free(huge);

    munmap(c.ring, RING_SIZE);
    close(c.eventfd_fd);
}

/* ---------------- 10. pub EOF flush + burst backlog ------------- */

/* Wrap one envelope into a length-prefixed wire frame. */
static size_t make_frame(uint8_t *out, size_t cap, const char *topic) {
    uint8_t env[2048];
    int n = zt_build(env, sizeof env, topic,
                     (const uint8_t *)"0123456789", 10);
    if (n < 0 || (size_t)(4 + n) > cap) return 0;
    uint32_t be = htonl((uint32_t)n);
    memcpy(out, &be, 4);
    memcpy(out + 4, env, (size_t)n);
    return 4 + (size_t)n;
}

/*
 * Regression for the 2026-06-11 EOF bug: a publisher that writes its
 * frames and closes immediately delivers data + EOF in one wakeup.
 * client_readable must parse the buffered frames (they reach the
 * counters / WAL / fan-out) BEFORE reporting the close.
 */
static void test_pub_eof_flush(void) {
    SECTION("pub EOF flushes buffered frames");
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) == 0);

    client_t *c = calloc(1, sizeof *c);
    c->ev.kind = EV_CLIENT;
    c->kind = CK_PUB;
    c->fd = sv[0];
    CHECK(rbuf_grow(c, READ_CHUNK) == 0);

    /* 25 frames, then immediate close — the worst-case timing. */
    uint8_t frame[2048];
    size_t fsz = make_frame(frame, sizeof frame, "eof.flush");
    CHECK(fsz > 0);
    for (int i = 0; i < 25; i++)
        CHECK(write(sv[1], frame, fsz) == (ssize_t)fsz);
    close(sv[1]);

    unsigned long long before = g_pub_msgs;
    int rc = client_readable(c);
    CHECK(rc == -1);                          /* EOF still closes   */
    CHECK(g_pub_msgs - before == 25);         /* ...but loses nothing */
    CHECK(c->rlen == 0);                      /* fully consumed     */

    close(sv[0]);
    free(c->rbuf);
    free(c);
}

/*
 * Regression for the burst-backlog kill: more than FRAME_MAX_LEN
 * bytes of *small* frames pending in one drain cycle must be parsed
 * in place, not treated as a protocol violation. We pre-fill the
 * read buffer to its cap (as a huge burst would) and verify the
 * read pump parses to make room instead of closing the client.
 */
static void test_pub_burst_backlog(void) {
    SECTION("pub burst backlog parses instead of killing");
    int sv[2];
    CHECK(socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, sv) == 0);

    client_t *c = calloc(1, sizeof *c);
    c->ev.kind = EV_CLIENT;
    c->kind = CK_PUB;
    c->fd = sv[0];
    CHECK(rbuf_grow(c, FRAME_MAX_LEN + 4) == 0);   /* buffer at cap  */

    /* Stuff the buffer with complete frames right up to the cap so
     * headroom < READ_CHUNK and rbuf_grow can no longer help. */
    uint8_t frame[2048];
    size_t fsz = make_frame(frame, sizeof frame, "burst.fill");
    CHECK(fsz > 0);
    unsigned long long filled = 0;
    while (c->rlen + fsz <= c->rcap) {
        memcpy(c->rbuf + c->rlen, frame, fsz);
        c->rlen += fsz;
        filled++;
    }
    CHECK(c->rcap - c->rlen < READ_CHUNK);

    /* More frames arrive behind the backlog, then EOF. */
    for (int i = 0; i < 10; i++)
        CHECK(write(sv[1], frame, fsz) == (ssize_t)fsz);
    close(sv[1]);

    unsigned long long before = g_pub_msgs;
    int rc = client_readable(c);
    CHECK(rc == -1);                              /* EOF closes     */
    CHECK(g_pub_msgs - before == filled + 10);    /* all parsed     */
    CHECK(c->rlen == 0);

    close(sv[0]);
    free(c->rbuf);
    free(c);
}

/* ---------------- 11. SHM producer drain (phase 5) -------------- */

/*
 * Test-side writer for an INBOUND ring — mirrors what pub-shm does:
 * sentinel on wrap, entry header + envelope + pad, release-store of
 * write_idx. The test is the sole writer, so the relaxed local read
 * of write_idx is fine.
 */
static void inring_write(shm_client_t *p, const uint8_t *env, size_t len) {
    ring_header_t *hdr = (ring_header_t *)p->ring;
    uint8_t *data = (uint8_t *)p->ring + RING_HEADER_SIZE;
    size_t padded = (len + 7u) & ~((size_t)7u);
    size_t total = sizeof(ring_entry_hdr_t) + padded;
    uint64_t cur = hdr->write_idx;
    size_t pos = (size_t)(cur % RING_DATA_SIZE);
    if (pos + total > RING_DATA_SIZE) {
        ring_entry_hdr_t s = {0};                 /* wrap sentinel   */
        memcpy(data + pos, &s, sizeof s);
        cur += RING_DATA_SIZE - pos;
        pos = 0;
    }
    ring_entry_hdr_t eh = {0};
    eh.length = (uint32_t)len;
    eh.origin = ORIGIN_SHM;
    eh.timestamp_ns = 42;
    memcpy(data + pos, &eh, sizeof eh);
    memcpy(data + pos + sizeof eh, env, len);
    __atomic_store_n(&hdr->write_idx, cur + total, __ATOMIC_RELEASE);
}

static void test_producer_drain(void) {
    SECTION("SHM producer drain");

    shm_client_t p;
    memset(&p, 0, sizeof p);
    p.ring = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE,
                  MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(p.ring != MAP_FAILED);
    p.ring_size = RING_SIZE;
    p.is_producer = 1;
    p.eventfd_fd = -1;             /* drain never touches it        */
    snprintf(p.peer, sizeof p.peer, "unit-prod");
    ring_header_t *hdr = (ring_header_t *)p.ring;

    uint8_t env[512];
    int n = zt_build(env, sizeof env, "drain.t",
                     (const uint8_t *)"payload", 7);
    CHECK(n > 0);

    /* (1) Five valid envelopes route and advance the cursor. */
    unsigned long long before = g_pub_msgs;
    for (int i = 0; i < 5; i++) inring_write(&p, env, (size_t)n);
    shm_producer_drain(&p);
    CHECK(g_pub_msgs - before == 5);
    CHECK(p.prod_msgs == 5);
    CHECK(p.prod_read == hdr->write_idx);
    CHECK(hdr->reserved[0] == p.prod_read);   /* cursor published   */
    CHECK(p.prod_bad == 0 && p.prod_resyncs == 0);

    /* (2) A bad-magic envelope is skipped — exactly one — without
     * desyncing the entries after it. */
    uint8_t bad[512];
    memcpy(bad, env, (size_t)n);
    bad[0] ^= 0xFF;                            /* corrupt the magic  */
    inring_write(&p, bad, (size_t)n);
    inring_write(&p, env, (size_t)n);
    inring_write(&p, env, (size_t)n);
    before = g_pub_msgs;
    shm_producer_drain(&p);
    CHECK(g_pub_msgs - before == 2);
    CHECK(p.prod_bad == 1);
    CHECK(p.prod_read == hdr->write_idx);

    /* (3) An insane entry header forces a resync to the head. */
    {
        uint8_t *data = (uint8_t *)p.ring + RING_HEADER_SIZE;
        uint64_t cur = hdr->write_idx;
        size_t pos = (size_t)(cur % RING_DATA_SIZE);
        ring_entry_hdr_t eh = {0};
        eh.length = FRAME_MAX_LEN + 1;         /* over protocol max  */
        memcpy(data + pos, &eh, sizeof eh);
        __atomic_store_n(&hdr->write_idx, cur + sizeof eh + 8,
                         __ATOMIC_RELEASE);
    }
    shm_producer_drain(&p);
    CHECK(p.prod_resyncs == 1);
    CHECK(p.prod_read == hdr->write_idx);

    /* (4) A lapped cursor (flow-control violation) also resyncs. */
    p.prod_read = hdr->write_idx;
    __atomic_store_n(&hdr->write_idx,
                     p.prod_read + RING_DATA_SIZE + 64,
                     __ATOMIC_RELEASE);
    shm_producer_drain(&p);
    CHECK(p.prod_resyncs == 2);
    CHECK(p.prod_read == hdr->write_idx);

    /* (5) Sustained stream across the wrap sentinel: ~36 MiB through
     * the 16 MiB ring with interleaved drains; nothing lost. */
    static uint8_t bigenv[3000];
    int bn = zt_build(bigenv, sizeof bigenv, "drain.big",
                      (const uint8_t *)bigenv, 2800);
    CHECK(bn > 0);
    before = g_pub_msgs;
    uint64_t bad_before = p.prod_bad, rs_before = p.prod_resyncs;
    for (int i = 0; i < 12000; i++) {
        inring_write(&p, bigenv, (size_t)bn);
        if (i % 97 == 0) shm_producer_drain(&p);
    }
    shm_producer_drain(&p);
    CHECK(g_pub_msgs - before == 12000);
    CHECK(p.prod_bad == bad_before);
    CHECK(p.prod_resyncs == rs_before);
    CHECK(p.prod_read == hdr->write_idx);

    munmap(p.ring, RING_SIZE);
}

/* ---------------- 12. zmesg helper <-> broker agreement -------- */

/*
 * Cross-check that envelopes built by the test helper put the topic
 * exactly where the broker's hardcoded offsets expect it.
 */
static void test_zmesg_agreement(void) {
    SECTION("zmesg helper vs broker offsets");
    uint8_t buf[512];
    int n = zt_build(buf, sizeof buf, "abc",
                     (const uint8_t *)"PAYLOAD", 7);
    CHECK(n == (int)(ZMESG_FIXED_HDR + 3 + 7));

    /* Magic where the broker reads it. */
    uint32_t magic;
    memcpy(&magic, buf, 4);
    CHECK(magic == ZMESG_MAGIC);

    /* topic_len via the broker's offset constant. */
    uint16_t tl;
    memcpy(&tl, buf + ZMESG_OFF_TOPIC_LEN, 2);
    CHECK(tl == 3);

    /* Topic bytes start right after the fixed header. */
    CHECK(memcmp(buf + ZMESG_FIXED_HDR, "abc", 3) == 0);

    /* And our own parser agrees with itself. */
    zt_parsed_t p;
    CHECK(zt_parse(buf, (size_t)n, &p) == 0);
    CHECK(p.topic_len == 3 && memcmp(p.topic, "abc", 3) == 0);
    CHECK(p.payload_len == 7 && memcmp(p.payload, "PAYLOAD", 7) == 0);

    /* Truncated buffers must be rejected, never over-read. */
    CHECK(zt_parse(buf, ZT_FIXED_HDR - 1, &p) == -1);
    CHECK(zt_parse(buf, (size_t)n - 1, &p) == -1);
}

/* ---------------- main ---------------- */

int main(void) {
    printf("planetar-broker unit tests\n");
    printf("(broker LOG lines below are expected — the WAL tests\n");
    printf(" run the real init/recovery paths)\n\n");

    test_crc32();
    test_segment_path();
    test_parse_sub_line();
    test_topic_matches();
    test_rbuf_grow();
    test_outq_drop_oldest();
    test_try_flush();
    test_wal();
    test_ring();
    test_pub_eof_flush();
    test_pub_burst_backlog();
    test_producer_drain();
    test_zmesg_agreement();

    printf("\n%d passed, %d failed\n", t_pass, t_fail);
    return t_fail ? 1 : 0;
}
