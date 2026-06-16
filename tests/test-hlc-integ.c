/*
 * test-hlc-integ.c — hermetic end-to-end test of HLC v2 stamping.
 *
 * Spawns its OWN planetar-broker on isolated, far-from-live ports
 * (PORT_BASE 13971) with a private WAL dir, private SHM socket, and
 * RING_ID set so stamping is on. It then drives the real TCP pub/sub
 * wire protocol and inspects the bytes a subscriber receives, proving
 * the three stamping behaviors from SPEC-hlc-envelope.md:
 *
 *   1. a v1 envelope is expanded to v2 (grows by exactly 32 bytes),
 *      carries this ring's id, a dense local_seq, and a nonzero HLC,
 *      with topic + payload preserved at the shifted offset;
 *   2. consecutive messages get strictly increasing HLC + dense seq;
 *   3. an already-stamped v2 envelope (foreign ring_id) passes through
 *      byte-for-byte unchanged (observed, never re-stamped).
 *
 * It binds loopback only and never touches ports 12001-12003, so it
 * cannot disturb a live broker. Everything is torn down on exit.
 *
 * Exit 0 on success; prints a TAP-ish line per check.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define PORT_BASE   13971
#define PUB_PORT    (PORT_BASE)
#define SUB_PORT    (PORT_BASE + 1)
#define RING_NAME   "test-ring-A"
#define FOREIGN_RID 0x1122334455667788ull

/* v2 envelope offsets (mirror planetar-broker.c / SPEC §3). */
#define OFF_VERSION    4u
#define OFF_HDR_LEN    6u
#define OFF_TOPIC_LEN  48u
#define OFF_PAYLOAD    62u
#define OFF_HLC_PHYS   66u
#define OFF_HLC_LOG    74u
#define OFF_RING_ID    78u
#define OFF_LOCAL_SEQ  86u
#define OFF_ERR_BOUND  94u
#define FIXED_V1       66u
#define FIXED_V2       98u
#define ZMAGIC         0x5A4D5347u

static int checks = 0, fails = 0;
#define CHECK(cond, ...) do { checks++; \
    if (!(cond)) { fails++; printf("not ok %d - ", checks); \
        printf(__VA_ARGS__); printf("\n"); } \
    else { printf("ok %d - ", checks); printf(__VA_ARGS__); printf("\n"); } \
} while (0)

/* Must match planetar-broker.c fnv1a64 exactly (ring id derivation). */
static uint64_t fnv1a64(const char *s) {
    uint64_t h = 0xCBF29CE484222325ull;
    while (*s) { h ^= (uint8_t)*s++; h *= 0x100000001B3ull; }
    return h;
}

/* Build a minimal v1 envelope: magic, version 1, topic, payload. */
static uint32_t build_v1(uint8_t *b, const char *topic,
                         const uint8_t *pl, uint32_t pl_len) {
    uint16_t tl = (uint16_t)strlen(topic);
    uint16_t hdr = (uint16_t)(FIXED_V1 + tl);
    memset(b, 0, FIXED_V1);
    uint32_t m = ZMAGIC; memcpy(b, &m, 4);
    b[4] = 1;
    memcpy(b + OFF_HDR_LEN, &hdr, 2);
    memcpy(b + OFF_TOPIC_LEN, &tl, 2);
    memcpy(b + OFF_PAYLOAD, &pl_len, 4);
    memcpy(b + FIXED_V1, topic, tl);
    if (pl_len) memcpy(b + FIXED_V1 + tl, pl, pl_len);
    return FIXED_V1 + tl + pl_len;
}

/* Build a pre-stamped v2 envelope with a given ring_id. */
static uint32_t build_v2(uint8_t *b, const char *topic, uint64_t ring_id,
                         uint64_t phys, uint32_t log, uint64_t seq) {
    uint16_t tl = (uint16_t)strlen(topic);
    uint16_t hdr = (uint16_t)(FIXED_V2 + tl);
    memset(b, 0, FIXED_V2);
    uint32_t m = ZMAGIC; memcpy(b, &m, 4);
    b[4] = 2;
    memcpy(b + OFF_HDR_LEN, &hdr, 2);
    memcpy(b + OFF_TOPIC_LEN, &tl, 2);
    memcpy(b + OFF_HLC_PHYS, &phys, 8);
    memcpy(b + OFF_HLC_LOG, &log, 4);
    memcpy(b + OFF_RING_ID, &ring_id, 8);
    memcpy(b + OFF_LOCAL_SEQ, &seq, 8);
    memcpy(b + FIXED_V2, topic, tl);
    return FIXED_V2 + tl;
}

static int connect_port(int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) return -1;
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
    struct sockaddr_in a = {0};
    a.sin_family = AF_INET;
    a.sin_port = htons((uint16_t)port);
    a.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&a, sizeof a) < 0) { close(fd); return -1; }
    return fd;
}

/* Frame and send one envelope to the pub socket (4-byte BE len + bytes). */
static int pub_send(int fd, const uint8_t *env, uint32_t len) {
    uint8_t hdr[4];
    uint32_t be = htonl(len);
    memcpy(hdr, &be, 4);
    if (write(fd, hdr, 4) != 4) return -1;
    return write(fd, env, len) == (ssize_t)len ? 0 : -1;
}

/* Read exactly one length-prefixed frame into out; returns frame len. */
static long recv_frame(int fd, uint8_t *out, size_t cap) {
    uint8_t lenb[4];
    size_t got = 0;
    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    while (got < 4) {
        if (poll(&pfd, 1, 3000) <= 0) return -1;
        ssize_t n = read(fd, lenb + got, 4 - got);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    uint32_t be; memcpy(&be, lenb, 4);
    uint32_t flen = ntohl(be);
    if (flen == 0 || flen > cap) return -1;
    got = 0;
    while (got < flen) {
        if (poll(&pfd, 1, 3000) <= 0) return -1;
        ssize_t n = read(fd, out + got, flen - got);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return (long)flen;
}

int main(void) {
    /* Isolated runtime dir for WAL + SHM socket. */
    char dir[] = "/tmp/hlc-integ-XXXXXX";
    if (!mkdtemp(dir)) { perror("mkdtemp"); return 2; }
    char wal[256], sock[256], base[16];
    snprintf(wal, sizeof wal, "%s/wal", dir);
    snprintf(sock, sizeof sock, "%s/b.sock", dir);
    snprintf(base, sizeof base, "%d", PORT_BASE);

    /* Spawn our own broker. The binary sits one dir up from tests/. */
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 2; }
    if (pid == 0) {
        setenv("PORT_BASE", base, 1);
        setenv("SHM_PATH", sock, 1);
        setenv("WAL_DIR", wal, 1);
        setenv("RING_ID", RING_NAME, 1);
        /* Quiet the broker's stderr so the TAP output stays readable. */
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) { dup2(devnull, 2); close(devnull); }
        execl("./planetar-broker", "planetar-broker", (char *)NULL);
        execl("../planetar-broker", "planetar-broker", (char *)NULL);
        _exit(127);
    }

    /* Wait for the pub port to accept (broker came up). */
    int pubfd = -1;
    for (int i = 0; i < 100; i++) {
        pubfd = connect_port(PUB_PORT);
        if (pubfd >= 0) break;
        struct timespec ts = { 0, 20 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }
    if (pubfd < 0) { fprintf(stderr, "broker did not start\n"); kill(pid, SIGTERM); return 2; }

    /* Subscriber on a unique topic prefix (coexistence-safe). */
    const char *topic = "hlctest.alpha";
    int subfd = connect_port(SUB_PORT);
    if (subfd < 0) { fprintf(stderr, "sub connect failed\n"); kill(pid, SIGTERM); return 2; }
    char subline[64];
    int n = snprintf(subline, sizeof subline, "SUB %s\n", topic);
    if (write(subfd, subline, (size_t)n) != n) { kill(pid, SIGTERM); return 2; }
    char ok[16]; (void)!read(subfd, ok, sizeof ok);   /* "OK\n" */

    uint64_t expect_rid = fnv1a64(RING_NAME);
    uint8_t env[512], frame[1024];

    /* ---- Case 1: v1 -> v2 expansion ---- */
    const uint8_t payload[] = "hello-disruptor";
    uint32_t v1len = build_v1(env, topic, payload, sizeof payload);
    pub_send(pubfd, env, v1len);

    long fl = recv_frame(subfd, frame, sizeof frame);
    CHECK(fl > 0, "received a frame for the v1 publish");
    if (fl > 0) {
        CHECK(frame[OFF_VERSION] == 2, "stamped envelope is v2 (was v1)");
        CHECK((uint32_t)fl == v1len + 32, "grew by exactly 32 bytes (%lu vs %u)",
              (unsigned long)fl, v1len + 32);
        uint64_t rid; memcpy(&rid, frame + OFF_RING_ID, 8);
        CHECK(rid == expect_rid, "ring_id is fnv1a64(RING_ID)");
        uint64_t seq; memcpy(&seq, frame + OFF_LOCAL_SEQ, 8);
        CHECK(seq == 1, "first message local_seq == 1 (got %llu)",
              (unsigned long long)seq);
        uint64_t phys; memcpy(&phys, frame + OFF_HLC_PHYS, 8);
        CHECK(phys != 0, "hlc_physical_ns is nonzero");
        uint16_t tl; memcpy(&tl, frame + OFF_TOPIC_LEN, 2);
        CHECK(tl == strlen(topic) &&
              memcmp(frame + FIXED_V2, topic, tl) == 0,
              "topic preserved at shifted offset 98");
        CHECK(memcmp(frame + FIXED_V2 + tl, payload, sizeof payload) == 0,
              "payload preserved after the topic");
    }

    /* ---- Case 2: monotonic across messages ---- */
    uint64_t phys1 = 0, log1 = 0;
    if (fl > 0) { memcpy(&phys1, frame + OFF_HLC_PHYS, 8);
                  uint32_t l; memcpy(&l, frame + OFF_HLC_LOG, 4); log1 = l; }
    v1len = build_v1(env, topic, payload, sizeof payload);
    pub_send(pubfd, env, v1len);
    fl = recv_frame(subfd, frame, sizeof frame);
    CHECK(fl > 0, "received a frame for the second publish");
    if (fl > 0) {
        uint64_t seq; memcpy(&seq, frame + OFF_LOCAL_SEQ, 8);
        CHECK(seq == 2, "second message local_seq == 2 (got %llu)",
              (unsigned long long)seq);
        uint64_t phys2; uint32_t log2;
        memcpy(&phys2, frame + OFF_HLC_PHYS, 8);
        memcpy(&log2, frame + OFF_HLC_LOG, 4);
        int greater = (phys2 > phys1) || (phys2 == phys1 && log2 > log1);
        CHECK(greater, "HLC strictly increased message-to-message");
    }

    /* ---- Case 3: already-stamped v2 passes through unchanged ---- */
    uint32_t v2len = build_v2(env, topic, FOREIGN_RID,
                              999999999ull, 7, 4242);
    uint8_t sent_copy[512];
    memcpy(sent_copy, env, v2len);
    pub_send(pubfd, env, v2len);
    fl = recv_frame(subfd, frame, sizeof frame);
    CHECK(fl > 0, "received a frame for the pre-stamped v2 publish");
    if (fl > 0) {
        CHECK((uint32_t)fl == v2len &&
              memcmp(frame, sent_copy, v2len) == 0,
              "foreign-ring v2 delivered byte-for-byte unchanged");
        uint64_t rid; memcpy(&rid, frame + OFF_RING_ID, 8);
        CHECK(rid == FOREIGN_RID, "foreign ring_id preserved (not re-stamped)");
    }

    /* ---- teardown ---- */
    close(pubfd);
    close(subfd);
    kill(pid, SIGTERM);
    waitpid(pid, NULL, 0);
    unlink(sock);
    /* best-effort WAL cleanup */
    char cmd[300]; snprintf(cmd, sizeof cmd, "rm -rf %s", dir);
    (void)!system(cmd);

    printf("1..%d\n", checks);
    printf("%s: %d checks, %d failed\n", fails ? "FAIL" : "PASS", checks, fails);
    return fails ? 1 : 0;
}
