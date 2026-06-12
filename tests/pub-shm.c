/*
 * pub-shm — shared-memory test producer for planetar-broker (phase 5).
 *
 * Performs the "PUB" handshake on /tmp/planetar-broker.sock, receives
 * the inbound ring memfd + eventfd via SCM_RIGHTS, maps the ring
 * read-write, and publishes zmesg envelopes straight into shared
 * memory — no kernel transport on the data path.
 *
 * Ring protocol (writer side — the broker's shm_producer_drain is the
 * reader; tests/test-unit.c's inring_write is the reference copy):
 *   - entry: [24-byte ring_entry_hdr_t][envelope][pad to 8]
 *   - a length==0 header is the wrap-to-start sentinel
 *   - publish order: copy bytes, RELEASE-store write_idx, bump eventfd
 *   - FLOW CONTROL: the broker release-stores its read cursor into
 *     header reserved[0]; we must never let write_idx run more than
 *     RING_DATA_SIZE past it. On a full ring we spin (with a bounded
 *     wait) until the broker catches up — ingest never overwrites
 *     unread data, unlike the outbound newest-wins rings.
 *
 * Pacing uses a spin-wait (not usleep) so sub-50 us intervals are
 * actually honored — usleep's timer slack floors around ~60 us.
 *
 * Usage:
 *     ./pub-shm [count] [topic] [payload_bytes] [interval_us]
 *
 * Defaults match pub-tcp: 10 / "test.topic" / 64 / 0.
 * Prints: SENT count=N bytes=B elapsed_ms=X rate=Y msg/s
 */

#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "zmesg-test.h"

#define SHM_PATH         "/tmp/planetar-broker.sock"
#define RING_SIZE        (16u * 1024 * 1024)
#define RING_HEADER_SIZE 64u
#define RING_DATA_SIZE   (RING_SIZE - RING_HEADER_SIZE)

/* Mirrors of the broker's ring structs (see planetar-broker.c). */
typedef struct {
    volatile uint64_t write_idx;
    uint64_t reserved[7];     /* [0] = broker's read cursor */
} ring_header_t;

typedef struct {
    uint32_t length;
    uint32_t origin;
    uint64_t sequence;
    uint64_t timestamp_ns;
} __attribute__((packed)) ring_entry_hdr_t;

#define ORIGIN_SHM 2u

/* Receive the two fds from the broker's SCM_RIGHTS reply. */
static int recv_two_fds(int sock_fd, int *out_a, int *out_b) {
    char dummy;
    struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };
    char cbuf[CMSG_SPACE(sizeof(int) * 2)];
    memset(cbuf, 0, sizeof cbuf);
    struct msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cbuf;
    msg.msg_controllen = sizeof cbuf;
    if (recvmsg(sock_fd, &msg, 0) != 1) return -1;
    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    if (!cm || cm->cmsg_level != SOL_SOCKET || cm->cmsg_type != SCM_RIGHTS)
        return -1;
    int fds[2];
    memcpy(fds, CMSG_DATA(cm), sizeof fds);
    *out_a = fds[0];
    *out_b = fds[1];
    return 0;
}

/*
 * ring_publish — write one envelope into the inbound ring, honoring
 * flow control. Returns 0 on success, -1 if the broker never freed
 * space (bounded wait expired — broker dead or wedged).
 */
static int ring_publish(ring_header_t *hdr, uint8_t *data, int evfd,
                        const uint8_t *env, size_t len, uint64_t seq) {
    size_t padded = (len + 7u) & ~((size_t)7u);
    size_t total = sizeof(ring_entry_hdr_t) + padded;

    uint64_t cur = hdr->write_idx;       /* we are the sole writer  */
    size_t pos = (size_t)(cur % RING_DATA_SIZE);

    /* Worst-case advance includes a possible wrap sentinel skip. */
    size_t skip = (pos + total > RING_DATA_SIZE)
                ? (RING_DATA_SIZE - pos) : 0;

    /* Flow control: wait until the whole advance fits between the
     * broker's read cursor and the ring size. ~2 s bounded spin. */
    for (uint64_t tries = 0;; tries++) {
        uint64_t rd = __atomic_load_n(&hdr->reserved[0], __ATOMIC_ACQUIRE);
        if (cur + skip + total - rd <= RING_DATA_SIZE) break;
        if (tries > 200000000ull) return -1;     /* broker wedged    */
    }

    if (skip) {
        ring_entry_hdr_t s = {0};                /* wrap sentinel    */
        memcpy(data + pos, &s, sizeof s);
        cur += skip;
        pos = 0;
    }

    ring_entry_hdr_t eh = {0};
    eh.length = (uint32_t)len;
    eh.origin = ORIGIN_SHM;
    eh.sequence = seq;                   /* producer-local; broker  */
    eh.timestamp_ns = zt_now_real_ns();  /* assigns bus identity    */
    memcpy(data + pos, &eh, sizeof eh);
    memcpy(data + pos + sizeof eh, env, len);

    /* Publish, then wake the broker. EAGAIN = counter saturated =
     * the broker has wakeups pending anyway. */
    __atomic_store_n(&hdr->write_idx, cur + total, __ATOMIC_RELEASE);
    uint64_t one = 1;
    ssize_t w = write(evfd, &one, sizeof one);
    (void)w;
    return 0;
}

int main(int argc, char **argv) {
    uint64_t count       = argc > 1 ? strtoull(argv[1], NULL, 10) : 10;
    const char *topic    = argc > 2 ? argv[2] : "test.topic";
    uint32_t payload_len = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 64;
    uint64_t interval_us = argc > 4 ? strtoull(argv[4], NULL, 10) : 0;
    if (payload_len < 16) payload_len = 16;

    /* ---- handshake ---- */
    int sock_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock_fd < 0) { perror("socket"); return 1; }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", SHM_PATH);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        fprintf(stderr, "pub-shm: connect %s: %s\n",
                SHM_PATH, strerror(errno));
        return 1;
    }
    if (write(sock_fd, "PUB\n", 4) != 4) { perror("write PUB"); return 1; }

    int memfd, evfd;
    if (recv_two_fds(sock_fd, &memfd, &evfd) < 0) {
        fprintf(stderr, "pub-shm: fd handshake failed\n");
        return 1;
    }
    void *ring = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE,
                      MAP_SHARED, memfd, 0);
    if (ring == MAP_FAILED) { perror("mmap"); return 1; }
    fprintf(stderr, "pub-shm: handshake OK memfd=%d evfd=%d\n",
            memfd, evfd);

    ring_header_t *hdr = (ring_header_t *)ring;
    uint8_t *data = (uint8_t *)ring + RING_HEADER_SIZE;

    /* ---- publish loop ---- */
    static uint8_t env[1u << 20];
    uint8_t *scratch = malloc(payload_len);
    if (!scratch) { perror("malloc"); return 1; }

    uint64_t t0 = zt_now_mono_ns();
    uint64_t next = t0;
    uint64_t bytes = 0;

    for (uint64_t i = 0; i < count; i++) {
        /* Spin-wait pacing: precise even at single-digit us. */
        if (interval_us) {
            next += interval_us * 1000ull;
            while (zt_now_mono_ns() < next) { /* spin */ }
        }
        int n = zt_build_timed(env, sizeof env, topic,
                               scratch, payload_len, i);
        if (n < 0) { fprintf(stderr, "pub-shm: envelope too big\n"); return 1; }
        if (ring_publish(hdr, data, evfd, env, (size_t)n, i) < 0) {
            fprintf(stderr, "pub-shm: ring full, broker not draining "
                    "(at msg %llu)\n", (unsigned long long)i);
            return 1;
        }
        bytes += (uint64_t)n;
    }

    uint64_t elapsed = zt_now_mono_ns() - t0;
    double ms = elapsed / 1e6;
    double rate = ms > 0 ? (double)count / (ms / 1000.0) : 0;
    printf("SENT count=%llu bytes=%llu elapsed_ms=%.1f rate=%.0f msg/s\n",
           (unsigned long long)count, (unsigned long long)bytes, ms, rate);

    free(scratch);
    munmap(ring, RING_SIZE);
    close(memfd);
    close(evfd);
    close(sock_fd);
    return 0;
}
