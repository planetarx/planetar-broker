/*
 * sub-shm — shared-memory test consumer for planetar-broker.
 *
 * Performs the SHM bootstrap exactly like shm-consumer.c (Unix
 * socket -> SUB line -> memfd + eventfd via SCM_RIGHTS -> mmap),
 * but instead of printing every envelope it counts them, verifies
 * broker sequence continuity, and computes end-to-end latency from
 * the embedded send timestamps.
 *
 * Usage:
 *     ./sub-shm [-b] <expect> <timeout_s> <filter> <topic> [topic...]
 *
 *   -b         busy-poll: spin on the ring's write_idx instead of
 *              blocking on the eventfd. Burns a core, but removes
 *              the wakeup syscall from the consumer side — this is
 *              how zbroker0's consumer-ultra measured its ns-class
 *              ring-hop numbers, so use this mode for benchmarks.
 *   expect     messages to wait for (0 = none; run to timeout,
 *              always exit 0).
 *   timeout_s  inactivity timeout in seconds.
 *   filter     topic prefix: only matching envelopes are counted and
 *              measured ("-" = count everything). Foreign traffic —
 *              e.g. live planetar-ais envelopes on a dev box — is
 *              drained but ignored. Note first_seq/last_seq/gaps are
 *              tracked over ALL ring entries regardless of filter:
 *              they describe ring integrity, not topic routing.
 *   topic...   topics for the SUB line ("**" = all).
 *
 * Output:
 *   - TWO latency blocks:
 *       e2e:      producer's payload timestamp -> consumer dequeue
 *                 (CLOCK_MONOTONIC both sides; includes the kernel
 *                 ingest transport, the broker, and the ring)
 *       ring hop: the broker's accept stamp (ring_entry_hdr_t
 *                 .timestamp_ns, CLOCK_REALTIME) -> consumer dequeue
 *                 (CLOCK_REALTIME). Isolates the bus core: WAL append
 *                 + fan-out + ring write + cross-process visibility.
 *                 With -b this is the closest in-architecture
 *                 analogue of zbroker0's ns-class ring-hop figure.
 *   - one greppable line:
 *       RESULT count=N expect=M first_seq=A last_seq=B gaps=G
 *              status=ok|bad
 *     `gaps` counts missing broker sequence ids between the first
 *     and last observed message — nonzero means the ring lapped us
 *     or another subscriber's topic interleaved (with per-topic
 *     SUBs, sequence ids are global, so gaps are EXPECTED when other
 *     topics are flowing; the functional suite only asserts gaps==0
 *     for whole-bus "**" subscriptions).
 * Exit code 0 iff count == expect (or expect == 0).
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "latency-stats.h"
#include "zmesg-test.h"

#define SHM_PATH         "/tmp/planetar-broker.sock"
#define RING_SIZE        (16u * 1024 * 1024)
#define RING_HEADER_SIZE 64u
#define RING_DATA_SIZE   (RING_SIZE - RING_HEADER_SIZE)
#define LINGER_MS        300

/* Mirrors of the broker's ring structs (see planetar-broker.c). */
typedef struct {
    volatile uint64_t write_idx;
    uint64_t reserved[7];
} ring_header_t;

typedef struct {
    uint32_t length;          /* 0 = wrap-to-start sentinel */
    uint32_t origin;
    uint64_t sequence;
    uint64_t timestamp_ns;
} __attribute__((packed)) ring_entry_hdr_t;

/* Receive the memfd + eventfd the broker sends via SCM_RIGHTS. */
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

int main(int argc, char **argv) {
    int busy = 0;
    if (argc > 1 && strcmp(argv[1], "-b") == 0) {
        busy = 1;
        argv++;
        argc--;
    }
    if (argc < 5) {
        fprintf(stderr,
            "usage: sub-shm [-b] <expect> <timeout_s> <filter> "
            "<topic> [topic...]\n");
        return 2;
    }
    uint64_t expect    = strtoull(argv[1], NULL, 10);
    int      timeout_s = atoi(argv[2]);
    const char *filter = argv[3];          /* "-" = count everything */
    size_t filter_len  = strcmp(filter, "-") ? strlen(filter) : 0;

    /* ---- bootstrap: SUB line, fd pair, mmap ---- */
    int sock_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock_fd < 0) { perror("socket"); return 2; }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", SHM_PATH);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        fprintf(stderr, "sub-shm: connect %s: %s\n",
                SHM_PATH, strerror(errno));
        return 2;
    }

    char line[1024];
    int off = snprintf(line, sizeof line, "SUB");
    for (int i = 4; i < argc; i++)
        off += snprintf(line + off, sizeof line - off, " %s", argv[i]);
    off += snprintf(line + off, sizeof line - off, "\n");
    if (write(sock_fd, line, (size_t)off) != off) {
        perror("write SUB");
        return 2;
    }

    int memfd, evfd;
    if (recv_two_fds(sock_fd, &memfd, &evfd) < 0) {
        fprintf(stderr, "sub-shm: fd handshake failed\n");
        return 3;
    }
    fprintf(stderr, "sub-shm: handshake OK memfd=%d evfd=%d\n", memfd, evfd);

    void *ring = mmap(NULL, RING_SIZE, PROT_READ, MAP_SHARED, memfd, 0);
    if (ring == MAP_FAILED) { perror("mmap"); return 2; }

    ring_header_t *hdr = (ring_header_t *)ring;
    const uint8_t *data = (const uint8_t *)ring + RING_HEADER_SIZE;

    if (busy) fprintf(stderr, "sub-shm: mode=busy-poll\n");

    /* ---- consume loop ---- */
    lat_stats_t st;        /* e2e: producer payload stamp -> dequeue */
    lat_stats_t hop;       /* ring hop: broker stamp -> dequeue      */
    lat_init(&st, 1u << 20);
    lat_init(&hop, 1u << 20);
    uint64_t count = 0, my_read = 0;
    uint64_t first_seq = 0, last_seq = 0, gaps = 0;
    int lingering = 0;
    uint64_t last_progress = zt_now_mono_ns();

    /* Poll the eventfd (it stays nonblocking — flags ride the shared
     * open file description from the broker) so we get a timeout.
     * In busy mode the eventfd is ignored entirely: the broker's
     * counter saturates harmlessly (its write sees EAGAIN). */
    struct pollfd pfd = { .fd = evfd, .events = POLLIN };
    for (;;) {
        uint64_t count_before = count;
        /* Drain everything published so far. */
        for (;;) {
            uint64_t widx = __atomic_load_n(&hdr->write_idx,
                                            __ATOMIC_ACQUIRE);
            if (my_read >= widx) break;
            while (my_read < widx) {
                size_t pos = (size_t)(my_read % RING_DATA_SIZE);
                ring_entry_hdr_t eh;
                memcpy(&eh, data + pos, sizeof eh);
                if (eh.length == 0) {            /* wrap sentinel   */
                    my_read += RING_DATA_SIZE - pos;
                    continue;
                }
                if (eh.length > RING_DATA_SIZE) {
                    fprintf(stderr, "sub-shm: bogus length %u; "
                            "jumping to head\n", eh.length);
                    my_read = widx;
                    break;
                }

                /* Sequence bookkeeping. */
                if (first_seq == 0) first_seq = eh.sequence;
                else if (eh.sequence > last_seq + 1)
                    gaps += eh.sequence - last_seq - 1;
                last_seq = eh.sequence;

                zt_parsed_t p;
                if (zt_parse(data + pos + sizeof eh, eh.length, &p) == 0) {
                    /* Count/measure only our topics when filtering. */
                    if (filter_len == 0 ||
                        (p.topic_len >= filter_len &&
                         memcmp(p.topic, filter, filter_len) == 0)) {
                        count++;
                        uint64_t send_ns = zt_payload_send_ns(&p);
                        if (send_ns)
                            lat_record(&st, zt_now_mono_ns() - send_ns);
                        /* Ring hop: broker accept stamp -> now, both
                         * CLOCK_REALTIME on the same host. */
                        uint64_t now_real = zt_now_real_ns();
                        if (eh.timestamp_ns &&
                            now_real > eh.timestamp_ns)
                            lat_record(&hop,
                                       now_real - eh.timestamp_ns);
                    }
                }
                my_read += sizeof eh + ((eh.length + 7u) & ~7u);
            }
        }

        if (count > count_before) last_progress = zt_now_mono_ns();
        if (expect && count >= expect && !lingering) {
            lingering = 1;
            last_progress = zt_now_mono_ns();
        }

        if (busy) {
            /* Spin: go straight back to the acquire-load of
             * write_idx. The only exits are the inactivity timeout
             * and the post-expect linger window. */
            uint64_t limit_ns = lingering
                ? (uint64_t)LINGER_MS * 1000000ull
                : (uint64_t)timeout_s * 1000000000ull;
            if (zt_now_mono_ns() - last_progress > limit_ns) break;
            continue;
        }

        int wait_ms = lingering ? LINGER_MS : timeout_s * 1000;
        int pr = poll(&pfd, 1, wait_ms);
        if (pr == 0) break;                      /* timeout / linger  */
        if (pr < 0) { if (errno == EINTR) continue; break; }

        /* Clear the eventfd counter; the value itself is unused —
         * the re-scan of write_idx above finds the actual work. */
        uint64_t v;
        ssize_t r = read(evfd, &v, sizeof v);
        (void)r;
    }

    lat_print(&st, "e2e, producer clock -> consumer clock");
    lat_print(&hop, "ring hop: broker accept stamp -> dequeue");
    int ok = expect ? (count == expect) : 1;
    printf("RESULT count=%llu expect=%llu first_seq=%llu last_seq=%llu "
           "gaps=%llu status=%s\n",
           (unsigned long long)count, (unsigned long long)expect,
           (unsigned long long)first_seq, (unsigned long long)last_seq,
           (unsigned long long)gaps, ok ? "ok" : "bad");

    lat_free(&st);
    lat_free(&hop);
    munmap(ring, RING_SIZE);
    close(memfd);
    close(evfd);
    close(sock_fd);
    return ok ? 0 : 1;
}
