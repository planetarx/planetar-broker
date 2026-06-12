/*
 * sub-tcp — TCP test consumer for planetar-broker.
 *
 * Connects to 127.0.0.1:12002, performs the "SUB ..." handshake,
 * then receives length-prefixed frames, validates each envelope,
 * counts them, and computes end-to-end latency from the embedded
 * send timestamps (see zmesg-test.h).
 *
 * Usage:
 *     ./sub-tcp <expect> <timeout_s> <slow_us> <filter> \
 *               <topic> [topic...]
 *
 *   expect     messages to wait for; reaching it starts a short
 *              linger window to catch unexpected extras, then exit.
 *              0 = no expectation: run until the inactivity timeout
 *              and always exit 0.
 *   timeout_s  inactivity timeout (no frame for this long -> stop).
 *   slow_us    sleep this long after every frame (simulates a slow
 *              consumer for backpressure tests). 0 = full speed.
 *   filter     topic prefix: only envelopes whose topic starts with
 *              this string are counted/measured; everything else is
 *              drained and ignored. "-" = count everything. This is
 *              what makes the suite robust on a host where live
 *              services (e.g. planetar-ais) are publishing to the
 *              same bus ports — see test-functional.sh.
 *   topic...   one or more topics for the SUB line ("**" = all).
 *
 * Output:
 *   - "LOCAL port=N" on stderr (lets scripts match this exact
 *     connection in the broker's log)
 *   - the latency stats block (latency-stats.h)
 *   - one greppable line:  RESULT count=N expect=M status=ok|bad
 * Exit code 0 iff status=ok (count == expect, or expect == 0).
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "latency-stats.h"
#include "zmesg-test.h"

#define SUB_PORT   12002
#define FRAME_MAX  (1u << 20)
#define LINGER_MS  300          /* extra-message catch window       */

/* Receive buffer: one max frame + read headroom, consumed in place
 * exactly the way the broker's own pub parser does it. */
static uint8_t rbuf[FRAME_MAX + 4 + 65536];
static size_t  rlen = 0;

int main(int argc, char **argv) {
    if (argc < 6) {
        fprintf(stderr,
            "usage: sub-tcp <expect> <timeout_s> <slow_us> "
            "<filter> <topic> [topic...]\n");
        return 2;
    }
    uint64_t expect    = strtoull(argv[1], NULL, 10);
    int      timeout_s = atoi(argv[2]);
    uint64_t slow_us   = strtoull(argv[3], NULL, 10);
    const char *filter = argv[4];          /* "-" = count everything */
    size_t filter_len  = strcmp(filter, "-") ? strlen(filter) : 0;

    /* ---- connect + handshake ---- */
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return 2; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(SUB_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        fprintf(stderr, "sub-tcp: connect: %s\n", strerror(errno));
        return 2;
    }

    /* Report our local port so test scripts can match this exact
     * connection in the broker's log (e.g. its dropped= count). */
    struct sockaddr_in local;
    socklen_t ll = sizeof local;
    if (getsockname(fd, (struct sockaddr *)&local, &ll) == 0)
        fprintf(stderr, "LOCAL port=%u\n", ntohs(local.sin_port));

    char line[1024];
    int off = snprintf(line, sizeof line, "SUB");
    for (int i = 5; i < argc; i++)
        off += snprintf(line + off, sizeof line - off, " %s", argv[i]);
    off += snprintf(line + off, sizeof line - off, "\n");
    if (write(fd, line, (size_t)off) != off) { perror("write SUB"); return 2; }

    /* Read the one-line reply; must be "OK". */
    char reply[64] = {0};
    ssize_t rn = read(fd, reply, sizeof reply - 1);
    if (rn <= 0 || strncmp(reply, "OK", 2) != 0) {
        fprintf(stderr, "sub-tcp: handshake rejected: %.*s\n",
                (int)(rn > 0 ? rn : 0), reply);
        return 3;
    }
    fprintf(stderr, "sub-tcp: handshake OK (%.*s", off, line);

    /* Any frame bytes that arrived glued to the "OK\n" belong to us. */
    if (rn > 3) {
        memcpy(rbuf, reply + 3, (size_t)(rn - 3));
        rlen = (size_t)(rn - 3);
    }

    /* ---- receive loop ---- */
    lat_stats_t st;
    lat_init(&st, 1u << 20);
    uint64_t count = 0;
    int lingering = 0;          /* expect reached; watching extras  */

    struct pollfd pfd = { .fd = fd, .events = POLLIN };
    for (;;) {
        /* Peel complete frames already buffered. */
        for (;;) {
            if (rlen < 4) break;
            uint32_t be;
            memcpy(&be, rbuf, 4);
            uint32_t flen = ntohl(be);
            if (flen == 0 || flen > FRAME_MAX) {
                fprintf(stderr, "sub-tcp: bogus frame length %u\n", flen);
                goto done;
            }
            if (rlen < 4 + flen) break;

            zt_parsed_t p;
            if (zt_parse(rbuf + 4, flen, &p) == 0) {
                /* Count/measure only our own topics when filtering;
                 * foreign traffic (live AIS etc.) is just drained. */
                if (filter_len == 0 ||
                    (p.topic_len >= filter_len &&
                     memcmp(p.topic, filter, filter_len) == 0)) {
                    count++;
                    uint64_t send_ns = zt_payload_send_ns(&p);
                    if (send_ns)
                        lat_record(&st, zt_now_mono_ns() - send_ns);
                }
            } else {
                fprintf(stderr, "sub-tcp: unparseable envelope\n");
            }
            memmove(rbuf, rbuf + 4 + flen, rlen - (4 + flen));
            rlen -= 4 + flen;
            if (slow_us) usleep((useconds_t)slow_us);
        }

        if (expect && count >= expect && !lingering) lingering = 1;

        /* Linger briefly after reaching the target so unexpected
         * extra deliveries (wrong-topic leaks) still get counted. */
        int wait_ms = lingering ? LINGER_MS : timeout_s * 1000;
        int pr = poll(&pfd, 1, wait_ms);
        if (pr == 0) break;                       /* timeout/linger up */
        if (pr < 0) { if (errno == EINTR) continue; break; }

        ssize_t n = read(fd, rbuf + rlen, sizeof rbuf - rlen);
        if (n <= 0) break;                        /* broker went away  */
        rlen += (size_t)n;
    }

done:
    lat_print(&st, "e2e, producer clock -> consumer clock");
    int ok = expect ? (count == expect) : 1;
    printf("RESULT count=%llu expect=%llu status=%s\n",
           (unsigned long long)count, (unsigned long long)expect,
           ok ? "ok" : "bad");
    lat_free(&st);
    close(fd);
    return ok ? 0 : 1;
}
