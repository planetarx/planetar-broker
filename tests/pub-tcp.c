/*
 * pub-tcp — TCP test producer for planetar-broker.
 *
 * Connects to 127.0.0.1:12001 and streams length-prefixed zmesg
 * envelopes. Each payload carries [u64 send_ns][u64 index] (see
 * zmesg-test.h) so a consumer can compute end-to-end latency.
 *
 * Usage:
 *     ./pub-tcp [count] [topic] [payload_bytes] [interval_us]
 *
 *   count          messages to send            (default 10)
 *   topic          envelope topic              (default "test.topic")
 *   payload_bytes  payload size, min 16        (default 64)
 *   interval_us    pacing between sends; 0 =   (default 0)
 *                  full speed
 *
 * Prints one machine-greppable summary line:
 *     SENT count=N bytes=B elapsed_ms=X rate=Y msg/s
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zmesg-test.h"

#define PUB_PORT 12001
#define BUF_MAX  (1u << 20)

/* Blocking write-all; the producer socket is left blocking. */
static int write_all(int fd, const uint8_t *p, size_t len) {
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, p + off, len - off);
        if (n > 0) { off += (size_t)n; continue; }
        if (n < 0 && errno == EINTR) continue;
        return -1;
    }
    return 0;
}

int main(int argc, char **argv) {
    uint64_t count       = argc > 1 ? strtoull(argv[1], NULL, 10) : 10;
    const char *topic    = argc > 2 ? argv[2] : "test.topic";
    uint32_t payload_len = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 64;
    uint64_t interval_us = argc > 4 ? strtoull(argv[4], NULL, 10) : 0;
    if (payload_len < 16) payload_len = 16;   /* room for ts + index */

    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return 1; }
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(PUB_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        fprintf(stderr, "pub-tcp: connect 127.0.0.1:%d: %s\n",
                PUB_PORT, strerror(errno));
        return 1;
    }

    /* Frame staging: [4-byte BE length][envelope]. The envelope is
     * rebuilt per message so each one carries a fresh send_ns. */
    static uint8_t frame[4 + BUF_MAX];
    uint8_t *scratch = malloc(payload_len);
    if (!scratch) { perror("malloc"); return 1; }

    uint64_t t0 = zt_now_mono_ns();
    uint64_t bytes = 0;

    for (uint64_t i = 0; i < count; i++) {
        int n = zt_build_timed(frame + 4, BUF_MAX, topic,
                               scratch, payload_len, i);
        if (n < 0) { fprintf(stderr, "pub-tcp: envelope too big\n"); return 1; }
        uint32_t be = htonl((uint32_t)n);
        memcpy(frame, &be, 4);
        if (write_all(fd, frame, 4 + (size_t)n) < 0) {
            fprintf(stderr, "pub-tcp: write failed at msg %llu: %s\n",
                    (unsigned long long)i, strerror(errno));
            return 1;
        }
        bytes += 4 + (uint64_t)n;
        if (interval_us) usleep((useconds_t)interval_us);
    }

    uint64_t elapsed = zt_now_mono_ns() - t0;
    double ms = elapsed / 1e6;
    double rate = ms > 0 ? (double)count / (ms / 1000.0) : 0;
    printf("SENT count=%llu bytes=%llu elapsed_ms=%.1f rate=%.0f msg/s\n",
           (unsigned long long)count, (unsigned long long)bytes, ms, rate);

    free(scratch);
    close(fd);
    return 0;
}
