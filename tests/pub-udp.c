/*
 * pub-udp — UDP test producer for planetar-broker.
 *
 * Sends one zmesg envelope per datagram to 127.0.0.1:12003 (no
 * length prefix — the datagram boundary frames it). Payloads carry
 * the [u64 send_ns][u64 index] test convention from zmesg-test.h.
 *
 * Usage:
 *     ./pub-udp [count] [topic] [payload_bytes] [interval_us]
 *
 * Defaults match pub-tcp: 10 / "test.topic" / 64 / 0.
 *
 * Note: UDP is lossy by nature even on loopback (a full socket
 * buffer drops datagrams silently). The functional suite therefore
 * paces UDP sends with a small interval; the perf suite reports the
 * delivered count alongside the sent count.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include "zmesg-test.h"

#define UDP_PORT 12003
#define DGRAM_MAX 65000   /* stay under the UDP payload ceiling */

int main(int argc, char **argv) {
    uint64_t count       = argc > 1 ? strtoull(argv[1], NULL, 10) : 10;
    const char *topic    = argc > 2 ? argv[2] : "test.topic";
    uint32_t payload_len = argc > 3 ? (uint32_t)strtoul(argv[3], NULL, 10) : 64;
    uint64_t interval_us = argc > 4 ? strtoull(argv[4], NULL, 10) : 0;
    if (payload_len < 16) payload_len = 16;

    int fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(UDP_PORT);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    static uint8_t env[DGRAM_MAX];
    uint8_t *scratch = malloc(payload_len);
    if (!scratch) { perror("malloc"); return 1; }

    uint64_t t0 = zt_now_mono_ns();
    uint64_t bytes = 0;

    for (uint64_t i = 0; i < count; i++) {
        int n = zt_build_timed(env, sizeof env, topic,
                               scratch, payload_len, i);
        if (n < 0) { fprintf(stderr, "pub-udp: envelope too big\n"); return 1; }
        ssize_t s = sendto(fd, env, (size_t)n, 0,
                           (struct sockaddr *)&addr, sizeof addr);
        if (s != n) {
            fprintf(stderr, "pub-udp: sendto at msg %llu: %s\n",
                    (unsigned long long)i, strerror(errno));
            return 1;
        }
        bytes += (uint64_t)n;
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
