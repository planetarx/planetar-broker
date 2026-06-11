/*
 * shm-consumer — test client for planetar-broker phase 4.
 *
 * Connects to /tmp/planetar-broker.sock, sends a SUB line, receives the
 * ring memfd + eventfd via SCM_RIGHTS, mmaps the ring, and prints each
 * envelope as it arrives.
 *
 * Usage:
 *     ./shm-consumer            # SUB **
 *     ./shm-consumer chat.pac.* # SUB chat.pac.*
 *     ./shm-consumer foo bar    # SUB foo bar
 */

#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/eventfd.h>
#include <sys/mman.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#define SHM_PATH "/tmp/planetar-broker.sock"
#define RING_SIZE (16u * 1024 * 1024)
#define RING_HEADER_SIZE 64u
#define RING_DATA_SIZE (RING_SIZE - RING_HEADER_SIZE)
#define ZMESG_MAGIC 0x5A4D5347u
#define ZMESG_FIXED_HDR 66u
#define ZMESG_OFF_TOPIC_LEN 48u

typedef struct {
    volatile uint64_t write_idx;
    uint64_t reserved[7];
} ring_header_t;

typedef struct {
    uint32_t length;
    uint32_t origin;
    uint64_t sequence;
    uint64_t timestamp_ns;
} __attribute__((packed)) ring_entry_hdr_t;

static volatile sig_atomic_t g_stop = 0;
static void on_sigint(int s) { (void)s; g_stop = 1; }

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

    ssize_t n = recvmsg(sock_fd, &msg, 0);
    if (n != 1) {
        fprintf(stderr, "recvmsg: expected 1 byte, got %zd (%s)\n", n,
                n < 0 ? strerror(errno) : "");
        return -1;
    }
    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    if (!cm || cm->cmsg_level != SOL_SOCKET || cm->cmsg_type != SCM_RIGHTS) {
        fprintf(stderr, "no SCM_RIGHTS in ancillary data\n");
        return -1;
    }
    if (cm->cmsg_len < (socklen_t)CMSG_LEN(sizeof(int) * 2)) {
        fprintf(stderr, "SCM_RIGHTS short: %zu\n", (size_t)cm->cmsg_len);
        return -1;
    }
    int fds[2];
    memcpy(fds, CMSG_DATA(cm), sizeof fds);
    *out_a = fds[0];
    *out_b = fds[1];
    return 0;
}

static const char *origin_str(uint32_t o) {
    switch (o) {
        case 0: return "TCP";
        case 1: return "UDP";
        case 2: return "SHM";
        default: return "?";
    }
}

static void print_envelope(uint32_t origin, uint64_t seq, uint64_t ts_ns,
                           const uint8_t *env, size_t env_len) {
    if (env_len < ZMESG_FIXED_HDR) {
        printf("[seq=%" PRIu64 " %s] short envelope (%zu)\n", seq, origin_str(origin), env_len);
        return;
    }
    uint32_t magic;
    memcpy(&magic, env, 4);
    if (magic != ZMESG_MAGIC) {
        printf("[seq=%" PRIu64 " %s] bad magic 0x%08x\n", seq, origin_str(origin), magic);
        return;
    }
    uint16_t topic_len;
    memcpy(&topic_len, env + ZMESG_OFF_TOPIC_LEN, 2);
    uint16_t source_len;
    memcpy(&source_len, env + ZMESG_OFF_TOPIC_LEN + 2, 2);
    uint32_t payload_len;
    memcpy(&payload_len, env + 62, 4);
    if ((uint32_t)ZMESG_FIXED_HDR + topic_len + source_len > env_len) {
        printf("[seq=%" PRIu64 " %s] topic+source overrun\n", seq, origin_str(origin));
        return;
    }
    const uint8_t *topic = env + ZMESG_FIXED_HDR;
    const uint8_t *source = topic + topic_len;
    /* Find payload start: after topic/source/schema_name/corr/caus.
     * Walk all variable fields per zmesg.h. */
    uint16_t schema_len, corr_len, caus_len;
    memcpy(&schema_len, env + 52, 2);
    memcpy(&corr_len, env + 54, 2);
    memcpy(&caus_len, env + 56, 2);
    size_t voff = ZMESG_FIXED_HDR + topic_len + source_len + schema_len + corr_len + caus_len;
    if (voff + payload_len > env_len) {
        printf("[seq=%" PRIu64 " %s] payload overrun\n", seq, origin_str(origin));
        return;
    }
    const uint8_t *payload = env + voff;
    /* Print compact one-liner */
    printf("[seq=%" PRIu64 " %s ts=%" PRIu64 "] topic=%.*s source=%.*s payload=%.*s\n",
           seq, origin_str(origin), ts_ns,
           (int)topic_len, (const char *)topic,
           (int)source_len, (const char *)source,
           (int)(payload_len > 200 ? 200 : payload_len), (const char *)payload);
    fflush(stdout);
}

int main(int argc, char **argv) {
    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    /* Build SUB line */
    char sub_line[1024];
    int off = snprintf(sub_line, sizeof sub_line, "SUB");
    if (argc < 2) {
        off += snprintf(sub_line + off, sizeof sub_line - off, " **");
    } else {
        for (int i = 1; i < argc; i++) {
            off += snprintf(sub_line + off, sizeof sub_line - off, " %s", argv[i]);
        }
    }
    off += snprintf(sub_line + off, sizeof sub_line - off, "\n");

    int sock_fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (sock_fd < 0) { perror("socket"); return 1; }
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", SHM_PATH);
    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        fprintf(stderr, "connect %s: %s\n", SHM_PATH, strerror(errno));
        return 1;
    }
    if (write(sock_fd, sub_line, off) != off) { perror("write SUB"); return 1; }
    fprintf(stderr, "[shm-consumer] sent: %.*s", off, sub_line);

    int memfd, evfd;
    if (recv_two_fds(sock_fd, &memfd, &evfd) < 0) return 1;
    fprintf(stderr, "[shm-consumer] received memfd=%d eventfd=%d\n", memfd, evfd);

    /* The eventfd was created EFD_NONBLOCK on the broker side; flags live on
     * the shared open file description. Clear them so our read() blocks. */
    int fl = fcntl(evfd, F_GETFL, 0);
    if (fl >= 0) fcntl(evfd, F_SETFL, fl & ~O_NONBLOCK);

    void *ring = mmap(NULL, RING_SIZE, PROT_READ, MAP_SHARED, memfd, 0);
    if (ring == MAP_FAILED) { perror("mmap"); return 1; }
    fprintf(stderr, "[shm-consumer] ring mapped (%u bytes)\n", RING_SIZE);

    ring_header_t *hdr = (ring_header_t *)ring;
    const uint8_t *data = (const uint8_t *)ring + RING_HEADER_SIZE;
    uint64_t my_read = 0;

    while (!g_stop) {
        uint64_t val;
        ssize_t r = read(evfd, &val, sizeof val);
        if (r < 0) {
            if (errno == EINTR) continue;
            perror("read eventfd"); break;
        }
        /* val counts batched wake-ups; we just re-scan write_idx. */
        for (;;) {
            uint64_t write_idx = __atomic_load_n(&hdr->write_idx, __ATOMIC_ACQUIRE);
            if (my_read >= write_idx) break;
            while (my_read < write_idx) {
                size_t pos = (size_t)(my_read % RING_DATA_SIZE);
                ring_entry_hdr_t eh;
                memcpy(&eh, data + pos, sizeof eh);
                if (eh.length == 0) {
                    /* wrap sentinel — skip to ring start */
                    my_read += (RING_DATA_SIZE - pos);
                    continue;
                }
                size_t entry_total = sizeof eh + ((eh.length + 7u) & ~7u);
                if (eh.length > RING_DATA_SIZE) {
                    fprintf(stderr, "[shm-consumer] bogus length %u at pos %zu; resetting\n",
                            eh.length, pos);
                    my_read = write_idx;
                    break;
                }
                print_envelope(eh.origin, eh.sequence, eh.timestamp_ns,
                               data + pos + sizeof eh, eh.length);
                my_read += entry_total;
            }
        }
    }

    munmap(ring, RING_SIZE);
    close(memfd);
    close(evfd);
    close(sock_fd);
    return 0;
}
