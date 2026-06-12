/*
 * ring-hop — direct single-hop SHM ring benchmark (no broker).
 *
 * Reproduces the zbroker0 measurement topology that produced the
 * proposal's headline figure (SHM p50 80–140 ns, 2026-04-27): one
 * producer process writes entries into a shared ring; one consumer
 * process busy-polls the same ring; latency is writer-stamp ->
 * reader-dequeue, CLOCK_MONOTONIC both sides, percentiles by qsort
 * over raw samples.
 *
 * The ring algorithm is byte-for-byte planetar-broker's contract:
 * the same ring_header_t / ring_entry_hdr_t layout, absolute
 * write_idx with release/acquire publication, the length==0 wrap
 * sentinel, and the inbound-ring flow-control cursor in reserved[0].
 * So this measures THIS codebase's ring implementation — the number
 * planetar-broker can claim as its own — in the one-hop topology the
 * zbroker0 figure used. (The broker-in-path numbers live in
 * test-perf.sh; they answer a different question.)
 *
 * Both processes are pinned to distinct CPUs: the hop is dominated
 * by one cache-line handoff, so letting the scheduler bounce the
 * processes around would swamp the signal.
 *
 * Usage:
 *     ./ring-hop [count] [payload] [cpu_writer] [cpu_reader] [gap_us]
 *
 *   count       messages (default 1000000)
 *   payload     envelope-ish payload bytes per entry (default 64)
 *   cpu_writer  CPU to pin the writer to (default 2)
 *   cpu_reader  CPU to pin the reader to (default 4)
 *   gap_us      spin-paced gap between writes (default 1; 0 = blast,
 *               which measures throughput, not per-message latency)
 *
 * Output: a "RING-HOP" header, the writer's SENT line, and the
 * reader's percentile block in NANOSECONDS.
 */

#ifndef _GNU_SOURCE
#define _GNU_SOURCE          /* sched_setaffinity */
#endif
#include <sched.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define RING_SIZE        (16u * 1024 * 1024)
#define RING_HEADER_SIZE 64u
#define RING_DATA_SIZE   (RING_SIZE - RING_HEADER_SIZE)

/* Mirrors of planetar-broker's ring structs. reserved[0] is the
 * reader's published cursor (flow control); reserved[6] is abused
 * here as a "reader ready" start flag — fine, it's our private
 * benchmark ring, and the broker leaves reserved[1..6] at zero. */
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

static inline uint64_t now_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static void pin_cpu(int cpu) {
    cpu_set_t set;
    CPU_ZERO(&set);
    CPU_SET(cpu, &set);
    if (sched_setaffinity(0, sizeof set, &set) != 0)
        fprintf(stderr, "ring-hop: warn: pin to cpu %d failed\n", cpu);
}

static int cmp_u64(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a, y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/* ---------------- reader (child) ---------------- */

static int reader(ring_header_t *hdr, uint64_t count, int cpu) {
    pin_cpu(cpu);
    const uint8_t *data = (const uint8_t *)hdr + RING_HEADER_SIZE;
    uint64_t *samples = calloc(count, sizeof *samples);
    if (!samples) { perror("calloc"); return 1; }

    /* Signal the writer we are pinned and spinning. */
    __atomic_store_n(&hdr->reserved[6], 1, __ATOMIC_RELEASE);

    uint64_t my_read = 0, seen = 0;
    while (seen < count) {
        uint64_t widx = __atomic_load_n(&hdr->write_idx, __ATOMIC_ACQUIRE);
        if (my_read >= widx) continue;            /* spin             */
        while (my_read < widx && seen < count) {
            size_t pos = (size_t)(my_read % RING_DATA_SIZE);
            ring_entry_hdr_t eh;
            memcpy(&eh, data + pos, sizeof eh);
            if (eh.length == 0) {                 /* wrap sentinel    */
                my_read += RING_DATA_SIZE - pos;
                continue;
            }
            samples[seen++] = now_ns() - eh.timestamp_ns;
            my_read += sizeof eh + ((eh.length + 7u) & ~7u);
            /* Publish the flow-control cursor occasionally — often
             * enough that the writer never stalls, rarely enough to
             * keep this line out of the hot loop's cache traffic. */
            if ((seen & 0xFFF) == 0)
                __atomic_store_n(&hdr->reserved[0], my_read,
                                 __ATOMIC_RELEASE);
        }
    }

    /* Percentiles in nanoseconds — this is a ns-class measurement. */
    qsort(samples, count, sizeof *samples, cmp_u64);
    uint64_t sum = 0;
    for (uint64_t i = 0; i < count; i++) sum += samples[i];
    printf("RING-HOP latency (writer store -> reader dequeue), ns:\n");
    printf("  samples: %llu\n", (unsigned long long)count);
    printf("  Min: %llu ns\n", (unsigned long long)samples[0]);
    printf("  Avg: %llu ns\n", (unsigned long long)(sum / count));
    printf("  p50: %llu ns\n", (unsigned long long)samples[count * 50 / 100]);
    printf("  p90: %llu ns\n", (unsigned long long)samples[count * 90 / 100]);
    printf("  p99: %llu ns\n", (unsigned long long)samples[count * 99 / 100]);
    printf("  Max: %llu ns\n", (unsigned long long)samples[count - 1]);
    free(samples);
    return 0;
}

/* ---------------- writer (parent) ---------------- */

static void writer(ring_header_t *hdr, uint64_t count, uint32_t payload,
                   int cpu, uint64_t gap_us) {
    pin_cpu(cpu);
    uint8_t *data = (uint8_t *)hdr + RING_HEADER_SIZE;
    uint8_t *env = malloc(payload);
    memset(env, 'x', payload);

    /* Wait for the reader to be pinned and spinning. */
    while (!__atomic_load_n(&hdr->reserved[6], __ATOMIC_ACQUIRE)) {}

    uint64_t t0 = now_ns(), next = t0;
    uint64_t cur = 0;
    size_t padded = (payload + 7u) & ~((size_t)7u);
    size_t total = sizeof(ring_entry_hdr_t) + padded;

    for (uint64_t i = 0; i < count; i++) {
        if (gap_us) {                            /* spin-paced gap   */
            next += gap_us * 1000ull;
            while (now_ns() < next) {}
        }
        size_t pos = (size_t)(cur % RING_DATA_SIZE);
        size_t skip = (pos + total > RING_DATA_SIZE)
                    ? (RING_DATA_SIZE - pos) : 0;

        /* Flow control against the reader's published cursor. */
        for (;;) {
            uint64_t rd = __atomic_load_n(&hdr->reserved[0],
                                          __ATOMIC_ACQUIRE);
            if (cur + skip + total - rd <= RING_DATA_SIZE) break;
        }
        if (skip) {
            ring_entry_hdr_t s = {0};
            memcpy(data + pos, &s, sizeof s);
            cur += skip;
            pos = 0;
        }

        ring_entry_hdr_t eh = {0};
        eh.length = payload;
        eh.origin = 2;                           /* ORIGIN_SHM       */
        eh.sequence = i + 1;
        memcpy(data + pos, &eh, sizeof eh);
        memcpy(data + pos + sizeof eh, env, payload);
        /* Stamp as late as possible: right before publication, so the
         * sample excludes our own memcpy and measures the handoff. */
        uint64_t ts = now_ns();
        memcpy(data + pos + 16, &ts, 8);         /* eh.timestamp_ns  */
        __atomic_store_n(&hdr->write_idx, cur + total, __ATOMIC_RELEASE);
        cur += total;
    }

    double ms = (now_ns() - t0) / 1e6;
    printf("SENT count=%llu elapsed_ms=%.1f rate=%.0f msg/s\n",
           (unsigned long long)count, ms,
           ms > 0 ? count / (ms / 1000.0) : 0);
    free(env);
}

/* ---------------- main ---------------- */

int main(int argc, char **argv) {
    uint64_t count   = argc > 1 ? strtoull(argv[1], NULL, 10) : 1000000;
    uint32_t payload = argc > 2 ? (uint32_t)strtoul(argv[2], NULL, 10) : 64;
    int cpu_w        = argc > 3 ? atoi(argv[3]) : 2;
    int cpu_r        = argc > 4 ? atoi(argv[4]) : 4;
    uint64_t gap_us  = argc > 5 ? strtoull(argv[5], NULL, 10) : 1;

    printf("RING-HOP direct (no broker): %llu msgs, %u B payload, "
           "writer cpu%d -> reader cpu%d, gap %llu us\n",
           (unsigned long long)count, payload, cpu_w, cpu_r,
           (unsigned long long)gap_us);

    /* One shared anonymous ring, mapped before fork. */
    ring_header_t *hdr = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE,
                              MAP_SHARED | MAP_ANONYMOUS, -1, 0);
    if (hdr == MAP_FAILED) { perror("mmap"); return 1; }
    memset(hdr, 0, RING_HEADER_SIZE);

    /* Flush before forking so the child doesn't inherit (and later
     * replay) buffered output; the child flushes its own results
     * before _exit, which bypasses stdio teardown. */
    fflush(stdout);
    pid_t pid = fork();
    if (pid < 0) { perror("fork"); return 1; }
    if (pid == 0) {
        int rc = reader(hdr, count, cpu_r);
        fflush(stdout);
        _exit(rc);
    }

    writer(hdr, count, payload, cpu_w, gap_us);

    int status = 0;
    waitpid(pid, &status, 0);
    munmap(hdr, RING_SIZE);
    return WIFEXITED(status) ? WEXITSTATUS(status) : 1;
}
