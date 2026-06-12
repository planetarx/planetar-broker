/*
 * latency-stats.h — Min/Max/Avg + p50/p90/p99 latency accounting for
 * the test consumers.
 *
 * Ported from the zbroker0 benchmark harness (consumer-ultra.c), the
 * same code that produced the proposal's headline percentiles, so
 * numbers from this suite are methodologically comparable: samples
 * are collected raw into a bounded array and percentiles come from a
 * single qsort at print time (no streaming approximation).
 *
 * Usage:
 *     lat_stats_t st;
 *     lat_init(&st, 1 << 20);          // capacity: samples kept
 *     ...
 *     lat_record(&st, now_ns - send_ns);
 *     ...
 *     lat_print(&st);                  // human block to stdout
 *     lat_free(&st);
 *
 * If more samples arrive than `capacity`, min/max/avg stay exact and
 * the percentiles are computed over the first `capacity` samples.
 */

#ifndef LATENCY_STATS_H
#define LATENCY_STATS_H

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

typedef struct {
    uint64_t  min_ns;
    uint64_t  max_ns;
    uint64_t  sum_ns;
    uint64_t  count;              /* total samples seen             */
    uint64_t *samples;            /* first `capacity` raw samples   */
    size_t    capacity;
} lat_stats_t;

static inline void lat_init(lat_stats_t *st, size_t capacity) {
    st->min_ns = UINT64_MAX;
    st->max_ns = 0;
    st->sum_ns = 0;
    st->count = 0;
    st->capacity = capacity;
    st->samples = calloc(capacity, sizeof(uint64_t));
}

static inline void lat_record(lat_stats_t *st, uint64_t ns) {
    if (ns < st->min_ns) st->min_ns = ns;
    if (ns > st->max_ns) st->max_ns = ns;
    st->sum_ns += ns;
    if (st->count < st->capacity) st->samples[st->count] = ns;
    st->count++;
}

static inline int lat_cmp_u64_(const void *a, const void *b) {
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y);
}

/*
 * lat_print — emit the stats block under the given label. Each line
 * is also greppable by the perf script ("p50:", "p99:", ...). Units
 * are microseconds with two decimals, matching the zbroker0 output
 * format. The label line always begins "Latency (" so scripts can
 * sed-range every block with /^Latency/,/Max:/.
 */
static inline void lat_print(lat_stats_t *st, const char *label) {
    if (st->count == 0) {
        printf("Latency (%s): no samples\n", label);
        return;
    }
    double avg = (double)st->sum_ns / (double)st->count;
    printf("Latency (%s):\n", label);
    printf("  samples: %llu\n", (unsigned long long)st->count);
    printf("  Min: %.2f us\n", st->min_ns / 1000.0);
    printf("  Avg: %.2f us\n", avg / 1000.0);

    size_t n = st->count < st->capacity ? (size_t)st->count : st->capacity;
    if (n > 0) {
        qsort(st->samples, n, sizeof(uint64_t), lat_cmp_u64_);
        printf("  p50: %.2f us\n", st->samples[n * 50 / 100] / 1000.0);
        printf("  p90: %.2f us\n", st->samples[n * 90 / 100] / 1000.0);
        printf("  p99: %.2f us\n", st->samples[n * 99 / 100] / 1000.0);
    }
    printf("  Max: %.2f us\n", st->max_ns / 1000.0);
}

static inline void lat_free(lat_stats_t *st) {
    free(st->samples);
    st->samples = NULL;
}

#endif /* LATENCY_STATS_H */
