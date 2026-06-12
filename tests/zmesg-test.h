/*
 * zmesg-test.h — minimal zmesg envelope builder/parser for the test
 * suite.
 *
 * This mirrors the wire format defined in ~/github/sness23/zmesg/
 * zmesg.h (the authoritative spec). Like the broker itself, the tests
 * deliberately COPY the few facts they need instead of #including the
 * external header, so this repo stays self-contained — but that means
 * a zmesg format change must be reflected here, in planetar-broker.c,
 * and in shm-consumer.c together.
 *
 * Envelope layout (all fields little-endian):
 *
 *   Offset  Size  Field
 *   ------  ----  -----
 *   0       4     magic (0x5A4D5347 = "ZMSG")
 *   4       1     version (1)
 *   5       1     flags
 *   6       2     header_len (66 + all variable string lengths)
 *   8       16    id (UUIDv7; tests leave it zeroed)
 *   24      8     created_at_ns
 *   32      8     stored_at_ns
 *   40      8     published_at_ns
 *   48      2     topic_len
 *   50      2     source_len
 *   52      2     schema_name_len
 *   54      2     correlation_id_len
 *   56      2     causation_id_len
 *   58      4     schema_version
 *   62      4     payload_len
 *   66      ...   topic, source, schema_name, corr_id, caus_id,
 *                 then payload_len bytes of payload
 *
 * Test conventions on top of the format:
 *   - Only `topic` and `payload` are populated; the other variable
 *     fields are empty (len 0).
 *   - For latency measurement the first 16 payload bytes are:
 *         [u64 send_ns (CLOCK_MONOTONIC)] [u64 message index]
 *     CLOCK_MONOTONIC is system-wide on Linux, so a consumer process
 *     on the same host can subtract it from its own monotonic clock.
 */

#ifndef ZMESG_TEST_H
#define ZMESG_TEST_H

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <time.h>

#define ZT_MAGIC      0x5A4D5347u
#define ZT_VERSION    1u
#define ZT_FIXED_HDR  66u

/* Offsets used when poking at raw envelopes in assertions. */
#define ZT_OFF_TOPIC_LEN    48u
#define ZT_OFF_PAYLOAD_LEN  62u

/* ---- clocks ---- */

/* Monotonic ns — for cross-process latency math on one host. */
static inline uint64_t zt_now_mono_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* Wall-clock ns — what created_at_ns is specified to hold. */
static inline uint64_t zt_now_real_ns(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/* ---- builder ---- */

/*
 * zt_build — serialize a minimal valid envelope into `buf`.
 *
 *   topic        NUL-terminated topic string (copied without the NUL).
 *   payload      payload bytes (may be NULL when payload_len is 0).
 *   payload_len  payload size.
 *
 * Only topic + payload are populated; source/schema/corr/caus are
 * empty. created_at_ns is stamped with wall-clock now. Returns the
 * total envelope size, or -1 if it would not fit in `cap` bytes.
 */
static inline int zt_build(uint8_t *buf, size_t cap,
                           const char *topic,
                           const uint8_t *payload, uint32_t payload_len) {
    uint16_t topic_len = (uint16_t)strlen(topic);
    uint16_t header_len = (uint16_t)(ZT_FIXED_HDR + topic_len);
    uint32_t total = (uint32_t)header_len + payload_len;
    if (total > cap) return -1;

    memset(buf, 0, ZT_FIXED_HDR);          /* zero id + reserved     */

    uint32_t magic = ZT_MAGIC;
    memcpy(buf + 0, &magic, 4);
    buf[4] = ZT_VERSION;
    buf[5] = 0;                            /* flags                  */
    memcpy(buf + 6, &header_len, 2);
    /* id (8..23) stays zero — fine for tests                        */
    uint64_t created = zt_now_real_ns();
    memcpy(buf + 24, &created, 8);
    /* stored_at/published_at (32..47) stay zero                     */
    memcpy(buf + 48, &topic_len, 2);
    /* source/schema/corr/caus lens (50..57) stay zero               */
    /* schema_version (58..61) stays zero                            */
    memcpy(buf + 62, &payload_len, 4);

    memcpy(buf + ZT_FIXED_HDR, topic, topic_len);
    if (payload_len)
        memcpy(buf + ZT_FIXED_HDR + topic_len, payload, payload_len);
    return (int)total;
}

/*
 * zt_build_timed — zt_build with the test payload convention:
 * payload = [u64 send_ns mono][u64 index][pad 'x' to payload_len].
 * payload_len is clamped up to 16 so the two u64s always fit.
 * `scratch` must hold payload_len bytes. Returns envelope size or -1.
 */
static inline int zt_build_timed(uint8_t *buf, size_t cap,
                                 const char *topic,
                                 uint8_t *scratch, uint32_t payload_len,
                                 uint64_t index) {
    if (payload_len < 16) payload_len = 16;
    memset(scratch, 'x', payload_len);
    uint64_t send_ns = zt_now_mono_ns();
    memcpy(scratch + 0, &send_ns, 8);
    memcpy(scratch + 8, &index, 8);
    return zt_build(buf, cap, topic, scratch, payload_len);
}

/* ---- parser ---- */

/*
 * zt_parsed_t — zero-copy view into a received envelope buffer.
 * Pointers reference the caller's buffer; nothing is owned.
 */
typedef struct {
    const char    *topic;        /* NOT NUL-terminated             */
    uint16_t       topic_len;
    const uint8_t *payload;      /* NULL when payload_len == 0     */
    uint32_t       payload_len;
    uint64_t       created_at_ns;
} zt_parsed_t;

/*
 * zt_parse — validate an envelope and extract the fields tests care
 * about. Walks all five variable-length fields (even though tests
 * only ever set topic) so it stays correct for envelopes built by
 * other zmesg producers. Returns 0 on success, -1 on any bounds or
 * magic failure.
 */
static inline int zt_parse(const uint8_t *buf, size_t len, zt_parsed_t *out) {
    if (len < ZT_FIXED_HDR) return -1;

    uint32_t magic;
    memcpy(&magic, buf + 0, 4);
    if (magic != ZT_MAGIC) return -1;

    uint16_t topic_len, source_len, schema_len, corr_len, caus_len;
    uint32_t payload_len;
    memcpy(&topic_len,  buf + 48, 2);
    memcpy(&source_len, buf + 50, 2);
    memcpy(&schema_len, buf + 52, 2);
    memcpy(&corr_len,   buf + 54, 2);
    memcpy(&caus_len,   buf + 56, 2);
    memcpy(&payload_len, buf + 62, 4);

    size_t var = (size_t)topic_len + source_len + schema_len
               + corr_len + caus_len;
    if (ZT_FIXED_HDR + var + payload_len > len) return -1;

    out->topic = (const char *)(buf + ZT_FIXED_HDR);
    out->topic_len = topic_len;
    out->payload_len = payload_len;
    out->payload = payload_len
        ? buf + ZT_FIXED_HDR + var
        : NULL;
    memcpy(&out->created_at_ns, buf + 24, 8);
    return 0;
}

/*
 * zt_payload_send_ns — pull the embedded monotonic send timestamp out
 * of a test-convention payload, or 0 if the payload is too small.
 */
static inline uint64_t zt_payload_send_ns(const zt_parsed_t *p) {
    if (!p->payload || p->payload_len < 8) return 0;
    uint64_t ns;
    memcpy(&ns, p->payload, 8);
    return ns;
}

#endif /* ZMESG_TEST_H */
