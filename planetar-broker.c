/*
 * planetar-broker — phase 5
 *
 * Native zmesg envelope bus. TCP + UDP + SHM + WAL (CRC32). Single-file C.
 *
 * Producers connect to 127.0.0.1:12001 and stream length-prefixed frames:
 *     [u32 length, big-endian (network byte order)]
 *     [length bytes: zmesg envelope]
 *
 * Consumers connect to 127.0.0.1:12002, send a one-line subscribe
 *     "SUB <topic>[ <topic>]*\n"            (max 1 KiB, up to 32 topics)
 * and the broker replies
 *     "OK\n"          on success
 *     "ERR <reason>\n"  on failure (then closes)
 * Subsequent bytes are length-prefixed frames matching one of the subscribed
 * topics. The special topic "**" matches every envelope.
 *
 * Topic matching uses the topic field stored *inside* the zmesg envelope
 * (offset 48..50 = topic_len, offset 66 = topic start). The broker never
 * inspects the payload.
 *
 * Single-threaded epoll loop. Backpressure is drop-oldest: if a subscriber's
 * pending write queue exceeds OUTQ_MAX_BYTES we discard the oldest queued
 * frames and bump a per-client counter.
 *
 * UDP: producers can also send datagrams to 127.0.0.1:12003. Each datagram
 * is exactly one zmesg envelope (no length prefix — UDP datagram boundary
 * frames it). The broker fans these out to TCP subscribers using the same
 * length-prefixed TCP framing. No UDP-as-subscriber yet.
 *
 * SHM (phase 4): consumers connect to /tmp/planetar-broker.sock (a Unix
 * stream socket) and send "SUB <topic>[ <topic>]*\n". The broker creates
 * a memfd (16 MiB ring) and an eventfd, sends both file descriptors back
 * via SCM_RIGHTS, then writes matching envelopes into the ring and signals
 * the eventfd. Ring layout:
 *     [64-byte ring_header_t: volatile write_idx + padding]
 *     [RING_DATA_SIZE bytes of entries]
 * Each entry:
 *     [u32 length][u32 origin][u64 sequence][u64 timestamp_ns][data]
 *     [pad to 8 bytes]
 * length==0 is a "skip to ring start" sentinel inserted when the next entry
 * would not fit before end-of-ring. write_idx is absolute (never wraps);
 * readers compute position as write_idx % RING_DATA_SIZE.
 *
 * SHM producers (phase 5): connect to /tmp/planetar-broker.sock and send
 *     "PUB\n"
 * The broker creates an INBOUND ring (16 MiB memfd) plus an eventfd and
 * sends both fds back via SCM_RIGHTS — the subscriber handshake with the
 * data direction reversed. The producer writes ring entries (identical
 * entry layout, including the length==0 wrap sentinel), release-stores
 * write_idx, then bumps the eventfd. The broker validates each envelope
 * (zmesg magic, topic bounds) and routes it like any other ingest: one
 * fresh global (sequence, timestamp), then WAL -> SHM fan-out -> TCP
 * fan-out, origin ORIGIN_SHM.
 *
 * Inbound rings have FLOW CONTROL (outbound rings stay newest-wins): the
 * broker publishes its read cursor in ring_header_t.reserved[0] and a
 * producer must never let write_idx - read_cursor exceed RING_DATA_SIZE.
 * Ingest loss would silently bypass the WAL, so the producer waits (or
 * drops, and knows it) instead of overwriting unread entries.
 *
 * BROKER_SPIN=1 (env) makes the reactor busy-poll inbound rings with a
 * zero epoll timeout while at least one producer is attached: benchmark
 * mode. It burns a core and removes the eventfd wake from the ingest
 * path (~5 us -> ~100 ns class). Default remains fully evented.
 *
 * WAL: every valid envelope is appended to a segment file under WAL_DIR
 * (default ./wal/) before fan-out. Entry layout:
 *     [u16 magic=0xB10C][u16 origin][u32 data_len][u64 sequence]
 *     [u64 timestamp_ns][u32 crc32][u32 total_len][data bytes]
 * Segments are 64 MiB; segment header is "WAL1" + base_sequence + metadata.
 * On startup the broker scans WAL_DIR, finds the latest segment, walks its
 * entries to recover the next sequence number, then opens a fresh segment.
 * CRC32 uses the standard IEEE polynomial 0xEDB88320, computed over data.
 */

/*
 * ------------------------------------------------------------------
 * HOW TO READ THIS FILE
 * ------------------------------------------------------------------
 * The file is organized bottom-up: primitives first, then state
 * types, then the I/O paths, with main() last. Section banners look
 * like "---- name ----". In order:
 *
 *   tunables          all compile-time knobs (sizes, ports, limits)
 *   logging           log_msg + LOG_* macros (stderr only)
 *   CRC32             table-driven checksum used by the WAL
 *   WAL               on-disk formats, append, rotation, recovery
 *   SHM ring          shared-memory layout + the lock-free contract
 *   epoll dispatch    the tagged-pointer trick the reactor runs on
 *   client state      client_t (TCP) and its output queue
 *   globals           counters + the stop flag
 *   utility           socket setup helpers (listen/bind/nonblock)
 *   buffer growth     bounded read-buffer realloc
 *   outq              per-subscriber write queue + backpressure
 *   client lifecycle  alloc / registry / teardown for TCP clients
 *   SHM subscriber    shm_client_t, its registry, ring_write_one
 *   topic matching    exact / "prefix.*" / "**" rules
 *   SUB handshake     "SUB ..." line parsing + OK/ERR replies
 *   fanout            TCP delivery loop
 *   PUB frame parsing the TCP ingest hot path
 *   UDP datagram pump the UDP ingest hot path
 *   read/write pumps  per-event socket draining
 *   accept            connection intake (TCP and Unix/SHM)
 *   main              wiring + the reactor loop + shutdown
 *
 * Three things worth internalizing before editing anything:
 *
 *   1. ONE thread. Nothing here is locked because nothing needs to
 *      be — except the SHM ring, whose reader is another process
 *      (see the release/acquire notes at ring_header_t).
 *   2. ONE identity per message: (g_sequence, timestamp) is sampled
 *      once per accepted message and shared by WAL + TCP + SHM, so
 *      every observer sees the same ids for the same message.
 *   3. NEVER block the reactor: every socket is non-blocking, slow
 *      consumers lose data (drop-oldest queues / ring overwrite),
 *      and WAL failures disable the WAL instead of stalling.
 * ------------------------------------------------------------------
 */

/*
 * System headers. Plain POSIX/Linux throughout — no third-party
 * libraries. The Linux-specific machinery: epoll (the reactor),
 * eventfd (SHM wakeups), memfd_create via syscall() (the ring),
 * accept4 (atomic NONBLOCK|CLOEXEC accept), and SCM_RIGHTS
 * fd-passing over Unix sockets.
 */
#include <arpa/inet.h>      /* htons/htonl/ntohl, inet_ntop          */
#include <dirent.h>         /* opendir/readdir — WAL segment scan    */
#include <errno.h>
#include <fcntl.h>          /* open flags, fcntl(F_SETFL)            */
#include <netinet/in.h>     /* sockaddr_in, INADDR_LOOPBACK          */
#include <netinet/tcp.h>    /* TCP_NODELAY                           */
#include <signal.h>         /* SIGINT/SIGTERM/SIGPIPE handling       */
#include <stdarg.h>         /* va_list for log_msg                   */
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/epoll.h>      /* the event loop                        */
#include <sys/eventfd.h>    /* SHM reader wakeup channel             */
#include <sys/mman.h>       /* mmap of the ring memfd                */
#include <sys/socket.h>     /* sockets, sendmsg/SCM_RIGHTS           */
#include <sys/stat.h>       /* mkdir, chmod                          */
#include <sys/syscall.h>    /* SYS_memfd_create                      */
#include <sys/types.h>
#include <sys/uio.h>        /* writev for contiguous WAL records     */
#include <sys/un.h>         /* sockaddr_un (Unix domain socket)      */
#include <time.h>           /* clock_gettime, strftime               */
#include <unistd.h>

/* memfd_create's MFD_CLOEXEC flag can be missing from older libc
 * headers even when the kernel supports the syscall; define it
 * ourselves so the build doesn't depend on glibc version. */
#ifndef MFD_CLOEXEC
#define MFD_CLOEXEC 0x0001U
#endif

/* ---------------- tunables ---------------- */

/*
 * Every compile-time knob lives here; change + recompile (there is
 * no config file). All endpoints are loopback-local by design. The
 * predecessor zbroker0 used the 1100x port range and a different
 * socket path, so both brokers can coexist on one host.
 */

#define PUB_PORT             12001   /* TCP producers connect here    */
#define SUB_PORT             12002   /* TCP consumers connect here    */
#define UDP_PORT             12003   /* one envelope per datagram     */
#define SHM_PATH             "/tmp/planetar-broker.sock" /* handshake */

#define UDP_RECV_MAX         65535   /* max receivable datagram size  */
#define LISTEN_BACKLOG       64      /* listen(2) pending-conn queue  */

/* SHM ring geometry. One private ring per SHM subscriber. 16 MiB of
 * data gives a lagging reader on the order of 100 k small envelopes
 * of slack before unread entries get overwritten. The header is
 * exactly one cache line so write_idx never false-shares with data. */
#define RING_SIZE            (16u * 1024 * 1024)    /* 16 MiB total memfd */
#define RING_HEADER_SIZE     64u                     /* cache-line aligned */
#define RING_DATA_SIZE       (RING_SIZE - RING_HEADER_SIZE)
#define MAX_SHM_SUBS         128     /* SHM subscriber slots          */
#define MAX_SHM_PRODS        64      /* SHM producer slots            */

/* TCP buffering and protocol limits. */
#define READ_CHUNK           8192    /* read() granularity + initial
                                      * read-buffer size              */
#define FRAME_MAX_LEN        (1u << 20)    /* 1 MiB per envelope */
#define SUB_LINE_MAX         1024    /* longest legal "SUB ..." line  */
#define MAX_TOPICS_PER_SUB   32      /* topic patterns per subscriber */
#define OUTQ_MAX_BYTES       (1u << 20)    /* 1 MiB pending per sub */
#define EPOLL_BATCH          64      /* max events per epoll_wait     */

/* zmesg envelope facts (mirrors zmesg.h in ~/github/sness23/zmesg).
 * Deliberately copied, not #included — the broker needs only these
 * three facts about the format, but they MUST track zmesg changes
 * in lockstep (the same constants also live in shm-consumer.c). */
#define ZMESG_MAGIC          0x5A4D5347u    /* "ZMSG" read little-endian */
#define ZMESG_FIXED_HDR      66u    /* fixed-header bytes; topic next */
#define ZMESG_OFF_TOPIC_LEN  48u    /* u16 topic_len lives here       */

/* WAL constants — see the WAL section for the formats they govern. */
#define WAL_DIR_DEFAULT      "./wal"        /* env WAL_DIR overrides  */
#define WAL_SEG_MAX_BYTES    (64u * 1024 * 1024)    /* 64 MiB per segment */
#define WAL_ENTRY_MAGIC      0xB10Cu        /* per-record sync marker */
#define WAL_SEG_MAGIC        0x57414C31u             /* "WAL1" */
#define WAL_SEG_VERSION      1u             /* on-disk format version */
#define WAL_ALIGN            8u             /* record alignment bound */

/* ---------------- logging ---------------- */

/*
 * log_msg — timestamped printf to stderr, the broker's only log sink.
 *
 * Every line is prefixed with a wall-clock "HH:MM:SS.mmm" stamp (local
 * time) and a level tag, e.g.
 *
 *     [14:03:21.880 info] listening 127.0.0.1:12001 (TCP)
 *
 * We deliberately use CLOCK_REALTIME here (human-facing) rather than the
 * monotonic clock used for sequencing — these timestamps are for the
 * operator reading the console, not for message ordering. stderr is
 * flushed on every call so a crash never swallows the last few lines.
 *
 * `level`  short tag printed inside the bracket ("info"/"warn"/"err").
 * `fmt`    printf-style format string, followed by its varargs.
 *
 * Callers should use the LOG_INFO / LOG_WARN / LOG_ERR macros below
 * rather than calling this directly.
 */
static void log_msg(const char *level, const char *fmt, ...) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    struct tm tm;
    localtime_r(&ts.tv_sec, &tm);   /* _r variant: no shared statics */
    char tbuf[32];
    strftime(tbuf, sizeof tbuf, "%H:%M:%S", &tm);
    /* "[HH:MM:SS.mmm level] " — milliseconds carved from tv_nsec. */
    fprintf(stderr, "[%s.%03ld %s] ", tbuf, ts.tv_nsec / 1000000, level);
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);      /* the caller's actual message   */
    va_end(ap);
    fputc('\n', stderr);
    fflush(stderr);                 /* never lose the last line      */
}

/* Convenience wrappers — call these, not log_msg directly. */
#define LOG_INFO(...)  log_msg("info", __VA_ARGS__)
#define LOG_WARN(...)  log_msg("warn", __VA_ARGS__)
#define LOG_ERR(...)   log_msg("err",  __VA_ARGS__)

/* ---------------- CRC32 (IEEE 0xEDB88320) ---------------- */

/*
 * Standard table-driven CRC32 (the IEEE 802.3 / zlib variant). The WAL
 * stores one CRC per entry so a later reader (recovery, replay tooling)
 * can tell a clean record from a torn or bit-rotted one. We compute the
 * lookup table lazily on first use rather than shipping a 1 KiB literal.
 *
 *   crc32_table        256-entry lookup table, one uint32 per byte value.
 *   crc32_table_built  guard so we only build the table once.
 */
static uint32_t crc32_table[256];
static int crc32_table_built = 0;

/*
 * crc32_build_table — precompute the per-byte CRC remainders.
 *
 * For each possible byte value we run the polynomial division 8 times
 * (once per bit), reflecting in the IEEE polynomial 0xEDB88320 whenever
 * the low bit is set. The result is cached in crc32_table so the hot
 * path (crc32_calc) is a single table lookup per byte.
 */
static void crc32_build_table(void) {
    for (uint32_t i = 0; i < 256; i++) {
        uint32_t c = i;
        for (int k = 0; k < 8; k++)
            c = (c & 1) ? (0xEDB88320u ^ (c >> 1)) : (c >> 1);
        crc32_table[i] = c;
    }
    crc32_table_built = 1;
}

/*
 * crc32_calc — checksum `len` bytes of `data`, building the table if the
 * first call hasn't already. Standard init/final XOR with 0xFFFFFFFF.
 * Returns the 32-bit CRC; the WAL stores this and recovery recomputes it
 * to validate each record.
 */
static uint32_t crc32_calc(const uint8_t *data, size_t len) {
    if (!crc32_table_built) crc32_build_table();
    uint32_t c = 0xFFFFFFFFu;       /* standard initial value        */
    for (size_t i = 0; i < len; i++)
        /* Fold in one byte: table lookup on (crc ^ byte)'s low 8
         * bits, then shift the crc down one byte. */
        c = crc32_table[(c ^ data[i]) & 0xFF] ^ (c >> 8);
    return c ^ 0xFFFFFFFFu;         /* standard final inversion      */
}

/* ---------------- WAL ---------------- */

/*
 * msg_origin_t — which transport a message arrived on.
 *
 * Stamped into both the WAL entry header and the SHM ring entry header
 * so a downstream observer can tell where a message entered the bus.
 * ORIGIN_SHM marks messages from shared-memory producers (the "PUB"
 * handshake, phase 5). The numeric values are part of the on-disk WAL
 * format; do not renumber.
 */
typedef enum {
    ORIGIN_TCP = 0,
    ORIGIN_UDP = 1,
    ORIGIN_SHM = 2,
} msg_origin_t;

/*
 * wal_entry_hdr_t — fixed 32-byte header in front of every WAL record.
 *
 * On-disk layout of one record is:
 *
 *     [ wal_entry_hdr_t (32 bytes, packed) ][ data_len envelope bytes ]
 *     [ zero padding up to an 8-byte boundary ]
 *
 * The header is packed (no compiler padding) so the byte offsets shown
 * below are exact and stable across builds — recovery code reads these
 * fields by structure, and external replay tooling reads them by offset.
 *
 *   magic         0xB10C ("BLOC"-ish) — sync marker; a mismatch during a
 *                 forward scan means we've hit unwritten/garbage tail and
 *                 should stop.
 *   origin        msg_origin_t — transport the message came in on.
 *   data_len      length of the zmesg envelope that follows, in bytes.
 *   sequence      the global monotonic message id (see g_sequence).
 *   timestamp_ns  CLOCK_REALTIME nanoseconds when the broker accepted it.
 *   crc32         IEEE CRC32 over exactly the `data_len` envelope bytes.
 *   total_len     full on-disk size of this record including header and
 *                 8-byte padding — lets a reader skip to the next record
 *                 without re-deriving the alignment math.
 */
typedef struct {
    uint16_t magic;          /*  0: 0xB10C                  */
    uint16_t origin;         /*  2: msg_origin_t            */
    uint32_t data_len;       /*  4: zmesg envelope length   */
    uint64_t sequence;       /*  8: global monotonic ID     */
    uint64_t timestamp_ns;   /* 16: CLOCK_REALTIME          */
    uint32_t crc32;          /* 24: CRC32 over data         */
    uint32_t total_len;      /* 28: aligned entry size      */
    /* data[] follows */
} __attribute__((packed)) wal_entry_hdr_t;

/*
 * wal_seg_header_t — 64-byte header at the very start of each segment
 * file. Written once when the segment is opened, then rewritten in place
 * at clean shutdown to record the final entry_count and set the clean bit.
 *
 *   magic          0x57414C31 ("WAL1") — identifies a valid segment.
 *   version        format version (currently 1).
 *   base_sequence  sequence id of the first entry this segment will hold;
 *                  also encoded in the filename (segment-<base>.wal).
 *   created_ns     wall-clock creation time, for forensics.
 *   entry_count    number of entries; 0 until finalize rewrites it.
 *   flags          bit 0 = "cleanly closed". If this bit is clear on
 *                  startup, the segment was open during a crash and must
 *                  be scanned entry-by-entry to find the true tail.
 *   reserved       padding to 64 bytes / room for future fields.
 */
typedef struct {
    uint32_t magic;            /*  0: 0x57414C31 ("WAL1")  */
    uint32_t version;          /*  4: 1                    */
    uint64_t base_sequence;    /*  8: first entry's seq    */
    uint64_t created_ns;       /* 16: creation timestamp   */
    uint32_t entry_count;      /* 24: entries written      */
    uint32_t flags;            /* 28: bit 0 = clean close  */
    uint8_t  reserved[32];     /* 32: future use           */
} __attribute__((packed)) wal_seg_header_t;

/*
 * wal_t — in-memory handle for the currently-open WAL segment. One
 * global instance (g_wal) drives the whole log; there is no concurrency
 * because the broker is single-threaded.
 *
 *   dir            directory holding all segment files.
 *   fd             append-only fd of the active segment (-1 if none).
 *   base_sequence  base_sequence of the active segment.
 *   entry_count    entries appended to the active segment so far.
 *   bytes_written  bytes in the active segment, including its header —
 *                  compared against WAL_SEG_MAX_BYTES to trigger rotation.
 *   path           full path of the active segment (for logging/finalize).
 *   enabled        1 while logging is healthy; flipped to 0 on any write
 *                  error so the broker keeps serving traffic without WAL
 *                  rather than dying ("availability over durability").
 */
typedef struct {
    char     dir[256];
    int      fd;
    uint64_t base_sequence;
    uint64_t entry_count;
    size_t   bytes_written;     /* including segment header */
    char     path[512];
    int      enabled;
} wal_t;

static wal_t g_wal;          /* the single active WAL */

/*
 * g_sequence — the broker-wide monotonic message counter. Pre-incremented
 * once per accepted message and that one value is then shared by the WAL
 * record, the TCP fan-out, and the SHM fan-out so an observer on any
 * transport sees identical ids for the same message. Recovered forward on
 * startup from the last WAL segment (see wal_init).
 */
static uint64_t g_sequence = 0;

/*
 * now_ns_real — current wall-clock time as nanoseconds since the Unix
 * epoch. Used for the per-message timestamp that travels with every
 * record. CLOCK_REALTIME (not MONOTONIC) so timestamps are comparable
 * across process restarts and against other machines' clocks.
 */
static uint64_t now_ns_real(void) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

/*
 * ensure_dir — mkdir that treats "already exists" as success. Returns 0
 * if the directory exists (or was created), -1 on any other error.
 */
static int ensure_dir(const char *dir) {
    if (mkdir(dir, 0755) == 0) return 0;
    if (errno == EEXIST) return 0;
    LOG_ERR("mkdir %s: %s", dir, strerror(errno));
    return -1;
}

/*
 * wal_segment_path — format the canonical path for a segment whose first
 * sequence id is `base_seq`, e.g. "./wal/segment-000000001234.wal". The
 * zero-padded, fixed-width base id makes the filenames sort the same way
 * lexically and numerically, which is how wal_init picks "the latest".
 */
static void wal_segment_path(char *out, size_t cap, const char *dir, uint64_t base_seq) {
    snprintf(out, cap, "%s/segment-%012llu.wal", dir, (unsigned long long)base_seq);
}

/*
 * wal_open_new_segment — create the next segment file and write its
 * header. Called both at startup and on rotation when the active segment
 * fills past WAL_SEG_MAX_BYTES.
 *
 * Opens with O_APPEND so every write lands at end-of-file regardless of
 * the fd offset, then writes a fresh wal_seg_header_t (entry_count 0,
 * clean bit clear — it gets set later by wal_finalize_segment). On
 * success the wal_t is repointed at the new segment and its counters
 * reset. Returns 0 on success, -1 if the file or header write failed
 * (caller disables the WAL).
 */
static int wal_open_new_segment(wal_t *w, uint64_t base_sequence) {
    wal_segment_path(w->path, sizeof w->path, w->dir, base_sequence);
    /* O_APPEND: every writev lands at EOF regardless of fd offset.
     * O_CLOEXEC: a future fork/exec can't leak the log fd. */
    int fd = open(w->path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (fd < 0) {
        LOG_ERR("open %s: %s", w->path, strerror(errno));
        return -1;
    }
    /* Fresh header: entry_count 0 and the clean bit clear. Both stay
     * that way until wal_finalize_segment rewrites them at shutdown,
     * so an in-use (or crashed) segment always reads as "dirty". */
    wal_seg_header_t hdr = {0};
    hdr.magic = WAL_SEG_MAGIC;
    hdr.version = WAL_SEG_VERSION;
    hdr.base_sequence = base_sequence;
    hdr.created_ns = now_ns_real();
    hdr.entry_count = 0;
    hdr.flags = 0;
    if (write(fd, &hdr, sizeof hdr) != (ssize_t)sizeof hdr) {
        LOG_ERR("write seg header %s: %s", w->path, strerror(errno));
        close(fd);
        return -1;
    }
    /* Repoint the live handle at the new segment. */
    w->fd = fd;
    w->base_sequence = base_sequence;
    w->entry_count = 0;
    w->bytes_written = sizeof hdr;
    LOG_INFO("WAL segment open %s (base_seq=%llu)",
             w->path, (unsigned long long)base_sequence);
    return 0;
}

/*
 * wal_finalize_segment — cleanly close the active segment.
 *
 * Reopens the file read-write, rewrites the header in place (pwrite at
 * offset 0) with the final entry_count and the clean-close flag set, then
 * fsyncs both that fd and the append fd so the data is durable before we
 * close. This is what lets a *later* startup trust entry_count instead of
 * having to rescan. Entirely best-effort: a failure here is logged but
 * not fatal, because the forward scan in wal_scan_segment can always
 * reconstruct the truth from the entries themselves.
 */
static void wal_finalize_segment(wal_t *w) {
    if (w->fd < 0) return;
    /* The live fd is O_APPEND, which silently forces writes to EOF —
     * useless for an in-place header rewrite. Open a second, plain
     * O_WRONLY fd just for the pwrite at offset 0. */
    int rfd = open(w->path, O_WRONLY | O_CLOEXEC);
    if (rfd >= 0) {
        wal_seg_header_t hdr = {0};
        hdr.magic = WAL_SEG_MAGIC;
        hdr.version = WAL_SEG_VERSION;
        hdr.base_sequence = w->base_sequence;
        hdr.created_ns = now_ns_real();
        hdr.entry_count = (uint32_t)w->entry_count;
        hdr.flags = 1u;
        if (pwrite(rfd, &hdr, sizeof hdr, 0) != (ssize_t)sizeof hdr)
            LOG_WARN("pwrite finalize %s: %s", w->path, strerror(errno));
        fsync(rfd);
        close(rfd);
    }
    fsync(w->fd);     /* and flush the appended records themselves */
    close(w->fd);
    w->fd = -1;
}

/*
 * wal_append — append one envelope to the log. The single durability
 * hook on the message hot path; called for every accepted message just
 * before fan-out.
 *
 *   data      raw zmesg envelope bytes.
 *   len       envelope length.
 *   origin    transport it arrived on.
 *   sequence  the shared monotonic id for this message.
 *   ts_ns     the shared wall-clock timestamp for this message.
 *
 * Steps:
 *   1. If the WAL is disabled (init failed, or a prior write errored)
 *      return 0 immediately — the broker runs fine without durability.
 *   2. Round the envelope up to WAL_ALIGN (8) so the next record's header
 *      stays naturally aligned, and check whether the record fits in the
 *      current segment; if not, finalize and rotate to a new one.
 *   3. Fill a header (including the CRC over the envelope) and write the
 *      header + envelope + pad in a single writev() so a record is never
 *      interleaved with another writer (there is only one, but the atomic
 *      vector write also keeps the record contiguous on disk).
 *
 * A short or failed write disables the WAL (w->enabled = 0) and returns
 * -1; we never block the reactor retrying disk I/O.
 */
static int wal_append(wal_t *w, const uint8_t *data, size_t len,
                      msg_origin_t origin, uint64_t sequence, uint64_t ts_ns) {
    if (!w->enabled || w->fd < 0) return 0;

    /* Pad the envelope out to an 8-byte boundary so the *next* record's
     * header begins aligned. total = our header + padded payload. */
    size_t padded = (len + (WAL_ALIGN - 1)) & ~((size_t)WAL_ALIGN - 1);
    size_t total = sizeof(wal_entry_hdr_t) + padded;

    /* Rotate before writing if this record would overflow the segment.
     * The new segment's base is "where this one's sequence ids left off",
     * keeping ids dense across the segment boundary. */
    if (w->bytes_written + total > WAL_SEG_MAX_BYTES) {
        uint64_t next_base = w->base_sequence + w->entry_count;
        wal_finalize_segment(w);
        if (wal_open_new_segment(w, next_base) < 0) {
            w->enabled = 0;
            return -1;
        }
    }

    wal_entry_hdr_t hdr = {0};
    hdr.magic = WAL_ENTRY_MAGIC;
    hdr.origin = (uint16_t)origin;
    hdr.data_len = (uint32_t)len;
    hdr.sequence = sequence;
    hdr.timestamp_ns = ts_ns;
    hdr.crc32 = crc32_calc(data, len);
    hdr.total_len = (uint32_t)total;

    /* Gather-write the record as three segments: header, envelope, and
     * (only if needed) the trailing zero padding. writev keeps them
     * contiguous on disk in one syscall. The pad source is a small static
     * zero buffer; we never write more than WAL_ALIGN-1 pad bytes. */
    struct iovec iov[3];
    iov[0].iov_base = &hdr;
    iov[0].iov_len = sizeof hdr;
    iov[1].iov_base = (void *)data;
    iov[1].iov_len = len;
    static const uint8_t pad[WAL_ALIGN] = {0};
    iov[2].iov_base = (void *)pad;
    iov[2].iov_len = padded - len;
    int niov = iov[2].iov_len ? 3 : 2;   /* skip the pad iov when aligned */

    ssize_t expected = (ssize_t)(sizeof hdr + len + iov[2].iov_len);
    ssize_t got = writev(w->fd, iov, niov);
    if (got != expected) {
        LOG_ERR("WAL writev short=%zd expected=%zd: %s",
                got, expected, got < 0 ? strerror(errno) : "partial");
        w->enabled = 0;
        return -1;
    }
    w->entry_count++;
    w->bytes_written += total;
    return 0;
}

/*
 * wal_scan_segment — recover the tail of a segment by walking it.
 *
 * Reads the segment header, then steps record-by-record: read a fixed
 * entry header, sanity-check its magic, and lseek past the (padded)
 * payload to the next header. The walk stops cleanly at EOF or at the
 * first header whose magic doesn't match — which is exactly where a crash
 * would have left a torn or never-written record. We deliberately do NOT
 * trust the segment header's entry_count here; only the entries on disk
 * are believed, so an unclean shutdown recovers correctly.
 *
 *   path          segment file to scan.
 *   out_next_seq  set to one past the last valid sequence id (i.e. the id
 *                 the broker should assign to its next message). If the
 *                 segment has no entries, this is the segment's base.
 *   out_count     set to the number of valid entries found.
 *
 * Returns 0 on success, -1 if the file can't be opened or its header is
 * not a valid segment header.
 */
static int wal_scan_segment(const char *path, uint64_t *out_next_seq, uint64_t *out_count) {
    int fd = open(path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;

    wal_seg_header_t shdr;
    if (read(fd, &shdr, sizeof shdr) != (ssize_t)sizeof shdr || shdr.magic != WAL_SEG_MAGIC) {
        close(fd);
        return -1;
    }
    uint64_t base = shdr.base_sequence;
    uint64_t count = 0;
    uint64_t last_seq = base; /* if no entries, "next" is base itself */

    for (;;) {
        wal_entry_hdr_t eh;
        ssize_t r = read(fd, &eh, sizeof eh);
        if (r == 0) break;                     /* clean EOF           */
        /* Short read or wrong magic: a torn or never-written tail —
         * the crash point. Everything before it is trusted. */
        if (r != (ssize_t)sizeof eh || eh.magic != WAL_ENTRY_MAGIC) break;
        /* Seek past the (padded) payload — the scan needs headers
         * only, so even large segments recover quickly. */
        size_t padded = (eh.data_len + (WAL_ALIGN - 1)) & ~((size_t)WAL_ALIGN - 1);
        if (lseek(fd, (off_t)padded, SEEK_CUR) < 0) break;
        last_seq = eh.sequence;
        count++;
    }
    close(fd);
    *out_next_seq = last_seq + (count ? 1 : 0);
    *out_count = count;
    return 0;
}

/*
 * wal_init — bring the WAL online at startup and recover the sequence.
 *
 * 1. Ensure the WAL directory exists.
 * 2. Scan it for the segment with the highest base id (filenames sort
 *    numerically because the base is zero-padded fixed-width).
 * 3. If one exists, walk it (wal_scan_segment) to learn the next sequence
 *    id to assign. If that walk fails outright, jump the sequence forward
 *    by a large margin (base + 1,000,000) rather than risk reissuing ids
 *    that a torn segment might already contain — gaps are harmless, reuse
 *    is not.
 * 4. Adopt that next id into g_sequence and open a brand-new segment based
 *    at it (we never reopen and append to an old segment — recovery is
 *    read-only over history, writes always go to a fresh file).
 *
 * Returns 0 on success, -1 if the directory or first segment couldn't be
 * created (the broker then runs WAL-disabled).
 */
static int wal_init(wal_t *w, const char *dir) {
    memset(w, 0, sizeof *w);
    w->fd = -1;
    snprintf(w->dir, sizeof w->dir, "%s", dir);
    if (ensure_dir(w->dir) < 0) return -1;

    DIR *d = opendir(w->dir);
    if (!d) { LOG_ERR("opendir %s: %s", w->dir, strerror(errno)); return -1; }

    /* Find the segment with the highest base id. sscanf doubles as
     * filter (non-matching filenames are skipped) and parser. */
    char latest_name[256] = {0};
    uint64_t latest_base = 0;
    int found = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        unsigned long long base = 0;
        if (sscanf(de->d_name, "segment-%llu.wal", &base) != 1) continue;
        if (!found || base > latest_base) {
            latest_base = base;
            snprintf(latest_name, sizeof latest_name, "%s", de->d_name);
            found = 1;
        }
    }
    closedir(d);

    uint64_t next_seq = 0;
    if (found) {
        char path[512];
        snprintf(path, sizeof path, "%s/%s", w->dir, latest_name);
        uint64_t count = 0;
        if (wal_scan_segment(path, &next_seq, &count) < 0) {
            LOG_WARN("WAL scan failed for %s; starting fresh", path);
            next_seq = latest_base + 1000000;  /* leave room rather than collide */
        } else {
            LOG_INFO("WAL recovered: %s base=%llu entries=%llu next_seq=%llu",
                     latest_name, (unsigned long long)latest_base,
                     (unsigned long long)count, (unsigned long long)next_seq);
        }
    } else {
        LOG_INFO("WAL fresh start in %s", w->dir);
    }

    g_sequence = next_seq;
    w->enabled = 1;
    if (wal_open_new_segment(w, g_sequence) < 0) {
        w->enabled = 0;
        return -1;
    }
    return 0;
}

/*
 * wal_close — finalize the active segment (set its clean bit + count) and
 * mark the WAL disabled. Called once during orderly shutdown.
 */
static void wal_close(wal_t *w) {
    if (w->fd >= 0) wal_finalize_segment(w);
    w->enabled = 0;
}

/* ---------------- SHM ring (shared memory pub/sub) ---------------- */

/*
 * ring_header_t — the 64-byte control block at the start of each SHM
 * ring. It contains exactly one live field, write_idx, isolated on its
 * own cache line (the reserved[7] padding) so the writer's stores to it
 * never falsely share a line with the entry data that follows.
 *
 * write_idx is an ABSOLUTE byte offset that only ever increases — it
 * never wraps. Both sides recover the physical slot with
 * `write_idx % RING_DATA_SIZE`. Using an absolute counter (instead of a
 * wrapping head pointer) means the reader can detect exactly how far
 * behind it is just by subtracting its own cursor, with no ambiguity
 * about whether the ring has lapped.
 *
 * Memory ordering is the whole contract: the writer publishes an entry by
 * doing a release-store to write_idx *after* it has copied the bytes, and
 * the reader does an acquire-load of write_idx *before* reading bytes.
 * That pairing guarantees the reader can never observe a new write_idx
 * without also seeing the entry bytes it points past. (The mirror reader
 * lives in shm-consumer.c.)
 */
typedef struct {
    /* Absolute monotonic write offset; reader computes
     * pos = write_idx % RING_DATA_SIZE. Updated with release semantics;
     * reader uses an acquire load. */
    volatile uint64_t write_idx;
    /* reserved[0] doubles as the READ CURSOR on inbound (producer)
     * rings: the broker release-stores its absolute read position
     * there after draining, and a well-behaved producer never lets
     * write_idx - reserved[0] exceed RING_DATA_SIZE (flow control —
     * ingest must not lose data). Outbound (subscriber) rings leave
     * it zero; they are deliberately newest-wins with no feedback.
     * reserved[1..6] pad the header to one 64-byte cache line. */
    uint64_t reserved[7];
} ring_header_t;

/*
 * ring_entry_hdr_t — the 24-byte header in front of every envelope
 * written into a SHM ring. Mirrors the WAL entry's identity fields so a
 * SHM reader and the WAL see the same (sequence, timestamp, origin).
 *
 *   length        envelope length in bytes, OR 0 as a special
 *                 "wrap-to-start" sentinel (see ring_write_one): when an
 *                 entry won't fit before end-of-ring, the writer drops a
 *                 length-0 header at the current position telling the
 *                 reader to skip to offset 0.
 *   origin        msg_origin_t.
 *   sequence      shared monotonic message id.
 *   timestamp_ns  shared wall-clock timestamp.
 *
 * The envelope bytes follow immediately, padded up to 8 bytes so the next
 * entry header stays 8-byte aligned.
 */
typedef struct {
    uint32_t length;          /* envelope length; 0 = wrap-to-start sentinel */
    uint32_t origin;          /* msg_origin_t */
    uint64_t sequence;
    uint64_t timestamp_ns;
    /* envelope bytes follow, padded to 8 */
} __attribute__((packed)) ring_entry_hdr_t;

/* These two sizes are part of the wire contract shared with consumers
 * (and asserted identically in shm-consumer.c). If either fires, the ring
 * layout has drifted and every SHM reader would misparse — fix the struct,
 * don't change the assert. */
_Static_assert(sizeof(ring_header_t) == RING_HEADER_SIZE, "ring header must be 64 bytes");
_Static_assert(sizeof(ring_entry_hdr_t) == 24, "ring entry header must be 24 bytes");

/* ---------------- epoll dispatch tag ---------------- */

/*
 * ev_kind_t — discriminates what an epoll event's data.ptr points at.
 *
 * The reactor stores a pointer in each epoll registration's data.ptr.
 * To know how to interpret that pointer when the event fires, every
 * pointed-at object begins with an ev_handle_t whose `kind` is one of
 * these. The four EV_LISTEN_* are the singleton listener sockets; the two
 * client kinds tag heap client objects.
 */
typedef enum {
    EV_LISTEN_PUB,    /* TCP publisher listener  (port 12001) */
    EV_LISTEN_SUB,    /* TCP subscriber listener (port 12002) */
    EV_LISTEN_UDP,    /* UDP datagram socket     (port 12003) */
    EV_LISTEN_SHM,    /* Unix-socket SHM handshake listener   */
    EV_CLIENT,        /* a client_t (TCP pub or sub)          */
    EV_SHM_CLIENT,    /* a shm_client_t (sub or producer sock) */
    EV_SHM_PROD_EV,   /* a producer's inbound-ring eventfd     */
} ev_kind_t;

/*
 * ev_handle_t — the common prefix that makes the tagged-dispatch trick
 * work. Both client_t and shm_client_t embed this as their FIRST member,
 * so a `(ev_handle_t *)data.ptr` read is always valid and reveals the
 * concrete type via `kind`. Keeping it first is load-bearing: reorder it
 * and the reactor will read the wrong bytes as the tag.
 */
typedef struct ev_handle {
    ev_kind_t kind;
} ev_handle_t;

/* ---------------- client state ---------------- */

/*
 * client_kind_t — is this TCP connection a publisher (sends frames in) or
 * a subscriber (receives matching frames out)? Decided by which listener
 * accepted it (port 12001 vs 12002), fixed for the connection's life.
 */
typedef enum {
    CK_PUB,   /* producer: streams length-prefixed frames to us */
    CK_SUB,   /* consumer: we stream matching frames to it       */
} client_kind_t;

/*
 * sub_state_t — a subscriber connection is first in SS_HANDSHAKE waiting
 * for its "SUB <topics>\n" line; once parsed and acknowledged it moves to
 * SS_RUNNING and starts receiving fan-out. Publishers ignore this field.
 */
typedef enum {
    SS_HANDSHAKE,   /* awaiting "SUB ..." line */
    SS_RUNNING,     /* subscribed; receiving fan-out */
} sub_state_t;

/*
 * outq_entry_t — one queued, not-yet-fully-written wire frame for a TCP
 * subscriber. Subscribers each own a singly-linked FIFO of these; the
 * frame bytes are stored inline (flexible array member) so each queued
 * frame is a single allocation.
 *
 *   next   next frame in the FIFO, or NULL.
 *   total  total bytes in `frame`.
 *   sent   bytes already written to the socket (partial-write cursor);
 *          the next write starts at frame[sent].
 *   frame  the length-prefixed wire bytes to send.
 */
typedef struct outq_entry {
    struct outq_entry *next;
    size_t   total;   /* total bytes in `frame` */
    size_t   sent;    /* bytes already written */
    uint8_t  frame[]; /* length-prefixed wire frame */
} outq_entry_t;

/*
 * client_t — state for one TCP connection, publisher or subscriber.
 * Allocated on accept, freed on close. Lives in the epoll set keyed by
 * its `ev` prefix.
 *
 * Read side (both kinds use it):
 *   rbuf/rlen/rcap   growable input buffer. Publishers accumulate bytes
 *                    here until a whole length-prefixed frame is present;
 *                    subscribers use it only to read the handshake line.
 *
 * Subscription (subscribers only):
 *   topics/n_topics  the topic patterns from the SUB line (owned strings).
 *   match_all        set if the client subscribed to "**" (everything).
 *
 * Write side (subscribers only):
 *   outq_head/tail   FIFO of pending frames (see outq_entry_t).
 *   outq_bytes       total queued bytes, bounded by OUTQ_MAX_BYTES.
 *   dropped          count of frames discarded by drop-oldest backpressure.
 *   want_write       the epoll event mask currently armed for this fd, so
 *                    update_epoll_mode can skip redundant EPOLL_CTL_MODs.
 *
 *   peer             "ip:port" string for logging.
 */
typedef struct client {
    ev_handle_t   ev;     /* MUST be first — epoll data.ptr looks at this */
    int           fd;
    client_kind_t kind;
    sub_state_t   sub_state;

    /* read buffer */
    uint8_t      *rbuf;
    size_t        rlen;
    size_t        rcap;

    /* sub state */
    char         *topics[MAX_TOPICS_PER_SUB];
    size_t        n_topics;
    int           match_all;

    /* outgoing queue */
    outq_entry_t *outq_head, *outq_tail;
    size_t        outq_bytes;
    uint64_t      dropped;
    int           want_write;

    char          peer[64];
} client_t;

/* ---------------- globals ---------------- */

static int g_epoll_fd = -1;                  /* the single epoll instance */
static volatile sig_atomic_t g_stop = 0;     /* set by SIGINT/SIGTERM      */
static unsigned long long g_pub_msgs = 0;    /* messages accepted (all)    */
static unsigned long long g_pub_bytes = 0;   /* envelope bytes accepted    */
static unsigned long long g_delivered = 0;   /* TCP fan-out deliveries     */

/*
 * g_spin — benchmark mode (env BROKER_SPIN=1). While at least one SHM
 * producer is attached, the reactor uses a zero epoll timeout and
 * drains every producer ring on every loop iteration, instead of
 * sleeping until the producer's eventfd fires. Removes the ~3-8 us
 * eventfd wakeup from the SHM ingest path at the cost of a pegged
 * core. With no producers attached it falls back to normal blocking,
 * so leaving it on for a mostly-TCP broker costs nothing.
 */
static int g_spin = 0;

/*
 * on_sigint — async-signal-safe stop handler. Just flips the g_stop flag;
 * the epoll loop notices on its next wakeup (or its 1s timeout) and exits
 * to run orderly shutdown. We do no real work in signal context.
 */
static void on_sigint(int signo) { (void)signo; g_stop = 1; }

/* ---------------- utility ---------------- */

/*
 * set_nonblock — flip O_NONBLOCK on so socket I/O returns EAGAIN instead
 * of blocking the single reactor thread. Used on every fd we own.
 */
static int set_nonblock(int fd) {
    int fl = fcntl(fd, F_GETFL, 0);
    if (fl < 0) return -1;
    return fcntl(fd, F_SETFL, fl | O_NONBLOCK);
}

/*
 * set_nodelay — disable Nagle on a TCP socket so small frames go out
 * immediately rather than being coalesced. This bus is latency-first, so
 * we always prefer a syscall over a few milliseconds of buffering delay.
 */
static int set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof one);
}

/*
 * listen_tcp — create a non-blocking, loopback-only TCP listener on
 * `port`. Sets SO_REUSEADDR so a quick restart isn't blocked by TIME_WAIT,
 * binds to 127.0.0.1 only (the bus is intentionally not exposed off-host),
 * and returns the listening fd or -1 on any failure (with the error
 * already logged).
 */
static int listen_tcp(int port) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { LOG_ERR("socket: %s", strerror(errno)); return -1; }

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port   = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        LOG_ERR("bind %d: %s", port, strerror(errno));
        close(fd);
        return -1;
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
        LOG_ERR("listen %d: %s", port, strerror(errno));
        close(fd);
        return -1;
    }
    if (set_nonblock(fd) < 0) {
        LOG_ERR("nonblock %d: %s", port, strerror(errno));
        close(fd);
        return -1;
    }
    LOG_INFO("listening 127.0.0.1:%d (TCP)", port);
    return fd;
}

/*
 * bind_udp — create the non-blocking, loopback-only UDP socket on `port`.
 * Like listen_tcp but for datagrams: no listen()/accept(), and we bump
 * SO_RCVBUF to 4 MiB so a bursty producer doesn't overrun the kernel
 * queue between reactor wakeups. Returns the fd or -1.
 */
static int bind_udp(int port) {
    int fd = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { LOG_ERR("udp socket: %s", strerror(errno)); return -1; }

    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one);

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        LOG_ERR("udp bind %d: %s", port, strerror(errno));
        close(fd);
        return -1;
    }
    if (set_nonblock(fd) < 0) {
        LOG_ERR("udp nonblock %d: %s", port, strerror(errno));
        close(fd);
        return -1;
    }
    /* Bump receive buffer for bursty publishers */
    int rcvbuf = 4 * 1024 * 1024;
    setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof rcvbuf);

    LOG_INFO("listening 127.0.0.1:%d (UDP)", port);
    return fd;
}

/*
 * bind_unix — create the Unix-domain stream listener used for the SHM
 * handshake. A consumer connects here, sends its SUB line, and receives
 * the ring memfd + eventfd back over this socket via SCM_RIGHTS. We
 * unlink any stale socket file from a previous run first, and chmod the
 * node to 0600 so only the broker's own user can connect. Returns the
 * listening fd or -1.
 */
static int bind_unix(const char *path) {
    /* Remove any stale socket file from a prior run. */
    unlink(path);

    int fd = socket(AF_UNIX, SOCK_STREAM | SOCK_CLOEXEC, 0);
    if (fd < 0) { LOG_ERR("unix socket: %s", strerror(errno)); return -1; }

    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof addr.sun_path, "%s", path);

    if (bind(fd, (struct sockaddr *)&addr, sizeof addr) < 0) {
        LOG_ERR("unix bind %s: %s", path, strerror(errno));
        close(fd);
        return -1;
    }
    if (listen(fd, LISTEN_BACKLOG) < 0) {
        LOG_ERR("unix listen %s: %s", path, strerror(errno));
        close(fd);
        return -1;
    }
    if (set_nonblock(fd) < 0) {
        LOG_ERR("unix nonblock %s: %s", path, strerror(errno));
        close(fd);
        return -1;
    }
    chmod(path, 0600); /* loopback-only access by current user */
    LOG_INFO("listening %s (SHM handshake)", path);
    return fd;
}

/* ---------------- buffer growth ---------------- */

/*
 * rbuf_grow — ensure a client's read buffer can hold at least `need`
 * bytes, doubling as needed. Capped at FRAME_MAX_LEN + 4 (the largest
 * legal frame plus its length prefix) so a malicious or buggy publisher
 * can't make us allocate without bound. Returns 0 on success, -1 if the
 * request exceeds the cap or the realloc fails (caller closes the client).
 */
static int rbuf_grow(client_t *c, size_t need) {
    if (need <= c->rcap) return 0;             /* already big enough */
    if (need > FRAME_MAX_LEN + 4) return -1;   /* over protocol max  */
    /* Double from the current size (or the READ_CHUNK floor) until
     * it fits, then clamp to the protocol maximum. */
    size_t cap = c->rcap ? c->rcap : READ_CHUNK;
    while (cap < need) cap *= 2;
    if (cap > FRAME_MAX_LEN + 4) cap = FRAME_MAX_LEN + 4;
    uint8_t *nb = realloc(c->rbuf, cap);
    if (!nb) return -1;
    c->rbuf = nb;
    c->rcap = cap;
    return 0;
}

/* ---------------- outq ---------------- */

/*
 * outq_free_all — release a subscriber's entire pending write FIFO and
 * zero its byte counter. Called on close.
 */
static void outq_free_all(client_t *c) {
    outq_entry_t *e = c->outq_head;
    while (e) {
        outq_entry_t *n = e->next;
        free(e);
        e = n;
    }
    c->outq_head = c->outq_tail = NULL;
    c->outq_bytes = 0;
}

/*
 * outq_push — enqueue one wire frame for a subscriber, applying
 * drop-oldest backpressure.
 *
 * If appending this frame would push the queue past OUTQ_MAX_BYTES, we
 * evict from the HEAD (the oldest, least-relevant data) until it fits,
 * bumping the per-client `dropped` counter for each discard. This is the
 * deliberate policy for a real-time sensor bus: a slow consumer should
 * fall behind and lose stale frames, never stall the broker or balloon
 * memory. Returns 0 on success, -1 only if the allocation for the new
 * entry fails.
 */
static int outq_push(client_t *c, const uint8_t *frame, size_t total) {
    /* drop-oldest backpressure */
    while (c->outq_bytes + total > OUTQ_MAX_BYTES && c->outq_head) {
        outq_entry_t *e = c->outq_head;
        c->outq_head = e->next;
        if (!c->outq_head) c->outq_tail = NULL;
        c->outq_bytes -= (e->total - e->sent);
        c->dropped++;
        free(e);
    }
    /* One allocation per frame: struct + bytes inline through the
     * flexible array member. */
    outq_entry_t *e = malloc(sizeof *e + total);
    if (!e) return -1;
    e->next = NULL;
    e->total = total;
    e->sent = 0;
    memcpy(e->frame, frame, total);
    /* FIFO append at the tail. */
    if (c->outq_tail) c->outq_tail->next = e;
    else c->outq_head = e;
    c->outq_tail = e;
    c->outq_bytes += total;
    return 0;
}

/*
 * try_flush — write as much of a subscriber's queued FIFO to its socket
 * as the kernel will currently accept. Pops fully-sent entries; on a
 * partial write advances the head entry's `sent` cursor and stops. An
 * EAGAIN/EWOULDBLOCK means the socket buffer is full — we return 0 and
 * wait for the next EPOLLOUT. Returns 0 if the queue is drained or merely
 * blocked, -1 on a real socket error (caller closes the client).
 */
static int try_flush(client_t *c) {
    while (c->outq_head) {
        outq_entry_t *e = c->outq_head;
        /* Resume at e->sent — the partial-write cursor. */
        ssize_t n = write(c->fd, e->frame + e->sent, e->total - e->sent);
        if (n > 0) {
            e->sent += (size_t)n;
            c->outq_bytes -= (size_t)n;
            if (e->sent == e->total) {
                c->outq_head = e->next;
                if (!c->outq_head) c->outq_tail = NULL;
                free(e);
            }
            continue;
        }
        if (n < 0) {
            /* Socket buffer full: stop here, EPOLLOUT resumes us. */
            if (errno == EAGAIN || errno == EWOULDBLOCK) return 0;
            return -1;                  /* real error — close client */
        }
        return -1; /* n == 0: write returned 0 unexpectedly */
    }
    return 0;
}

/*
 * update_epoll_mode — arm EPOLLOUT only while there is queued data.
 *
 * We watch EPOLLIN always, and add EPOLLOUT exactly when the FIFO is
 * non-empty (so the kernel tells us when the socket is writable again).
 * The cached `want_write` lets us skip the EPOLL_CTL_MOD syscall when the
 * desired mask hasn't changed, which matters on the hot path.
 */
static void update_epoll_mode(client_t *c) {
    int want = c->outq_head ? (EPOLLIN | EPOLLOUT) : EPOLLIN;
    if (want == c->want_write) return;
    struct epoll_event ev = { .events = (uint32_t)want, .data.ptr = c };
    if (epoll_ctl(g_epoll_fd, EPOLL_CTL_MOD, c->fd, &ev) == 0) c->want_write = want;
}

/* ---------------- client lifecycle ---------------- */

/*
 * client_new — allocate and initialize a client_t for a freshly accepted
 * TCP connection. Sets the ev tag to EV_CLIENT, records the peer "ip:port"
 * for logs, and pre-grows the read buffer to READ_CHUNK. Subscribers start
 * in SS_HANDSHAKE. Returns NULL (and the caller closes the fd) on OOM.
 */
static client_t *client_new(int fd, client_kind_t kind, const struct sockaddr_in *sa) {
    /* calloc zeroes every field: counters, queue pointers, topics. */
    client_t *c = calloc(1, sizeof *c);
    if (!c) return NULL;
    c->ev.kind = EV_CLIENT;
    c->fd = fd;
    c->kind = kind;
    c->sub_state = SS_HANDSHAKE;

    /* Render "ip:port" once now so logging never re-does it. */
    char ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &sa->sin_addr, ip, sizeof ip);
    snprintf(c->peer, sizeof c->peer, "%s:%u", ip, ntohs(sa->sin_port));

    if (rbuf_grow(c, READ_CHUNK) < 0) { free(c); return NULL; }
    return c;
}

/* Forward declaration: subs_add (below) closes on a full table. */
static void client_close(client_t *c);

/* ---------------- subscriber registry (fixed-size array) ---------------- */

/*
 * The set of running TCP subscribers, held in a flat fixed-size array
 * rather than a list so fan-out is a tight linear scan over contiguous
 * pointers. Removal is swap-with-last (order doesn't matter), which keeps
 * the array dense — note fanout() must NOT advance its index on a removal
 * because the slot it just looked at now holds a different client.
 */
#define MAX_SUBS 256
static client_t *g_subs[MAX_SUBS];
static size_t    g_subs_count = 0;

/*
 * subs_add — register a subscriber for fan-out. If the table is full the
 * client is logged and closed rather than silently leaked.
 */
static void subs_add(client_t *c) {
    if (g_subs_count >= MAX_SUBS) {
        LOG_WARN("sub table full; dropping %s", c->peer);
        client_close(c);
        return;
    }
    g_subs[g_subs_count++] = c;
}
/*
 * subs_remove — drop a subscriber from the registry by swapping the last
 * entry into its slot. O(n) find, O(1) removal; safe to call on a client
 * that isn't registered (no-op).
 */
static void subs_remove(client_t *c) {
    for (size_t i = 0; i < g_subs_count; i++) {
        if (g_subs[i] == c) {
            g_subs[i] = g_subs[--g_subs_count];
            return;
        }
    }
}

/*
 * client_close — fully tear down a TCP client: deregister it from the
 * subscriber table (if it was one), remove its fd from epoll, close the
 * socket, and free the read buffer, topic strings, and output FIFO. Logs
 * the disconnect with its lifetime drop count. After this returns the
 * pointer is dangling — callers must not touch it.
 */
static void client_close(client_t *c) {
    if (c->kind == CK_SUB) subs_remove(c);  /* stop fan-out first    */
    epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, c->fd, NULL); /* then events */
    close(c->fd);
    free(c->rbuf);
    for (size_t i = 0; i < c->n_topics; i++) free(c->topics[i]);
    outq_free_all(c);
    LOG_INFO("disconnect %s%s (dropped=%llu)",
             c->kind == CK_PUB ? "PUB " : "SUB ",
             c->peer, (unsigned long long)c->dropped);
    free(c);
}

/* ---------------- SHM client state (subscribers + producers) ----- */

struct shm_client;   /* forward — shm_prod_ev_t points back at it */

/*
 * shm_prod_ev_t — a second epoll tag embedded in producer clients.
 *
 * A producer's inbound-ring eventfd is its own fd in the epoll set,
 * separate from the handshake socket, so it needs its own data.ptr.
 * This wrapper carries the EV_SHM_PROD_EV tag plus a back-pointer to
 * the owning client; dispatch casts to shm_prod_ev_t and follows
 * `owner`. (Same tagged-pointer idiom as ev_handle_t — the tag must
 * stay the first member.)
 */
typedef struct shm_prod_ev {
    ev_handle_t        ev;      /* MUST be first; EV_SHM_PROD_EV   */
    struct shm_client *owner;
} shm_prod_ev_t;

/*
 * shm_client_t — state for one shared-memory peer: a subscriber OR,
 * since phase 5, a producer (is_producer distinguishes them; both
 * arrive through the same Unix-socket handshake).
 *
 * Subscriber: the broker never streams bytes to this client. After
 * the handshake the only thing flowing over sock_fd is connection
 * liveness (a hangup tells us to clean up). Real delivery is the broker
 * writing entries into the shared `ring` and bumping the eventfd.
 *
 * Producer: the data direction reverses — the CLIENT writes `ring`
 * and bumps the eventfd; the broker reads entries (shm_producer_drain)
 * and publishes its read cursor into the ring header for flow control.
 * Producer-only fields:
 *   is_producer      role flag; set by the "PUB" handshake.
 *   prod_ev          epoll tag for the eventfd (see shm_prod_ev_t).
 *   prod_read        broker's absolute read cursor into the ring.
 *   prod_msgs        envelopes accepted from this producer.
 *   prod_bad         envelopes skipped (bad magic / topic bounds).
 *   prod_resyncs     overrun/corruption resyncs (each loses data).
 *
 *   ev               dispatch tag (EV_SHM_CLIENT); MUST be first.
 *   sock_fd          Unix stream socket — handshake + hangup detection.
 *   memfd            the ring's memfd. Broker keeps its own copy; the
 *                    client got a duplicate via SCM_RIGHTS. The mapping
 *                    survives as long as either side holds an fd/mapping.
 *   eventfd_fd       wakeup fd: broker writes, client blocks reading it.
 *   ring             the broker's own mmap of the ring (where it writes).
 *   ring_size        bytes mapped (= RING_SIZE).
 *   rline/rlen       buffer for the single "SUB ...\n" handshake line.
 *   handshake_done   1 once the ring exists and FDs have been sent; until
 *                    then fan-out skips this client.
 *   topics/n_topics/match_all   subscription, same semantics as TCP.
 *   peer             "shm-<fd>" label for logging.
 *   delivered/dropped  per-client counters (dropped = oversized entries).
 */
typedef struct shm_client {
    ev_handle_t  ev;        /* MUST be first; kind = EV_SHM_CLIENT */
    int          sock_fd;   /* Unix stream socket to client */
    int          memfd;     /* ring memfd (broker holds, client received copy) */
    int          eventfd_fd; /* wakeup fd (broker writes, client reads) */
    void        *ring;      /* mmap'd ring in broker's address space */
    size_t       ring_size; /* total mmap size (=RING_SIZE) */
    char         rline[SUB_LINE_MAX]; /* handshake line buffer */
    size_t       rlen;
    int          handshake_done;
    char        *topics[MAX_TOPICS_PER_SUB];
    size_t       n_topics;
    int          match_all;
    char         peer[64];
    uint64_t     delivered;
    uint64_t     dropped;

    /* producer role (phase 5) */
    int           is_producer;
    shm_prod_ev_t prod_ev;
    uint64_t      prod_read;
    uint64_t      prod_msgs;
    uint64_t      prod_bad;
    uint64_t      prod_resyncs;
} shm_client_t;

/* Forward decl — defined with the other ingest pumps below; the close
 * path calls it to flush a departing producer's tail. */
static void shm_producer_drain(shm_client_t *p);

/* SHM subscriber registry — same flat-array design as the TCP one. */
#define MAX_SHM_CLIENTS MAX_SHM_SUBS
static shm_client_t *g_shm_subs[MAX_SHM_CLIENTS];
static size_t g_shm_subs_count = 0;

/*
 * shm_subs_add — register a completed SHM subscriber for fan-out. Full
 * table -> logged and dropped (the connection is left to be cleaned up).
 */
static void shm_subs_add(shm_client_t *c) {
    if (g_shm_subs_count >= MAX_SHM_CLIENTS) {
        LOG_WARN("SHM sub table full; dropping %s", c->peer);
        return;
    }
    g_shm_subs[g_shm_subs_count++] = c;
}

/*
 * shm_subs_remove — swap-with-last removal from the SHM registry; no-op
 * if not present.
 */
static void shm_subs_remove(shm_client_t *c) {
    for (size_t i = 0; i < g_shm_subs_count; i++) {
        if (g_shm_subs[i] == c) {
            g_shm_subs[i] = g_shm_subs[--g_shm_subs_count];
            return;
        }
    }
}

/* SHM producer registry — same flat-array pattern. The spin-mode
 * reactor walks this every loop iteration, so density matters. */
static shm_client_t *g_shm_prods[MAX_SHM_PRODS];
static size_t g_shm_prods_count = 0;

/* shm_prods_add — register a completed producer for ring draining. */
static int shm_prods_add(shm_client_t *c) {
    if (g_shm_prods_count >= MAX_SHM_PRODS) {
        LOG_WARN("SHM producer table full; rejecting %s", c->peer);
        return -1;
    }
    g_shm_prods[g_shm_prods_count++] = c;
    return 0;
}

/* shm_prods_remove — swap-with-last removal; no-op if absent. */
static void shm_prods_remove(shm_client_t *c) {
    for (size_t i = 0; i < g_shm_prods_count; i++) {
        if (g_shm_prods[i] == c) {
            g_shm_prods[i] = g_shm_prods[--g_shm_prods_count];
            return;
        }
    }
}

/*
 * shm_client_close — tear down a SHM peer (subscriber or producer).
 *
 * For a producer we first drain whatever is still unread in its ring —
 * the SHM mirror of the TCP EOF-flush rule: a producer that writes its
 * tail and exits must not lose it. Then both roles converge: deregister,
 * remove fds from epoll (a producer's eventfd has its own registration),
 * close them, unmap the ring, free owned strings. The client's own fd
 * copies keep its mapping alive on its side until it exits. Pointer is
 * dangling afterward.
 */
static void shm_client_close(shm_client_t *c) {
    if (c->is_producer) {
        if (c->ring) shm_producer_drain(c);      /* flush the tail   */
        shm_prods_remove(c);
        if (c->eventfd_fd >= 0)
            epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, c->eventfd_fd, NULL);
    }
    shm_subs_remove(c);
    if (c->sock_fd >= 0) {
        epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, c->sock_fd, NULL);
        close(c->sock_fd);
    }
    if (c->ring) munmap(c->ring, c->ring_size);
    if (c->memfd >= 0) close(c->memfd);
    if (c->eventfd_fd >= 0) close(c->eventfd_fd);
    for (size_t i = 0; i < c->n_topics; i++) free(c->topics[i]);
    if (c->is_producer)
        LOG_INFO("disconnect SHM PUB %s (msgs=%llu bad=%llu resyncs=%llu)",
                 c->peer, (unsigned long long)c->prod_msgs,
                 (unsigned long long)c->prod_bad,
                 (unsigned long long)c->prod_resyncs);
    else
        LOG_INFO("disconnect SHM %s (delivered=%llu dropped=%llu)",
                 c->peer, (unsigned long long)c->delivered,
                 (unsigned long long)c->dropped);
    free(c);
}

/*
 * shm_topic_matches — does this SHM subscriber want a message on `topic`?
 * Identical rules to topic_matches() (exact, "prefix.*" wildcard, or the
 * match_all "**" flag). Kept as a separate copy because shm_client_t and
 * client_t are distinct types; the two must be edited in lockstep.
 */
static int shm_topic_matches(const shm_client_t *c, const char *topic, size_t topic_len) {
    if (c->match_all) return 1;
    for (size_t i = 0; i < c->n_topics; i++) {
        const char *t = c->topics[i];
        size_t tl = strlen(t);
        if (tl == topic_len && memcmp(t, topic, topic_len) == 0) return 1;
        if (tl >= 2 && t[tl - 1] == '*' && t[tl - 2] == '.' &&
            topic_len >= tl - 1 && memcmp(t, topic, tl - 1) == 0)
            return 1;
    }
    return 0;
}

/*
 * ring_write_one — publish one envelope into a single subscriber's ring.
 * This is the shared-memory fast path; it runs once per matching SHM
 * subscriber per message.
 *
 * The whole correctness argument lives in the ordering of three steps:
 *
 *   1. Compute where the entry goes. write_idx is absolute, so the
 *      physical slot is `write_idx % RING_DATA_SIZE`. If the entry can't
 *      fit between that slot and end-of-ring, write a length-0 sentinel
 *      header at the slot and advance the cursor to the ring start — the
 *      reader sees the sentinel and skips forward identically.
 *   2. Copy the entry header and envelope bytes into the ring. These are
 *      plain stores; they are NOT yet visible-as-published.
 *   3. Release-store the advanced write_idx. The release barrier
 *      guarantees that any reader doing an acquire-load of write_idx and
 *      seeing the new value also sees all the entry bytes from step 2.
 *      Only after this does the entry "exist" for the reader.
 *
 * Finally we bump the eventfd to wake a blocked reader. The eventfd is a
 * counter; if the write returns EAGAIN the counter is already saturated
 * and the reader will re-scan write_idx anyway, so we ignore it.
 *
 * Oversized envelopes (larger than the whole data region) can never fit
 * and are counted as dropped. There is no per-entry overwrite protection:
 * a reader too far behind will have its unread bytes overwritten — that
 * is the intended "newest data wins" behavior for a real-time bus.
 */
static void ring_write_one(shm_client_t *c,
                           const uint8_t *envelope, size_t env_len,
                           msg_origin_t origin, uint64_t sequence, uint64_t ts_ns) {
    ring_header_t *hdr = (ring_header_t *)c->ring;
    uint8_t *data = (uint8_t *)c->ring + RING_HEADER_SIZE;

    /* Entry = 24-byte header + envelope padded to 8. Drop if it could
     * never fit in the ring at all. */
    size_t padded = (env_len + 7u) & ~((size_t)7u);
    size_t entry_total = sizeof(ring_entry_hdr_t) + padded;
    if (entry_total > RING_DATA_SIZE) { c->dropped++; return; }

    /* Relaxed load is fine: the broker is the sole writer, so it always
     * has the freshest write_idx; ordering only matters for the reader. */
    uint64_t cur = __atomic_load_n(&hdr->write_idx, __ATOMIC_RELAXED);
    size_t pos = (size_t)(cur % RING_DATA_SIZE);

    /* Need to wrap if the entry won't fit before end-of-ring. */
    if (pos + entry_total > RING_DATA_SIZE) {
        ring_entry_hdr_t sentinel = {0}; /* length=0 marks "skip to start" */
        memcpy(data + pos, &sentinel, sizeof sentinel);
        /* Account the skipped tail in the absolute cursor so the reader's
         * "skip to start" math (RING_DATA_SIZE - pos) lines up exactly. */
        size_t skipped = RING_DATA_SIZE - pos;
        cur += skipped;
        pos = 0;
    }

    ring_entry_hdr_t eh = {0};
    eh.length = (uint32_t)env_len;
    eh.origin = (uint32_t)origin;
    eh.sequence = sequence;
    eh.timestamp_ns = ts_ns;
    memcpy(data + pos, &eh, sizeof eh);
    memcpy(data + pos + sizeof eh, envelope, env_len);

    /* Publish: release-store on write_idx so the reader's acquire-load is
     * guaranteed to see the entry bytes before the new write_idx value. */
    __atomic_store_n(&hdr->write_idx, cur + entry_total, __ATOMIC_RELEASE);

    /* Wake the reader. eventfd in counter mode just increments; EAGAIN means
     * the counter is saturated and the reader will catch up regardless. */
    uint64_t one = 1;
    ssize_t w = write(c->eventfd_fd, &one, sizeof one);
    (void)w;

    c->delivered++;
}

/*
 * shm_fanout — deliver one envelope to every matching SHM subscriber.
 * Linear scan over the registry; skips clients still mid-handshake or
 * whose subscription doesn't match the topic, and calls ring_write_one
 * for the rest. No index trickery here because ring_write_one never
 * removes clients from the registry (unlike the TCP fanout path).
 */
static void shm_fanout(const uint8_t *envelope, size_t env_len,
                       const char *topic, size_t topic_len,
                       msg_origin_t origin, uint64_t sequence, uint64_t ts_ns) {
    for (size_t i = 0; i < g_shm_subs_count; ) {
        shm_client_t *s = g_shm_subs[i];
        if (!s->handshake_done) { i++; continue; }
        if (!shm_topic_matches(s, topic, topic_len)) { i++; continue; }
        ring_write_one(s, envelope, env_len, origin, sequence, ts_ns);
        i++;
    }
}

/* ---------------- topic matching ---------------- */

/*
 * topic_matches — does TCP subscriber `c` want a message published on
 * `topic` (length `topic_len`, not NUL-terminated)? Three ways to match:
 *
 *   - match_all: the client subscribed to "**" and gets everything.
 *   - exact: a subscribed topic equals the message topic byte-for-byte.
 *   - prefix wildcard: a subscribed pattern ending in ".*" matches any
 *     topic sharing the part before the star, e.g. "chat.pac.*" matches
 *     "chat.pac.anything". Only this single trailing-segment form is
 *     supported — there is no general glob.
 *
 * Returns 1 on the first match, 0 if none. (shm_topic_matches is the twin
 * of this function for SHM clients.)
 */
static int topic_matches(const client_t *c, const char *topic, size_t topic_len) {
    if (c->match_all) return 1;
    for (size_t i = 0; i < c->n_topics; i++) {
        const char *t = c->topics[i];
        size_t tl = strlen(t);
        if (tl == topic_len && memcmp(t, topic, topic_len) == 0) return 1;
        /* prefix wildcard:   "chat.pac.*" matches "chat.pac.<anything>" */
        if (tl >= 2 && t[tl - 1] == '*' && t[tl - 2] == '.' &&
            topic_len >= tl - 1 && memcmp(t, topic, tl - 1) == 0)
            return 1;
    }
    return 0;
}

/* ---------------- SUB handshake ---------------- */

/*
 * send_all_now — blocking write of a short control string (the "OK\n" /
 * "ERR ...\n" handshake replies). Loops over partial writes and spins on
 * EAGAIN; only used for tiny payloads at handshake time, never on the
 * data hot path, so the busy-wait is acceptable. Returns 0 / -1.
 */
static int send_all_now(int fd, const char *s) {
    size_t len = strlen(s);
    size_t off = 0;
    while (off < len) {
        ssize_t n = write(fd, s + off, len - off);
        if (n > 0) { off += (size_t)n; continue; }
        if (n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) continue;
        return -1;
    }
    return 0;
}

/*
 * parse_sub_line — parse a "SUB <topic> [<topic> ...]" line into the
 * client's topic list. Splits on spaces (after the "SUB" keyword), copies
 * each token into an owned string, and sets match_all if the special "**"
 * token appears. Caps at MAX_TOPICS_PER_SUB. Returns 0 if at least one
 * topic was parsed, -1 on a malformed line or allocation failure. The
 * buffer is modified in place only by length trimming. (Twin:
 * parse_shm_sub_line.)
 */
static int parse_sub_line(client_t *c, char *line, size_t len) {
    /* Trim trailing CR/spaces so "SUB foo\r" parses like "SUB foo". */
    while (len > 0 && (line[len - 1] == '\r' || line[len - 1] == ' ')) len--;
    /* Must begin with the literal keyword. */
    if (len < 3 || memcmp(line, "SUB", 3) != 0) return -1;
    char *p = line + 3;
    char *end = line + len;
    while (p < end) {
        while (p < end && *p == ' ') p++;     /* skip separators     */
        if (p >= end) break;
        char *tok = p;                        /* token start         */
        while (p < end && *p != ' ') p++;     /* advance to its end  */
        size_t tl = (size_t)(p - tok);
        if (tl == 0) continue;
        if (c->n_topics >= MAX_TOPICS_PER_SUB) return -1;
        /* Copy into an owned NUL-terminated string the client keeps
         * until close. */
        char *t = malloc(tl + 1);
        if (!t) return -1;
        memcpy(t, tok, tl);
        t[tl] = '\0';
        /* "**" flips the match-everything flag; matching then short-
         * circuits before ever consulting the stored patterns. */
        if (tl == 2 && t[0] == '*' && t[1] == '*') c->match_all = 1;
        c->topics[c->n_topics++] = t;
    }
    return c->n_topics > 0 ? 0 : -1;
}

/*
 * handle_sub_handshake — drive a TCP subscriber through its one-line
 * handshake. Called whenever handshake-state bytes arrive.
 *
 * Scans the read buffer for a newline; until one appears we just wait for
 * more bytes (returning 0). Once a full line is present we parse it,
 * consume it from the buffer, and either:
 *   - reply "ERR ...\n" and return -1 (caller closes), or
 *   - reply "OK\n", flip the client to SS_RUNNING, register it for
 *     fan-out via subs_add, log the subscription, and return 0.
 *
 * Over-long lines (no newline within SUB_LINE_MAX) are rejected to bound
 * buffering. Returns 0 to keep the connection, -1 to close it.
 */
static int handle_sub_handshake(client_t *c) {
    /* Look for a newline in the read buffer */
    for (size_t i = 0; i < c->rlen; i++) {
        if (c->rbuf[i] == '\n') {
            /* Full line found: rbuf[0..i). Parse it, then consume it
             * (plus the newline) before acting on the result. */
            char *line = (char *)c->rbuf;
            size_t llen = i;
            int ok = parse_sub_line(c, line, llen) == 0;
            /* Consume the line */
            size_t consumed = i + 1;
            memmove(c->rbuf, c->rbuf + consumed, c->rlen - consumed);
            c->rlen -= consumed;

            if (!ok) {
                send_all_now(c->fd, "ERR bad SUB line\n");
                return -1;
            }
            if (send_all_now(c->fd, "OK\n") < 0) return -1;
            c->sub_state = SS_RUNNING;
            subs_add(c);          /* visible to fanout from now on   */
            /* Comma-join the topics purely for one tidy log line. */
            char topics_log[256] = {0};
            size_t off = 0;
            for (size_t k = 0; k < c->n_topics && off < sizeof topics_log - 1; k++) {
                int w = snprintf(topics_log + off, sizeof topics_log - off,
                                 "%s%s", k ? "," : "", c->topics[k]);
                if (w < 0) break;
                off += (size_t)w;
            }
            LOG_INFO("SUB OK %s topics=[%s]%s",
                     c->peer, topics_log, c->match_all ? " (match_all)" : "");
            return 0;
        }
        /* No newline within the legal limit — reject to bound RAM. */
        if (i >= SUB_LINE_MAX) {
            send_all_now(c->fd, "ERR SUB line too long\n");
            return -1;
        }
    }
    if (c->rlen >= SUB_LINE_MAX) {
        send_all_now(c->fd, "ERR SUB line too long\n");
        return -1;
    }
    return 0; /* need more bytes */
}

/* ---------------- fanout ---------------- */

/*
 * fanout — deliver one already-framed message to every matching TCP
 * subscriber.
 *
 *   length_prefixed  the complete wire frame (4-byte BE length + envelope)
 *                    ready to copy straight into a subscriber's queue.
 *   total            its size.
 *   topic/topic_len  the topic extracted from the envelope, for matching.
 *
 * For each running subscriber whose subscription matches, we enqueue a
 * copy (outq_push) and arm EPOLLOUT. A failed enqueue (OOM) closes that
 * subscriber. Note the manual index handling: outq_push's drop-oldest
 * never removes a client, but the OOM path calls client_close which
 * swap-compacts the array, so on that branch we must NOT advance `i`.
 */
static void fanout(const uint8_t *length_prefixed, size_t total,
                   const char *topic, size_t topic_len) {
    for (size_t i = 0; i < g_subs_count; ) {
        client_t *s = g_subs[i];
        if (s->sub_state != SS_RUNNING) { i++; continue; }
        if (!topic_matches(s, topic, topic_len)) { i++; continue; }
        if (outq_push(s, length_prefixed, total) < 0) {
            LOG_WARN("outq alloc failed for %s; closing", s->peer);
            subs_remove(s);
            client_close(s);
            continue; /* don't increment — array compacted */
        }
        update_epoll_mode(s);
        g_delivered++;
        i++;
    }
}

/* ---------------- PUB frame parsing ---------------- */

/*
 * handle_pub_data — parse and dispatch all complete frames currently
 * buffered from a TCP publisher.
 *
 * Wire format per frame: a 4-byte big-endian length followed by that many
 * envelope bytes. We loop peeling frames until the buffer holds only a
 * partial one:
 *
 *   - <4 bytes buffered: need more, return 0.
 *   - length 0 or > FRAME_MAX_LEN: protocol violation, return -1 (close).
 *   - whole frame not yet buffered: grow the buffer to fit and return 0.
 *   - whole frame present: validate the zmesg magic, read the topic_len
 *     from inside the envelope, then assign ONE shared (sequence,
 *     timestamp) and route the message three ways — wal_append (durable),
 *     fanout (TCP subs), shm_fanout (SHM subs) — before consuming the
 *     frame from the buffer.
 *
 * A bad magic closes the connection (strict protocol); a too-short or
 * inconsistent envelope is dropped but the connection survives. Returns 0
 * to keep reading, -1 to close.
 */
static int handle_pub_data(client_t *c) {
    for (;;) {
        /* Step 1: need the 4-byte big-endian length prefix. */
        if (c->rlen < 4) return 0;
        uint32_t net_len;
        memcpy(&net_len, c->rbuf, 4);   /* memcpy = alignment-safe   */
        uint32_t flen = ntohl(net_len);
        if (flen == 0 || flen > FRAME_MAX_LEN) {
            LOG_WARN("PUB %s bogus length %u; closing", c->peer, flen);
            return -1;
        }
        /* Step 2: whole frame buffered yet? If not, make sure there
         * is room for it, then wait for more bytes. */
        if (c->rlen < 4 + flen) {
            if (rbuf_grow(c, 4 + flen) < 0) return -1;
            return 0; /* need more bytes */
        }
        /* zmesg envelope starts at c->rbuf + 4 */
        if (flen < ZMESG_FIXED_HDR) {
            LOG_WARN("PUB %s envelope too short (%u)", c->peer, flen);
            /* still pass it through? no — strict protocol. drop frame. */
        } else {
            /* Step 3: validate and route. Envelope begins at +4. */
            uint32_t magic;
            memcpy(&magic, c->rbuf + 4, 4);
            if (magic != ZMESG_MAGIC) {
                LOG_WARN("PUB %s bad magic 0x%08x; closing", c->peer, magic);
                return -1;
            }
            uint16_t topic_len;
            memcpy(&topic_len, c->rbuf + 4 + ZMESG_OFF_TOPIC_LEN, 2);
            if ((uint32_t)ZMESG_FIXED_HDR + topic_len > flen) {
                LOG_WARN("PUB %s topic_len %u exceeds frame %u",
                         c->peer, topic_len, flen);
            } else {
                const char *topic = (const char *)(c->rbuf + 4 + ZMESG_FIXED_HDR);
                /* Assign one sequence + timestamp shared by WAL and fan-out
                 * so observers across transports see the same identifiers. */
                g_sequence++;
                uint64_t seq = g_sequence;
                uint64_t ts = now_ns_real();
                wal_append(&g_wal, c->rbuf + 4, flen, ORIGIN_TCP, seq, ts);
                /* SHM before TCP: the ring is the latency-critical
                 * tier (ns/us targets), while TCP subscribers queue
                 * through the kernel anyway. Ordering the ring write
                 * first keeps per-TCP-subscriber work (mallocs,
                 * memcpys) out of the SHM hop. Measured 2026-06-11:
                 * with a "**" TCP sub attached, ring-hop p50 dropped
                 * from ~1.2 us to ~0.3 us after this swap. */
                shm_fanout(c->rbuf + 4, flen, topic, topic_len, ORIGIN_TCP, seq, ts);
                fanout(c->rbuf, 4 + flen, topic, topic_len);
                g_pub_msgs++;
                g_pub_bytes += flen;
            }
        }
        /* Step 4: slide any following bytes down and loop — the
         * buffer may hold several complete frames per wakeup. */
        memmove(c->rbuf, c->rbuf + 4 + flen, c->rlen - (4 + flen));
        c->rlen -= 4 + flen;
    }
}

/* ---------------- UDP datagram pump ---------------- */

static unsigned long long g_udp_msgs = 0;   /* messages that arrived via UDP */

/*
 * udp_drain — read and dispatch every queued UDP datagram.
 *
 * UDP framing is implicit: one datagram is exactly one zmesg envelope (no
 * length prefix — the datagram boundary frames it). We recvfrom() in a
 * loop until EAGAIN, and for each datagram validate the magic, pull out
 * the topic, assign the shared (sequence, timestamp), and route it to the
 * WAL, TCP subscribers, and SHM subscribers.
 *
 * To reach TCP subscribers (which speak length-prefixed framing) we
 * re-frame the datagram into a static staging buffer by prepending its
 * 4-byte BE length. SHM subscribers take the raw envelope directly. Bad
 * or short datagrams are logged and skipped; there is no connection to
 * close. Uses static buffers because it's single-threaded and they'd be
 * too large for the stack.
 */
static void udp_drain(int fd) {
    static uint8_t recv_buf[UDP_RECV_MAX];
    /* Length-prefixed TCP frame staging: 4 + UDP_RECV_MAX. */
    static uint8_t tcp_frame[4 + UDP_RECV_MAX];

    for (;;) {
        struct sockaddr_in sa;
        socklen_t sl = sizeof sa;
        ssize_t n = recvfrom(fd, recv_buf, sizeof recv_buf, 0,
                             (struct sockaddr *)&sa, &sl);
        if (n < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            LOG_WARN("udp recvfrom: %s", strerror(errno));
            return;
        }
        /* Validation mirrors the TCP path (size, magic, topic bounds)
         * minus framing — the datagram boundary IS the frame. */
        if (n < (ssize_t)ZMESG_FIXED_HDR) {
            LOG_WARN("UDP datagram too short (%zd)", n);
            continue;
        }
        uint32_t magic;
        memcpy(&magic, recv_buf, 4);
        if (magic != ZMESG_MAGIC) {
            LOG_WARN("UDP bad magic 0x%08x", magic);
            continue;
        }
        uint16_t topic_len;
        memcpy(&topic_len, recv_buf + ZMESG_OFF_TOPIC_LEN, 2);
        if ((uint32_t)ZMESG_FIXED_HDR + topic_len > (uint32_t)n) {
            LOG_WARN("UDP topic_len %u exceeds datagram %zd", topic_len, n);
            continue;
        }
        const char *topic = (const char *)(recv_buf + ZMESG_FIXED_HDR);

        /* Same one-identity rule as the TCP path: a single (seq, ts)
         * pair shared by the WAL, TCP fan-out, and SHM fan-out. */
        g_sequence++;
        uint64_t seq = g_sequence;
        uint64_t ts = now_ns_real();
        wal_append(&g_wal, recv_buf, (size_t)n, ORIGIN_UDP, seq, ts);

        /* SHM first — same latency-tiering as handle_pub_data. */
        shm_fanout(recv_buf, (size_t)n, topic, topic_len, ORIGIN_UDP, seq, ts);

        /* Re-frame for TCP fan-out: 4-byte BE length + datagram bytes. */
        uint32_t net_len = htonl((uint32_t)n);
        memcpy(tcp_frame, &net_len, 4);
        memcpy(tcp_frame + 4, recv_buf, (size_t)n);
        fanout(tcp_frame, 4 + (size_t)n, topic, topic_len);

        g_pub_msgs++;
        g_udp_msgs++;
        g_pub_bytes += (uint32_t)n;
    }
}

/* ---------------- SHM ingest pump (phase 5) ---------------- */

static unsigned long long g_shm_msgs = 0;  /* messages in via SHM rings */

/*
 * shm_producer_drain — consume every entry a SHM producer has
 * published since our last visit, routing each like any other ingest.
 *
 * The third ingest pump, sibling to handle_pub_data (TCP) and
 * udp_drain (UDP). Reading side of the inbound-ring contract:
 *
 *   1. Acquire-load write_idx. Seeing it guarantees (release/acquire
 *      pairing) that all entry bytes it covers are visible.
 *   2. Walk entries from our cursor (prod_read) to write_idx. The
 *      walk mirrors the consumer walk in shm-consumer.c: a length-0
 *      header is the wrap sentinel; everything else is
 *      [24-byte hdr][envelope][pad to 8].
 *   3. Each VALID envelope gets the standard treatment: one fresh
 *      global (sequence, timestamp), then WAL -> SHM fan-out -> TCP
 *      fan-out (the sink-order invariant), origin ORIGIN_SHM. The
 *      producer's own sequence/timestamp_ns header fields are its
 *      business — the broker assigns bus identity at accept, same as
 *      every other transport.
 *   4. Release-store the advanced cursor into header reserved[0].
 *      That is the producer's flow-control signal: it must never let
 *      write_idx run more than RING_DATA_SIZE past this value, which
 *      also guarantees nothing we are reading in step 2/3 can be
 *      overwritten underneath us (the cursor only moves after we are
 *      done with those bytes).
 *
 * Error containment, mildest to harshest:
 *   - bad envelope (short / wrong magic / topic overrun): the entry
 *     HEADER is still trusted, so we skip exactly that entry and
 *     count prod_bad — one bad message never desyncs the stream;
 *   - insane entry header (length > FRAME_MAX_LEN, or an entry that
 *     crosses end-of-ring where only a sentinel may live): framing
 *     itself is untrustworthy, so resync the cursor to write_idx and
 *     count prod_resyncs (data loss, logged);
 *   - lapped (write_idx - cursor > RING_DATA_SIZE, i.e. the producer
 *     ignored flow control): same resync, because the bytes under
 *     the cursor are already overwritten.
 *
 * Outer loop re-loads write_idx after each batch so a producer that
 * keeps publishing while we drain is caught up with in one call.
 */
static void shm_producer_drain(shm_client_t *p) {
    ring_header_t *hdr = (ring_header_t *)p->ring;
    const uint8_t *data = (const uint8_t *)p->ring + RING_HEADER_SIZE;
    /* Length-prefixed staging for TCP fan-out, like udp_drain's. */
    static uint8_t tcp_frame[4 + FRAME_MAX_LEN];

    for (;;) {
        uint64_t widx = __atomic_load_n(&hdr->write_idx, __ATOMIC_ACQUIRE);
        if (p->prod_read >= widx) break;          /* caught up        */

        if (widx - p->prod_read > RING_DATA_SIZE) {
            /* Lapped: the producer broke flow control. Everything
             * between cursor and (widx - RING_DATA_SIZE) is gone. */
            LOG_WARN("SHM PUB %s overran broker by %llu bytes; resync",
                     p->peer,
                     (unsigned long long)(widx - p->prod_read
                                          - RING_DATA_SIZE));
            p->prod_resyncs++;
            p->prod_read = widx;
            break;
        }

        while (p->prod_read < widx) {
            size_t pos = (size_t)(p->prod_read % RING_DATA_SIZE);
            ring_entry_hdr_t eh;
            memcpy(&eh, data + pos, sizeof eh);

            if (eh.length == 0) {                 /* wrap sentinel    */
                p->prod_read += RING_DATA_SIZE - pos;
                continue;
            }
            size_t padded = (eh.length + 7u) & ~((size_t)7u);
            if (eh.length > FRAME_MAX_LEN ||
                pos + sizeof eh + padded > RING_DATA_SIZE) {
                /* Framing is garbage — we can't find the next entry,
                 * so jump to the producer's head and start fresh. */
                LOG_WARN("SHM PUB %s bogus entry len=%u at pos=%zu; "
                         "resync", p->peer, eh.length, pos);
                p->prod_resyncs++;
                p->prod_read = widx;
                break;
            }

            const uint8_t *env = data + pos + sizeof eh;
            size_t env_len = eh.length;

            /* Envelope validation — identical rules to udp_drain. */
            uint32_t magic = 0;
            uint16_t topic_len = 0;
            if (env_len >= ZMESG_FIXED_HDR) {
                memcpy(&magic, env, 4);
                memcpy(&topic_len, env + ZMESG_OFF_TOPIC_LEN, 2);
            }
            if (env_len < ZMESG_FIXED_HDR || magic != ZMESG_MAGIC ||
                (uint32_t)ZMESG_FIXED_HDR + topic_len > env_len) {
                p->prod_bad++;                    /* skip just this   */
            } else {
                const char *topic = (const char *)(env + ZMESG_FIXED_HDR);

                /* One identity, standard sink order (WAL, SHM, TCP). */
                g_sequence++;
                uint64_t seq = g_sequence;
                uint64_t ts = now_ns_real();
                wal_append(&g_wal, env, env_len, ORIGIN_SHM, seq, ts);
                shm_fanout(env, env_len, topic, topic_len,
                           ORIGIN_SHM, seq, ts);

                uint32_t net_len = htonl((uint32_t)env_len);
                memcpy(tcp_frame, &net_len, 4);
                memcpy(tcp_frame + 4, env, env_len);
                fanout(tcp_frame, 4 + env_len, topic, topic_len);

                g_pub_msgs++;
                g_shm_msgs++;
                g_pub_bytes += env_len;
                p->prod_msgs++;
            }
            p->prod_read += sizeof eh + padded;
        }

        /* Publish the cursor: the producer may now reuse everything
         * behind it. Release pairs with the producer's acquire. */
        __atomic_store_n(&hdr->reserved[0], p->prod_read,
                         __ATOMIC_RELEASE);
    }

    /* Publish once more on the way out so a fully-drained ring shows
     * an exactly-caught-up cursor even if the loop broke early. */
    __atomic_store_n(&hdr->reserved[0], p->prod_read, __ATOMIC_RELEASE);
}

/* ---------------- read pump ---------------- */

/*
 * client_dispatch_buffered — hand a client's buffered bytes to the
 * right parser for its role: publishers peel frames (handle_pub_data),
 * handshaking subscribers parse their SUB line, and a running
 * subscriber shouldn't be sending anything, so its bytes are
 * discarded. Returns the parser's verdict (0 keep, -1 close).
 */
static int client_dispatch_buffered(client_t *c) {
    if (c->kind == CK_PUB) return handle_pub_data(c);
    if (c->sub_state == SS_HANDSHAKE) return handle_sub_handshake(c);
    c->rlen = 0;   /* running SUBs don't get to talk; drop it */
    return 0;
}

/*
 * client_readable — called when a TCP client's fd is readable. Drains
 * the socket into the read buffer, parses, and decides the client's
 * fate. Two subtleties here are load-bearing (both found by the test
 * suite, 2026-06-11):
 *
 *   1. EOF must flush, not discard. A short-lived publisher writes
 *      its frames and closes immediately, so the broker often reads
 *      the data AND the EOF in a single wakeup. We note the EOF,
 *      parse everything buffered (frames reach the WAL and both
 *      fan-outs), and only then report -1 so the close loses nothing.
 *
 *   2. A burst backlog must not kill the connection. The buffer cap
 *      (FRAME_MAX_LEN + 4) exists to bound a single frame, but a fast
 *      publisher can pile >1 MiB of *small* frames into one drain
 *      cycle. When the buffer can't grow, we parse-and-consume in
 *      place to make room and keep reading. Only a client whose
 *      buffer stays full after parsing (i.e. a single over-limit
 *      frame) is closed.
 *
 * Returns 0 to keep the client, -1 to close it (peer EOF, protocol
 * violation, or hard socket error).
 */
static int client_readable(client_t *c) {
    int eof = 0;
    for (;;) {
        /* Ensure read headroom: grow toward the cap; at the cap,
         * parse what's buffered to free space instead of giving up. */
        if (c->rcap - c->rlen < READ_CHUNK &&
            rbuf_grow(c, c->rlen + READ_CHUNK) < 0) {
            if (client_dispatch_buffered(c) < 0) return -1;
            if (c->rcap == c->rlen) return -1;  /* frame > protocol max */
        }
        ssize_t n = read(c->fd, c->rbuf + c->rlen, c->rcap - c->rlen);
        if (n > 0) { c->rlen += (size_t)n; continue; }  /* got bytes */
        if (n == 0) { eof = 1; break; }    /* peer closed; flush below */
        if (errno == EAGAIN || errno == EWOULDBLOCK) break; /* drained */
        if (errno == EINTR) continue;                  /* retry read */
        return -1;                                     /* hard error */
    }
    if (client_dispatch_buffered(c) < 0) return -1;
    return eof ? -1 : 0;
}

/*
 * client_writable — called on EPOLLOUT: flush as much of the subscriber's
 * queued FIFO as the socket accepts, then re-arm EPOLLOUT only if data
 * remains. Returns -1 on a socket error (caller closes).
 */
static int client_writable(client_t *c) {
    if (try_flush(c) < 0) return -1;
    update_epoll_mode(c);
    return 0;
}

/* ---------------- accept ---------------- */

/*
 * accept_loop — drain the backlog of a TCP listener in one go.
 *
 * Loops accept4() (with SOCK_NONBLOCK|SOCK_CLOEXEC) until EAGAIN, and for
 * each connection: disables Nagle, allocates a client_t of the given kind,
 * and registers it with epoll for EPOLLIN. Any per-connection failure
 * (alloc, epoll_ctl) closes just that fd and continues — one bad accept
 * never takes down the listener. `kind` is CK_PUB or CK_SUB depending on
 * which listener fired.
 */
static void accept_loop(int listen_fd, client_kind_t kind) {
    for (;;) {
        struct sockaddr_in sa;
        socklen_t sl = sizeof sa;
        /* accept4 applies NONBLOCK+CLOEXEC atomically at accept time
         * — no separate fcntl race window. */
        int fd = accept4(listen_fd, (struct sockaddr *)&sa, &sl,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return; /* done */
            if (errno == EINTR) continue;
            LOG_WARN("accept: %s", strerror(errno));
            return;
        }
        set_nodelay(fd);            /* latency beats batching here   */
        client_t *c = client_new(fd, kind, &sa);
        if (!c) { close(fd); continue; }

        struct epoll_event ev = { .events = EPOLLIN, .data.ptr = c };
        if (epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERR("epoll_ctl ADD: %s", strerror(errno));
            close(fd);
            free(c->rbuf);
            free(c);
            continue;
        }
        c->want_write = EPOLLIN;
        LOG_INFO("connect %s%s", kind == CK_PUB ? "PUB " : "SUB ", c->peer);
    }
}

/* ---------------- SHM accept + handshake ---------------- */

/*
 * memfd_create_safe — thin wrapper invoking memfd_create via syscall()
 * directly (rather than the glibc wrapper, which isn't guaranteed present
 * on every libc we target). Returns the anonymous-memory fd or -1.
 */
static int memfd_create_safe(const char *name, unsigned int flags) {
    long r = syscall(SYS_memfd_create, name, flags);
    if (r < 0) return -1;
    return (int)r;
}

/*
 * send_fds_with_byte — hand two file descriptors to a SHM client over the
 * Unix socket using SCM_RIGHTS ancillary data. POSIX requires at least
 * one byte of regular payload to accompany the control message, so we
 * send a single '\0'. The kernel dup()s the fds into the receiver; the
 * two ints carried here are the ring memfd and the eventfd. Returns 0 if
 * exactly the one byte was sent, -1 otherwise. (Mirror on the client side:
 * recv_two_fds in shm-consumer.c.)
 */
static int send_fds_with_byte(int sock_fd, int fds[2]) {
    /* The mandatory 1 byte of real payload the fds ride along with. */
    char dummy = '\0';
    struct iovec iov = { .iov_base = &dummy, .iov_len = 1 };
    /* Control buffer sized (and aligned) for exactly two ints. */
    char cbuf[CMSG_SPACE(sizeof(int) * 2)];
    memset(cbuf, 0, sizeof cbuf);

    struct msghdr msg = {0};
    msg.msg_iov = &iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cbuf;
    msg.msg_controllen = sizeof cbuf;

    /* One SCM_RIGHTS control message carrying both fds; the kernel
     * dup()s them into the receiving process at delivery. */
    struct cmsghdr *cm = CMSG_FIRSTHDR(&msg);
    cm->cmsg_level = SOL_SOCKET;
    cm->cmsg_type = SCM_RIGHTS;
    cm->cmsg_len = CMSG_LEN(sizeof(int) * 2);
    memcpy(CMSG_DATA(cm), fds, sizeof(int) * 2);

    ssize_t n = sendmsg(sock_fd, &msg, 0);
    return n == 1 ? 0 : -1;
}

/*
 * parse_shm_sub_line — the shm_client_t twin of parse_sub_line: split a
 * "SUB <topics>" line into owned topic strings, honoring "**" as
 * match_all and capping at MAX_TOPICS_PER_SUB. Returns 0 if ≥1 topic was
 * parsed, -1 otherwise. Kept separate only because the client type differs.
 */
static int parse_shm_sub_line(shm_client_t *c, char *line, size_t len) {
    /* Same trim/keyword/tokenize logic as parse_sub_line — see the
     * step-by-step comments there. */
    while (len > 0 && (line[len - 1] == '\r' || line[len - 1] == ' ')) len--;
    if (len < 3 || memcmp(line, "SUB", 3) != 0) return -1;
    char *p = line + 3;
    char *end = line + len;
    while (p < end) {
        while (p < end && *p == ' ') p++;
        if (p >= end) break;
        char *tok = p;
        while (p < end && *p != ' ') p++;
        size_t tl = (size_t)(p - tok);
        if (tl == 0) continue;
        if (c->n_topics >= MAX_TOPICS_PER_SUB) return -1;
        char *t = malloc(tl + 1);
        if (!t) return -1;
        memcpy(t, tok, tl);
        t[tl] = '\0';
        if (tl == 2 && t[0] == '*' && t[1] == '*') c->match_all = 1;
        c->topics[c->n_topics++] = t;
    }
    return c->n_topics > 0 ? 0 : -1;
}

/*
 * shm_complete_handshake — stand up the shared ring for a SHM client.
 *
 * Runs once, after the SUB line is parsed. Sequence:
 *   1. memfd_create a RING_SIZE anonymous file and ftruncate it to size.
 *   2. mmap it read-write into the broker's address space (the broker's
 *      write window), and zero the 64-byte header (the data area is
 *      already zero from ftruncate).
 *   3. Create a non-blocking eventfd as the wakeup channel.
 *   4. Send both fds to the client via SCM_RIGHTS. The client mmaps the
 *      same memfd read-only and blocks reading the eventfd.
 *   5. Record everything on the shm_client_t, mark handshake_done, and add
 *      it to the fan-out registry.
 *
 * Any failure cleans up whatever it already created and returns -1 so the
 * caller closes the connection; success returns 0. After this the broker
 * delivers purely by writing the ring + bumping the eventfd — no more
 * bytes flow over the control socket.
 */
static int shm_complete_handshake(shm_client_t *c) {
    /* Create the ring memfd and eventfd, mmap the ring locally, send both
     * file descriptors to the client over the Unix socket. */
    /* (1) The shared file: anonymous memory, never touches disk. */
    int memfd = memfd_create_safe("planetar-broker-ring", MFD_CLOEXEC);
    if (memfd < 0) {
        LOG_ERR("memfd_create: %s", strerror(errno));
        return -1;
    }
    if (ftruncate(memfd, (off_t)RING_SIZE) < 0) {
        LOG_ERR("ftruncate memfd: %s", strerror(errno));
        close(memfd);
        return -1;
    }
    /* (2) The broker's read-write window onto the ring. */
    void *ring = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, memfd, 0);
    if (ring == MAP_FAILED) {
        LOG_ERR("mmap memfd: %s", strerror(errno));
        close(memfd);
        return -1;
    }
    /* Zero the header (data area is already zero from ftruncate). */
    memset(ring, 0, RING_HEADER_SIZE);

    /* (3) Wakeup channel. NONBLOCK so ring_write_one can never stall
     * on a saturated counter. */
    int evfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (evfd < 0) {
        LOG_ERR("eventfd: %s", strerror(errno));
        munmap(ring, RING_SIZE);
        close(memfd);
        return -1;
    }

    /* (4) Hand both fds across; the client mmaps the same memfd
     * read-only and blocks reading the eventfd. */
    int fds[2] = { memfd, evfd };
    if (send_fds_with_byte(c->sock_fd, fds) < 0) {
        LOG_ERR("send_fds to %s: %s", c->peer, strerror(errno));
        munmap(ring, RING_SIZE);
        close(memfd);
        close(evfd);
        return -1;
    }

    /* (5) Go live: from here fan-out includes this client. */
    c->memfd = memfd;
    c->eventfd_fd = evfd;
    c->ring = ring;
    c->ring_size = RING_SIZE;
    c->handshake_done = 1;
    shm_subs_add(c);

    char topics_log[256] = {0};
    size_t off = 0;
    for (size_t k = 0; k < c->n_topics && off < sizeof topics_log - 1; k++) {
        int w = snprintf(topics_log + off, sizeof topics_log - off,
                         "%s%s", k ? "," : "", c->topics[k]);
        if (w < 0) break;
        off += (size_t)w;
    }
    LOG_INFO("SHM handshake OK %s topics=[%s]%s memfd=%d evfd=%d",
             c->peer, topics_log, c->match_all ? " (match_all)" : "", memfd, evfd);
    return 0;
}

/*
 * shm_complete_pub_handshake — stand up an INBOUND ring for a SHM
 * producer (phase 5). The mirror image of shm_complete_handshake:
 *
 *   1. memfd_create + ftruncate a RING_SIZE ring.
 *   2. mmap it read-write. The broker READS entries from it, but also
 *      WRITES one thing back: its read cursor into header reserved[0]
 *      (the producer's flow-control signal), so PROT_WRITE is needed.
 *   3. Create the eventfd the producer bumps after each publish.
 *   4. Send both fds via SCM_RIGHTS; the producer maps the ring
 *      read-write and starts publishing.
 *   5. Register the eventfd in epoll under the client's prod_ev tag
 *      (its own registration — the handshake socket stays separately
 *      registered for hangup detection), mark the role, and add the
 *      client to the producer registry.
 *
 * Any failure unwinds what was created and returns -1 (caller closes).
 */
static int shm_complete_pub_handshake(shm_client_t *c) {
    int memfd = memfd_create_safe("planetar-broker-pubring", MFD_CLOEXEC);
    if (memfd < 0) {
        LOG_ERR("memfd_create (pub): %s", strerror(errno));
        return -1;
    }
    if (ftruncate(memfd, (off_t)RING_SIZE) < 0) {
        LOG_ERR("ftruncate pub memfd: %s", strerror(errno));
        close(memfd);
        return -1;
    }
    void *ring = mmap(NULL, RING_SIZE, PROT_READ | PROT_WRITE,
                      MAP_SHARED, memfd, 0);
    if (ring == MAP_FAILED) {
        LOG_ERR("mmap pub memfd: %s", strerror(errno));
        close(memfd);
        return -1;
    }
    memset(ring, 0, RING_HEADER_SIZE);

    int evfd = eventfd(0, EFD_CLOEXEC | EFD_NONBLOCK);
    if (evfd < 0) {
        LOG_ERR("eventfd (pub): %s", strerror(errno));
        munmap(ring, RING_SIZE);
        close(memfd);
        return -1;
    }

    int fds[2] = { memfd, evfd };
    if (send_fds_with_byte(c->sock_fd, fds) < 0) {
        LOG_ERR("send_fds (pub) to %s: %s", c->peer, strerror(errno));
        goto fail;
    }

    /* The eventfd gets its own epoll registration, tagged so dispatch
     * can find its way back to this client (see shm_prod_ev_t). */
    c->prod_ev.ev.kind = EV_SHM_PROD_EV;
    c->prod_ev.owner = c;
    struct epoll_event ev = { .events = EPOLLIN, .data.ptr = &c->prod_ev };
    if (epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, evfd, &ev) < 0) {
        LOG_ERR("epoll_ctl ADD pub evfd: %s", strerror(errno));
        goto fail;
    }

    c->memfd = memfd;
    c->eventfd_fd = evfd;
    c->ring = ring;
    c->ring_size = RING_SIZE;
    c->is_producer = 1;
    c->handshake_done = 1;
    if (shm_prods_add(c) < 0) {
        /* Table full: undo the epoll add; caller closes the rest. */
        epoll_ctl(g_epoll_fd, EPOLL_CTL_DEL, evfd, NULL);
        c->is_producer = 0;
        goto fail;
    }

    LOG_INFO("SHM PUB handshake OK %s memfd=%d evfd=%d%s",
             c->peer, memfd, evfd, g_spin ? " (spin)" : "");
    return 0;

fail:
    munmap(ring, RING_SIZE);
    close(memfd);
    close(evfd);
    c->ring = NULL;
    c->memfd = -1;
    c->eventfd_fd = -1;
    return -1;
}

/*
 * shm_handle_handshake_data — read handshake bytes from a SHM client's
 * control socket and complete the handshake once a full line has
 * arrived: "SUB <topics>" makes the client a subscriber,
 * "PUB" makes it a producer.
 *
 * Accumulates into the fixed rline buffer (overflowing it is an error),
 * scans for a newline, and on finding one parses the line and calls
 * the matching completion. Returns 0 if still waiting for more bytes or
 * the handshake succeeded, -1 on EOF / malformed line / setup failure
 * (caller closes). A no-op once handshake_done is set.
 */
static int shm_handle_handshake_data(shm_client_t *c) {
    if (c->handshake_done) return 0;
    /* Drain whatever arrived into the fixed-size line buffer. */
    for (;;) {
        ssize_t n = read(c->sock_fd, c->rline + c->rlen, sizeof c->rline - c->rlen);
        if (n > 0) c->rlen += (size_t)n;
        else if (n == 0) return -1;
        else if (errno == EAGAIN || errno == EWOULDBLOCK) break;
        else if (errno == EINTR) continue;
        else return -1;
        if (c->rlen >= sizeof c->rline) return -1;
    }
    /* Full line yet? If so, parse it and stand up the ring now. */
    for (size_t i = 0; i < c->rlen; i++) {
        if (c->rline[i] == '\n') {
            /* "PUB" -> producer role (no topics; producers declare a
             * topic inside each envelope, and the broker routes on
             * those). Anything else must be a "SUB ..." line. */
            if (i >= 3 && memcmp(c->rline, "PUB", 3) == 0)
                return shm_complete_pub_handshake(c);
            int ok = parse_shm_sub_line(c, c->rline, i) == 0;
            if (!ok) return -1;
            return shm_complete_handshake(c);
        }
    }
    return 0; /* more bytes needed */
}

/*
 * accept_shm — drain the Unix-socket listener's backlog. For each
 * connection, allocate a shm_client_t (tagged EV_SHM_CLIENT, fds set to
 * -1 until the handshake fills them) and register it for EPOLLIN |
 * EPOLLRDHUP so we learn both about its SUB line and about it hanging up.
 * The ring isn't created here — that waits for shm_complete_handshake.
 * Per-connection failures close just that fd and continue.
 */
static void accept_shm(int listen_fd) {
    for (;;) {
        struct sockaddr_un sa;
        socklen_t sl = sizeof sa;
        int fd = accept4(listen_fd, (struct sockaddr *)&sa, &sl,
                         SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR) continue;
            LOG_WARN("unix accept: %s", strerror(errno));
            return;
        }
        shm_client_t *c = calloc(1, sizeof *c);
        if (!c) { close(fd); continue; }
        c->ev.kind = EV_SHM_CLIENT;
        c->sock_fd = fd;
        c->memfd = -1;        /* -1 = not created yet, so teardown   */
        c->eventfd_fd = -1;   /* knows exactly what to skip          */
        snprintf(c->peer, sizeof c->peer, "shm-%d", fd);

        struct epoll_event ev = { .events = EPOLLIN | EPOLLRDHUP, .data.ptr = c };
        if (epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, fd, &ev) < 0) {
            LOG_ERR("epoll_ctl ADD shm: %s", strerror(errno));
            close(fd);
            free(c);
            continue;
        }
        LOG_INFO("connect SHM %s", c->peer);
    }
}

/* ---------------- main ---------------- */

/*
 * main — wire everything up and run the single epoll reactor.
 *
 * Startup:
 *   - Ignore SIGPIPE (a peer vanishing mid-write must return EPIPE, not
 *     kill us) and catch SIGINT/SIGTERM to request a clean stop.
 *   - Bring up the WAL (recovers g_sequence); failure is non-fatal — we
 *     log it and run without durability.
 *   - Open the four listeners (TCP pub, TCP sub, UDP, Unix/SHM) and the
 *     epoll instance, registering each listener with a static ev_handle_t
 *     tag so the loop can tell them apart.
 *
 * Reactor: epoll_wait with a 1s timeout (the timeout just lets us notice
 * g_stop promptly). Each ready fd is dispatched on its tag:
 *   - listener tags run the matching accept/drain helper;
 *   - EV_SHM_CLIENT handles handshake bytes or hangup;
 *   - everything else is a client_t handled read-then-write, closed on
 *     error/hangup.
 *
 * Shutdown: log lifetime counters, finalize the WAL, close every client
 * and listener, remove the Unix socket node, and exit 0.
 */
int main(void) {
    /* SIGPIPE off: writing to a vanished peer must fail with EPIPE,
     * not kill the process. INT/TERM request an orderly stop. */
    signal(SIGPIPE, SIG_IGN);
    signal(SIGINT, on_sigint);
    signal(SIGTERM, on_sigint);

    const char *wal_dir = getenv("WAL_DIR");
    if (!wal_dir || !*wal_dir) wal_dir = WAL_DIR_DEFAULT;
    if (wal_init(&g_wal, wal_dir) < 0) {
        LOG_WARN("WAL init failed; continuing without durability");
    }

    /* Benchmark mode: busy-poll inbound SHM rings (see g_spin). */
    const char *spin = getenv("BROKER_SPIN");
    if (spin && *spin && *spin != '0') {
        g_spin = 1;
        LOG_INFO("BROKER_SPIN on: busy-polling SHM producer rings");
    }

    int pub_fd = listen_tcp(PUB_PORT);
    int sub_fd = listen_tcp(SUB_PORT);
    int udp_fd = bind_udp(UDP_PORT);
    int shm_fd = bind_unix(SHM_PATH);
    if (pub_fd < 0 || sub_fd < 0 || udp_fd < 0 || shm_fd < 0) return 1;

    g_epoll_fd = epoll_create1(EPOLL_CLOEXEC);
    if (g_epoll_fd < 0) { LOG_ERR("epoll_create1: %s", strerror(errno)); return 1; }

    /* The four listener tags. Static storage: they must outlive the
     * loop, and being plain ev_handle_t (kind only) is exactly what
     * lets the dispatch below tell them apart from heap clients. */
    static ev_handle_t pub_ev = { EV_LISTEN_PUB };
    static ev_handle_t sub_ev = { EV_LISTEN_SUB };
    static ev_handle_t udp_ev = { EV_LISTEN_UDP };
    static ev_handle_t shm_ev = { EV_LISTEN_SHM };
    struct epoll_event ev;
    ev.events = EPOLLIN; ev.data.ptr = &pub_ev;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, pub_fd, &ev);
    ev.events = EPOLLIN; ev.data.ptr = &sub_ev;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, sub_fd, &ev);
    ev.events = EPOLLIN; ev.data.ptr = &udp_ev;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, udp_fd, &ev);
    ev.events = EPOLLIN; ev.data.ptr = &shm_ev;
    epoll_ctl(g_epoll_fd, EPOLL_CTL_ADD, shm_fd, &ev);

    LOG_INFO("planetar-broker phase-4 ready · PUB=%d SUB=%d UDP=%d SHM=%s · WAL=%s seq=%llu",
             PUB_PORT, SUB_PORT, UDP_PORT, SHM_PATH,
             g_wal.enabled ? wal_dir : "off", (unsigned long long)g_sequence);

    /* The reactor. The 1000 ms timeout exists only so a signal that
     * lands between events still stops us within a second. In spin
     * mode WITH producers attached the timeout drops to zero: the
     * loop becomes a busy-poll that drains producer rings directly,
     * with epoll degraded to a non-blocking check for everything
     * else (accepts, TCP traffic, hangups). */
    struct epoll_event events[EPOLL_BATCH];
    while (!g_stop) {
        int timeout = (g_spin && g_shm_prods_count > 0) ? 0 : 1000;
        int n = epoll_wait(g_epoll_fd, events, EPOLL_BATCH, timeout);
        if (n < 0) { if (errno == EINTR) continue; LOG_ERR("epoll_wait: %s", strerror(errno)); break; }
        for (int i = 0; i < n; i++) {
            /* Dispatch on the kind tag every registered object leads
             * with — see ev_handle_t. */
            ev_handle_t *h = events[i].data.ptr;
            if (h->kind == EV_LISTEN_PUB) { accept_loop(pub_fd, CK_PUB); continue; }
            if (h->kind == EV_LISTEN_SUB) { accept_loop(sub_fd, CK_SUB); continue; }
            if (h->kind == EV_LISTEN_UDP) { udp_drain(udp_fd); continue; }
            if (h->kind == EV_LISTEN_SHM) { accept_shm(shm_fd); continue; }
            if (h->kind == EV_SHM_PROD_EV) {
                /* A producer bumped its eventfd: clear the counter
                 * (its value is irrelevant — the ring's write_idx is
                 * the source of truth) and drain the ring. */
                shm_client_t *p = ((shm_prod_ev_t *)h)->owner;
                uint64_t v;
                ssize_t r = read(p->eventfd_fd, &v, sizeof v);
                (void)r;
                shm_producer_drain(p);
                continue;
            }
            if (h->kind == EV_SHM_CLIENT) {
                shm_client_t *sc = (shm_client_t *)h;
                int rc = 0;
                if (events[i].events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) rc = -1;
                if (rc == 0 && (events[i].events & EPOLLIN)) rc = shm_handle_handshake_data(sc);
                if (rc < 0) shm_client_close(sc);
                continue;
            }
            /* Anything else is a TCP client. Error/hangup beats I/O;
             * read before write; any -1 tears the client down. */
            client_t *c = (client_t *)h;
            int rc = 0;
            if (events[i].events & (EPOLLERR | EPOLLHUP)) rc = -1;
            if (rc == 0 && (events[i].events & EPOLLIN))  rc = client_readable(c);
            if (rc == 0 && (events[i].events & EPOLLOUT)) rc = client_writable(c);
            if (rc < 0) client_close(c);
        }

        /* Spin mode: poll every producer ring each iteration instead
         * of waiting for eventfd wakeups. Costs a no-op acquire-load
         * per idle producer per spin; delivers ~100 ns-class ingest. */
        if (g_spin) {
            for (size_t pi = 0; pi < g_shm_prods_count; pi++)
                shm_producer_drain(g_shm_prods[pi]);
        }
    }

    LOG_INFO("shutting down. pub_msgs=%llu (tcp=%llu udp=%llu shm=%llu) pub_bytes=%llu delivered=%llu seq=%llu",
             g_pub_msgs, g_pub_msgs - g_udp_msgs - g_shm_msgs,
             g_udp_msgs, g_shm_msgs,
             g_pub_bytes, g_delivered, (unsigned long long)g_sequence);
    wal_close(&g_wal);
    /* Best-effort cleanup before exit (the OS reclaims regardless).
     * NOTE: each client_close() swap-compacts g_subs — an unvisited
     * subscriber is swapped into the current slot and the forward
     * loop then steps past it, so only about half get closed here.
     * Harmless at exit; the SHM loop below shows the correct
     * backward-iteration pattern. */
    for (size_t i = 0; i < g_subs_count; i++) client_close(g_subs[i]);
    for (size_t i = g_shm_subs_count; i > 0; i--) shm_client_close(g_shm_subs[i - 1]);
    /* Producers last: their close path drains any unread ring tail
     * into the WAL/fan-out first (subscribers above are gone by now,
     * but durability still wants the entries). */
    for (size_t i = g_shm_prods_count; i > 0; i--) shm_client_close(g_shm_prods[i - 1]);
    close(pub_fd);
    close(sub_fd);
    close(udp_fd);
    close(shm_fd);
    unlink(SHM_PATH);
    close(g_epoll_fd);
    return 0;
}
