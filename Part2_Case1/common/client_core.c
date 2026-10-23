#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64
#include "client_core.h"
#include "protocol.h"
#include "io_helpers.h"

#include <arpa/inet.h>
#include <errno.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CASE_PART1 1
#define CASE_RESTART 2
#define CASE_RESUME 3
#define CASE_ENHANCED 4
#define MAX_RETRIES 10000

typedef struct {
    char topic[IT305_MAX_TOPIC + 1];
    char filename[IT305_MAX_FILENAME + 1];
    uint64_t offset;
    uint32_t files_completed;
} ResumeCheckpoint;

typedef struct {
    char filename[IT305_MAX_FILENAME + 1];
    uint64_t file_size;
    uint64_t bytes_received;
    int completed;
    pthread_mutex_t mutex;
} EnhancedEntry;

typedef struct {
    char server_ip[256];
    int port;
    char topic[IT305_MAX_TOPIC + 1];
    char topic_dir[PATH_MAX];
    char checkpoint_path[PATH_MAX];
    EnhancedEntry *entries;
    size_t entry_count;
    size_t next_entry;
    pthread_mutex_t queue_mutex;
    pthread_mutex_t checkpoint_mutex; /* kept for API stability; no longer held on the data path */
    int journal_fd;                   /* append-only completed-file journal (Enhanced) */
    int failed;
    uint64_t received_payload_bytes;
    uint64_t useful_payload_bytes;
    uint64_t retries;
} EnhancedContext;

typedef struct {
    EnhancedContext *ctx;
} EnhancedWorkerArgs;

static void sleep_ms(long ms) {
    struct timespec ts;
    ts.tv_sec = ms / 1000;
    ts.tv_nsec = (ms % 1000) * 1000000L;
    while (nanosleep(&ts, &ts) != 0 && errno == EINTR) {}
}

/* Delay before reconnecting after an interrupted transfer. Default 0 ms so that measurements reflect the
   protocol, not an artificial back-off; override with IT305_RECONNECT_DELAY_MS for a polite back-off. */
static long reconnect_delay_ms(void) {
    static long cached = -1;           /* read/written with atomics: called from several worker threads */
    long v = __atomic_load_n(&cached, __ATOMIC_RELAXED);
    if (v < 0) {
        const char *e = getenv("IT305_RECONNECT_DELAY_MS");
        v = e ? atol(e) : 0;
        if (v < 0) v = 0;
        __atomic_store_n(&cached, v, __ATOMIC_RELAXED);
    }
    return v;
}

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static int parse_port(const char *s, int *out) {
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (!s || !*s || !end || *end || v < 1 || v > 65535) return -1;
    *out = (int)v;
    return 0;
}

static int connect_to_server(const char *host, int port, int enhanced) {
    char service[16];
    snprintf(service, sizeof(service), "%d", port);
    struct addrinfo hints;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    struct addrinfo *list = NULL;
    int e = getaddrinfo(host, service, &hints, &list);
    if (e != 0) { fprintf(stderr, "getaddrinfo(%s): %s\n", host, gai_strerror(e)); return -2; }
    int sock = -1;
    for (struct addrinfo *it = list; it; it = it->ai_next) {
        sock = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
        if (sock < 0) continue;
        if (enhanced) (void)it305_set_socket_buffers(sock, 256 * 1024);
        if (connect(sock, it->ai_addr, it->ai_addrlen) == 0) { (void)it305_set_nodelay(sock); break; }
        close(sock); sock = -1;
    }
    freeaddrinfo(list);
    return sock;
}

/* Connect, retrying for up to IT305_CONNECT_TIMEOUT_S seconds (default 10). A server that is down, on another port or
 * behind a firewall now produces a clear error and a non-zero exit instead of minutes of silent retries; a server that
 * restarts within the budget is picked up again (and the transfer resumes where the design allows it). */
static int connect_with_budget(const char *host, int port, int enhanced) {
    static long cached = -1;            /* atomics: also called from the Enhanced worker threads */
    long budget_ms = __atomic_load_n(&cached, __ATOMIC_RELAXED);
    if (budget_ms < 0) {
        const char *e = getenv("IT305_CONNECT_TIMEOUT_S");
        long v = e ? atol(e) : 10;
        budget_ms = (v < 0 ? 0 : v) * 1000;
        __atomic_store_n(&cached, budget_ms, __ATOMIC_RELAXED);
    }
    uint64_t t0 = monotonic_ns();
    int warned = 0;
    for (;;) {
        int sock = connect_to_server(host, port, enhanced);
        if (sock >= 0) return sock;
        if (sock == -2) return -1;      /* host name does not resolve: retrying cannot help */
        long waited_ms = (long)((monotonic_ns() - t0) / 1000000ull);
        if (waited_ms >= budget_ms) {
            fprintf(stderr, "Cannot connect to %s:%d after %.0f s (server not running, wrong address/port, or blocked by a firewall?)\n",
                    host, port, waited_ms / 1000.0);
            return -1;
        }
        if (!warned) { fprintf(stderr, "Cannot connect to %s:%d yet; retrying for up to %ld s\n", host, port, budget_ms / 1000); warned = 1; }
        sleep_ms(100);
    }
}

static int remove_tree(const char *path) {
    struct stat st;
    if (lstat(path, &st) != 0) return errno == ENOENT ? 0 : -1;
    if (S_ISDIR(st.st_mode)) {
        DIR *d = opendir(path);
        if (!d) return -1;
        struct dirent *de;
        while ((de = readdir(d)) != NULL) {
            if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
            char child[PATH_MAX];
            if (it305_path_join(child, sizeof(child), path, de->d_name) != 0 || remove_tree(child) != 0) {
                closedir(d); return -1;
            }
        }
        closedir(d);
        return rmdir(path);
    }
    return unlink(path);
}

static int sum_regular_files(const char *dir, uint64_t *total) {
    DIR *d = opendir(dir);
    if (!d) return -1;
    *total = 0;
    struct dirent *de;
    while ((de = readdir(d)) != NULL) {
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..") || de->d_name[0] == '.') continue;
        char path[PATH_MAX];
        if (it305_path_join(path, sizeof(path), dir, de->d_name) != 0) continue;
        struct stat st;
        if (lstat(path, &st) == 0 && S_ISREG(st.st_mode) && st.st_size >= 0) *total += (uint64_t)st.st_size;
    }
    closedir(d);
    return 0;
}

static int make_client_paths(const char *output_dir, const char *topic,
                             char *topic_dir, size_t topic_dir_size,
                             char *checkpoint, size_t checkpoint_size) {
    if (!it305_valid_component(topic) || strlen(topic) > IT305_MAX_TOPIC) {
        fprintf(stderr, "topic must be a single directory name (no slash or '..')\n"); return -1;
    }
    if (it305_mkdir_p(output_dir, 0755) != 0) { perror("create output directory"); return -1; }
    if (it305_path_join(topic_dir, topic_dir_size, output_dir, topic) != 0) return -1;
    if (it305_mkdir_p(topic_dir, 0755) != 0) { perror("create topic output directory"); return -1; }
    if (it305_path_join(checkpoint, checkpoint_size, topic_dir, ".checkpoint.tmp") != 0) return -1;
    return 0;
}

/* The checkpoint is a fixed 512-byte record rewritten in place with a single pwrite(). A process crash cannot
   tear a one-page write, and the client additionally clamps the stored offset to the length of the local
   file when it loads the checkpoint, so a stale or short record is always safe. No fsync: durability against
   an OS crash is deliberately traded for throughput (see README, "Checkpoint durability"). */
#define CHECKPOINT_RECORD 512
static int save_resume_checkpoint(const char *path, const ResumeCheckpoint *cp) {
    char text[CHECKPOINT_RECORD];
    int n = snprintf(text, sizeof(text), "V1\n%s\n%u\n%s\n%llu\n",
                     cp->topic, cp->files_completed, cp->filename, (unsigned long long)cp->offset);
    if (n < 0 || (size_t)n >= sizeof(text)) { errno = EOVERFLOW; return -1; }
    memset(text + n, ' ', sizeof(text) - (size_t)n);
    text[sizeof(text) - 1] = '\n';
    int fd = open(path, O_WRONLY | O_CREAT | O_CLOEXEC, 0644);
    if (fd < 0) return -1;
    ssize_t w;
    do { w = pwrite(fd, text, sizeof(text), 0); } while (w < 0 && errno == EINTR);
    int saved = errno;
    close(fd);
    if (w != (ssize_t)sizeof(text)) { errno = w < 0 ? saved : EIO; return -1; }
    return 0;
}

static int load_resume_checkpoint(const char *path, const char *topic, ResumeCheckpoint *cp) {
    FILE *f = fopen(path, "r");
    if (!f) return 0;
    /* sized for the widest conversion below: %259[^\n] can store 259 chars + NUL */
    char version[16], loaded_topic[IT305_MAX_TOPIC + 8], file[IT305_MAX_FILENAME + 8];
    unsigned long long offset;
    unsigned completed;
    int ok = fscanf(f, "%15[^\n]\n%130[^\n]\n%u\n%259[^\n]\n%llu", version,
                    loaded_topic, &completed, file, &offset) == 5;
    fclose(f);
    if (!ok || strcmp(version, "V1") || strcmp(loaded_topic, topic) ||
        strlen(loaded_topic) > IT305_MAX_TOPIC ||
        !it305_valid_component(file) || strlen(file) > IT305_MAX_FILENAME) return 0;
    memset(cp, 0, sizeof(*cp));
    memcpy(cp->topic, loaded_topic, strlen(loaded_topic) + 1);
    memcpy(cp->filename, file, strlen(file) + 1);
    cp->offset = (uint64_t)offset;
    cp->files_completed = completed;
    return 1;
}

static int send_get(int sock, const char *topic) {
    size_t n = strlen(topic);
    return it305_send_frame(sock, MSG_CMD_GET, 0, 0, topic, (uint32_t)n);
}

static int send_resume(int sock, const char *topic, const char *filename, uint64_t offset) {
    uint32_t tl = (uint32_t)strlen(topic), fl = (uint32_t)strlen(filename);
    uint32_t n = 16u + tl + fl;
    unsigned char *p = malloc(n);
    if (!p) return -1;
    it305_put_u32(p, tl); it305_put_u32(p + 4, fl); it305_put_u64(p + 8, offset);
    memcpy(p + 16, topic, tl); memcpy(p + 16 + tl, filename, fl);
    int rc = it305_send_frame(sock, MSG_CMD_RESUME, 0, 0, p, n);
    free(p);
    return rc;
}

static int send_get_file(int sock, const char *topic, const char *filename) {
    uint32_t tl = (uint32_t)strlen(topic), fl = (uint32_t)strlen(filename);
    uint32_t n = 8u + tl + fl;
    unsigned char *p = malloc(n);
    if (!p) return -1;
    it305_put_u32(p, tl); it305_put_u32(p + 4, fl);
    memcpy(p + 8, topic, tl); memcpy(p + 8 + tl, filename, fl);
    int rc = it305_send_frame(sock, MSG_CMD_GET_FILE, 0, 0, p, n);
    free(p);
    return rc;
}

static int parse_file_header(const It305Frame *f, char *filename, size_t filename_cap,
                             uint64_t *file_size, uint64_t *start_offset) {
    if (f->type != MSG_FILE_HEADER || f->length < 20) return -1;
    *file_size = it305_get_u64(f->payload);
    *start_offset = it305_get_u64(f->payload + 8);
    uint32_t n = it305_get_u32(f->payload + 16);
    if (n == 0 || n >= filename_cap || (uint64_t)20 + n != f->length ||
        memchr(f->payload + 20, '\0', n) != NULL) return -1;
    memcpy(filename, f->payload + 20, n); filename[n] = '\0';
    if (!it305_valid_component(filename)) return -1;
    return 0;
}

static int write_at(int fd, const unsigned char *buf, size_t n, uint64_t offset) {
    size_t done = 0;
    while (done < n) {
        ssize_t w = pwrite(fd, buf + done, n - done, (off_t)(offset + done));
        if (w < 0 && errno == EINTR) continue;
        if (w <= 0) return -1;
        done += (size_t)w;
    }
    return 0;
}

/* Transfer the full topic or a resumed topic. Returns 0 completed, 1 disconnected,
   2 invalid resume checkpoint (restart cleanly), -1 fatal application/protocol error. */
static int receive_topic_stream(int sock, int case_id, const char *topic,
                                const char *topic_dir, const char *checkpoint_path,
                                ResumeCheckpoint *cp, uint64_t *received_bytes,
                                uint64_t *successful_attempt_bytes) {
    char current_name[IT305_MAX_FILENAME + 1] = "";
    uint64_t current_size = 0, offset = 0;
    int file_fd = -1;
    int have_file = 0;
    for (;;) {
        It305Frame f;
        int rr = it305_recv_frame(sock, &f);
        if (rr == 0 || rr == -1) { if (file_fd >= 0) close(file_fd); return 1; }
        if (f.type == MSG_ERR_BAD_OFFSET) {
            it305_frame_free(&f); if (file_fd >= 0) close(file_fd); return 2;
        }
        if (f.type == MSG_ERR_NOT_FOUND || f.type == MSG_ERR_PROTOCOL) {
            fprintf(stderr, "Server error: %.*s\n", (int)f.length, f.payload ? (char *)f.payload : "application error");
            it305_frame_free(&f); if (file_fd >= 0) close(file_fd); return -1;
        }
        if (f.type == MSG_FILE_HEADER) {
            if (file_fd >= 0) close(file_fd);
            if (parse_file_header(&f, current_name, sizeof(current_name), &current_size, &offset) != 0 || offset > current_size) {
                it305_frame_free(&f); return -1;
            }
            char file_path[PATH_MAX];
            if (it305_path_join(file_path, sizeof(file_path), topic_dir, current_name) != 0) { it305_frame_free(&f); return -1; }
            int flags = O_CREAT | O_RDWR | O_CLOEXEC;
            file_fd = open(file_path, flags, 0644);
            if (file_fd < 0) { perror("open output file"); it305_frame_free(&f); return -1; }
            struct stat st;
            if (offset == 0) {
                if (ftruncate(file_fd, 0) != 0) { close(file_fd); file_fd = -1; it305_frame_free(&f); return -1; }
            } else if (fstat(file_fd, &st) != 0 || st.st_size < 0 || (uint64_t)st.st_size < offset) {
                close(file_fd); file_fd = -1; it305_frame_free(&f); return 2;
            }
            have_file = 1;
            if (case_id == CASE_RESUME) {
                snprintf(cp->topic, sizeof(cp->topic), "%s", topic);
                snprintf(cp->filename, sizeof(cp->filename), "%s", current_name);
                cp->offset = offset;
                if (save_resume_checkpoint(checkpoint_path, cp) != 0) { perror("save checkpoint"); close(file_fd); file_fd = -1; it305_frame_free(&f); return -1; }
            }
            it305_frame_free(&f);
            continue;
        }
        if (f.type == MSG_FILE_DATA) {
            if (!have_file || file_fd < 0 || f.length > IT305_CHUNK_SIZE || offset + f.length > current_size) {
                it305_frame_free(&f); if (file_fd >= 0) close(file_fd); return -1;
            }
            if (f.length && write_at(file_fd, f.payload, f.length, offset) != 0) {
                perror("write downloaded data"); it305_frame_free(&f); close(file_fd); return -1;
            }
            offset += f.length;
            *received_bytes += f.length;
            *successful_attempt_bytes += f.length;
            if (case_id == CASE_RESUME) {
                cp->offset = offset;
                if (save_resume_checkpoint(checkpoint_path, cp) != 0) { perror("save checkpoint"); it305_frame_free(&f); close(file_fd); return -1; }
            }
            int final_chunk = (f.flags & MSG_FLAG_FINAL_CHUNK) != 0;
            it305_frame_free(&f);
            if (final_chunk) {
                if (offset != current_size) { close(file_fd); return -1; }
                close(file_fd); file_fd = -1; have_file = 0;
                if (case_id == CASE_RESUME) {
                    cp->offset = current_size;
                    cp->files_completed++;
                    if (save_resume_checkpoint(checkpoint_path, cp) != 0) { perror("save checkpoint"); return -1; }
                }
            }
            continue;
        }
        if (f.type == MSG_END_OF_TOPIC) {
            it305_frame_free(&f);
            if (file_fd >= 0) close(file_fd);
            if (case_id == CASE_RESUME) unlink(checkpoint_path);
            return 0;
        }
        if (f.type == MSG_FILE_COMPLETE) {
            /* FILE_COMPLETE is expected only for enhanced per-file requests. */
            it305_frame_free(&f); if (file_fd >= 0) close(file_fd); return -1;
        }
        fprintf(stderr, "Unexpected server opcode 0x%02x\n", f.type);
        it305_frame_free(&f); if (file_fd >= 0) close(file_fd); return -1;
    }
}

static void print_result(const char *case_name, double elapsed, uint64_t received,
                         uint64_t useful, uint64_t redundant, uint64_t retries) {
    double throughput = elapsed > 0.0 ? (double)useful / elapsed : 0.0;
    printf("RESULT,case=%s,elapsed_sec=%.9f,received_payload_bytes=%llu,useful_payload_bytes=%llu,"
           "redundant_payload_bytes=%llu,retries=%llu,throughput_Bps=%.3f\n",
           case_name, elapsed, (unsigned long long)received, (unsigned long long)useful,
           (unsigned long long)redundant, (unsigned long long)retries, throughput);
    fflush(stdout);
}

static int client_restart_mode(const char *host, int port, const char *topic,
                               const char *topic_dir, const char *checkpoint_path,
                               int case_id) {
    (void)checkpoint_path;
    uint64_t started = monotonic_ns();
    uint64_t received = 0, final_attempt_bytes = 0, retries = 0;
    int rc = -1;
    for (unsigned long attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        if (attempt > 0) {
            retries++;
            if (remove_tree(topic_dir) != 0 || it305_mkdir_p(topic_dir, 0755) != 0) { perror("reset topic directory"); break; }
        }
        int sock = connect_with_budget(host, port, 0);
        if (sock < 0) break;                          /* gave up: exit code 1 */
        if (send_get(sock, topic) != 0) { close(sock); sleep_ms(reconnect_delay_ms()); continue; }
        uint64_t this_attempt = 0;
        rc = receive_topic_stream(sock, case_id, topic, topic_dir, checkpoint_path, NULL, &received, &this_attempt);
        close(sock);
        if (rc == 0) { final_attempt_bytes = this_attempt; break; }
        if (rc < 0) break;
        fprintf(stderr, "Connection interrupted; restarting topic from byte 0 (attempt %lu)\n", attempt + 2);
        sleep_ms(reconnect_delay_ms());
    }
    uint64_t ended = monotonic_ns();
    double elapsed = ended > started ? (double)(ended - started) / 1e9 : 0.0;
    if (rc == 0) {
        uint64_t total = 0; (void)sum_regular_files(topic_dir, &total);
        uint64_t redundant = received > final_attempt_bytes ? received - final_attempt_bytes : 0;
        print_result(case_id == CASE_PART1 ? "Part1" : "Part2_Case1", elapsed, received, final_attempt_bytes, redundant, retries);
        fprintf(stdout, "Transfer completed: topic=%s files_bytes=%llu directory=%s\n", topic,
                (unsigned long long)total, topic_dir);
        return 0;
    }
    return 1;
}

static int client_resume_mode(const char *host, int port, const char *topic,
                              const char *topic_dir, const char *checkpoint_path) {
    uint64_t started = monotonic_ns(), received = 0, retries = 0;
    ResumeCheckpoint cp;
    memset(&cp, 0, sizeof(cp));
    int has_cp = load_resume_checkpoint(checkpoint_path, topic, &cp);
    if (has_cp) {
        char local_file[PATH_MAX];
        if (it305_path_join(local_file, sizeof(local_file), topic_dir, cp.filename) != 0) has_cp = 0;
        else {
            struct stat st;
            if (stat(local_file, &st) != 0 || st.st_size < 0) cp.offset = 0;
            else if ((uint64_t)st.st_size < cp.offset) cp.offset = (uint64_t)st.st_size;
        }
    }
    int rc = -1;
    for (unsigned long attempt = 0; attempt < MAX_RETRIES; ++attempt) {
        int sock = connect_with_budget(host, port, 0);
        if (sock < 0) break;                          /* gave up: exit code 1 */
        int send_rc = (has_cp && cp.filename[0]) ? send_resume(sock, topic, cp.filename, cp.offset) : send_get(sock, topic);
        if (send_rc != 0) { close(sock); retries++; continue; }
        uint64_t this_attempt = 0;
        rc = receive_topic_stream(sock, CASE_RESUME, topic, topic_dir, checkpoint_path, &cp, &received, &this_attempt);
        close(sock);
        if (rc == 0) break;
        if (rc == 2) {
            fprintf(stderr, "Server rejected resume checkpoint; restarting topic from zero\n");
            (void)remove_tree(topic_dir);
            if (it305_mkdir_p(topic_dir, 0755) != 0) break;
            memset(&cp, 0, sizeof(cp)); has_cp = 0; retries++; continue;
        }
        if (rc < 0) break;
        has_cp = cp.filename[0] != '\0';
        retries++;
        fprintf(stderr, "Connection interrupted; resuming %s at offset %llu\n",
                cp.filename[0] ? cp.filename : "topic start", (unsigned long long)cp.offset);
        sleep_ms(reconnect_delay_ms());
    }
    uint64_t ended = monotonic_ns();
    double elapsed = ended > started ? (double)(ended - started) / 1e9 : 0.0;
    if (rc == 0) {
        uint64_t total = 0; (void)sum_regular_files(topic_dir, &total);
        /* useful = bytes that ended up in the output files (this run); redundant = everything else received */
        uint64_t useful = received < total ? received : total;
        print_result("Part2_Case2", elapsed, received, useful, received - useful, retries);
        printf("Transfer completed: topic=%s files_bytes=%llu directory=%s\n", topic, (unsigned long long)total, topic_dir);
        return 0;
    }
    return 1;
}

static int request_manifest(const char *host, int port, const char *topic, int *sock_out,
                            EnhancedEntry **entries_out, size_t *count_out) {
    int sock = connect_with_budget(host, port, 1);
    if (sock < 0) return -1;
    if (it305_send_frame(sock, MSG_CMD_MANIFEST, 0, 0, topic, (uint32_t)strlen(topic)) != 0) { close(sock); return -1; }
    It305Frame f;
    int rr = it305_recv_frame(sock, &f);
    if (rr != 1) { close(sock); return -1; }
    if (f.type == MSG_ERR_NOT_FOUND || f.type == MSG_ERR_PROTOCOL) {
        fprintf(stderr, "Manifest error: %.*s\n", (int)f.length, f.payload ? (char *)f.payload : "error");
        it305_frame_free(&f); close(sock); return -1;
    }
    if (f.type != MSG_MANIFEST_DATA || f.length < 4) { it305_frame_free(&f); close(sock); return -1; }
    uint32_t count = it305_get_u32(f.payload);
    if (count > 1000000u) { it305_frame_free(&f); close(sock); errno = EOVERFLOW; return -1; }
    EnhancedEntry *entries = calloc(count ? count : 1, sizeof(*entries));
    if (!entries) { it305_frame_free(&f); close(sock); return -1; }
    uint32_t initialized = 0;
    for (uint32_t i = 0; i < count; ++i) {
        if (pthread_mutex_init(&entries[i].mutex, NULL) != 0) goto bad_manifest;
        initialized++;
    }
    size_t off = 4;
    for (uint32_t i = 0; i < count; ++i) {
        if (off + 4 > f.length) goto bad_manifest;
        uint32_t len = it305_get_u32(f.payload + off); off += 4;
        if (!len || len > IT305_MAX_FILENAME || off + len + 8 > f.length) goto bad_manifest;
        if (memchr(f.payload + off, '\0', len) != NULL) goto bad_manifest;
        memcpy(entries[i].filename, f.payload + off, len); entries[i].filename[len] = '\0'; off += len;
        if (!it305_valid_component(entries[i].filename)) goto bad_manifest;
        entries[i].file_size = it305_get_u64(f.payload + off); off += 8;
    }
    if (off != f.length) goto bad_manifest;
    it305_frame_free(&f);
    *sock_out = sock; *entries_out = entries; *count_out = count;
    return 0;
bad_manifest:
    for (uint32_t j = 0; j < initialized; ++j) pthread_mutex_destroy(&entries[j].mutex);
    free(entries); it305_frame_free(&f); close(sock); errno = EPROTO; return -1;
}

/* Enhanced checkpoint = append-only journal ".checkpoint.tmp":
 *     V3\n<topic>\n        header
 *     D\t<filename>\n       one line per completed file, appended exactly once
 * Cost is O(1) per completed file and nothing is written per chunk. Progress inside an unfinished file is
 * recovered from that file's length on disk, because every file is written strictly sequentially. */
static int load_enhanced_checkpoint(EnhancedContext *ctx) {
    FILE *f = fopen(ctx->checkpoint_path, "r");
    if (!f) return 0;
    char *line = NULL; size_t cap = 0; ssize_t n;
    int valid = 0;
    n = getline(&line, &cap, f);
    if (n > 0 && strcmp(line, "V3\n") == 0) {
        n = getline(&line, &cap, f);
        if (n > 0) { if (line[n - 1] == '\n') line[n - 1] = '\0'; valid = strcmp(line, ctx->topic) == 0; }
    }
    if (valid) {
        while ((n = getline(&line, &cap, f)) > 0) {
            if (line[n - 1] == '\n') line[n - 1] = '\0';
            if (line[0] != 'D' || line[1] != '\t') continue;
            for (size_t j = 0; j < ctx->entry_count; ++j)
                if (strcmp(ctx->entries[j].filename, line + 2) == 0) { ctx->entries[j].completed = 1; break; }
        }
    }
    free(line); fclose(f);
    if (!valid) return 0;
    for (size_t j = 0; j < ctx->entry_count; ++j) {   /* reconcile with what is really on disk */
        EnhancedEntry *e = &ctx->entries[j];
        char path[PATH_MAX]; struct stat st; int have = 0; uint64_t local = 0;
        if (it305_path_join(path, sizeof(path), ctx->topic_dir, e->filename) == 0 && stat(path, &st) == 0 && st.st_size >= 0) {
            have = 1; local = (uint64_t)st.st_size;
        }
        if (have && local == e->file_size) e->completed = 1;           /* fully written (journal line may be missing) */
        else if (e->completed) e->completed = 0;                        /* journal claims done but file is not */
        e->bytes_received = e->completed ? e->file_size : (have && local < e->file_size ? local : 0);
    }
    return 1;
}

static void open_enhanced_journal(EnhancedContext *ctx) {
    char header[IT305_MAX_TOPIC + 16];
    int hn = snprintf(header, sizeof(header), "V3\n%s\n", ctx->topic);
    ctx->journal_fd = open(ctx->checkpoint_path, O_WRONLY | O_CREAT | O_APPEND | O_CLOEXEC, 0644);
    if (ctx->journal_fd < 0) return;
    struct stat st;
    if (fstat(ctx->journal_fd, &st) == 0 && st.st_size == 0 && hn > 0) {
        ssize_t w = write(ctx->journal_fd, header, (size_t)hn); (void)w;
    }
}

static void journal_append_done(EnhancedContext *ctx, const char *filename) {
    if (ctx->journal_fd < 0) return;
    char line[IT305_MAX_FILENAME + 8];
    int n = snprintf(line, sizeof(line), "D\t%s\n", filename);
    if (n <= 0 || (size_t)n >= sizeof(line)) return;
    ssize_t w = write(ctx->journal_fd, line, (size_t)n);   /* single O_APPEND write: atomic w.r.t. other workers */
    (void)w;
}

static void update_enhanced_entry(EnhancedContext *ctx, EnhancedEntry *e, uint64_t offset, int completed) {
    pthread_mutex_lock(&e->mutex);      /* per-entry lock only: workers never contend with each other */
    e->bytes_received = offset;
    if (completed) e->completed = 1;
    pthread_mutex_unlock(&e->mutex);
    if (completed) journal_append_done(ctx, e->filename);
}

/* ---- Enhanced worker: pipelined per-file requests over one persistent connection --------------------------------
 * A worker keeps up to W file requests in flight on its connection (W = IT305_ENH_WINDOW, default 8). The server
 * answers requests strictly in order, so while file N is still being received the requests for N+1..N+W-1 are
 * already queued at the server and the connection never idles for a round trip between files. Stop-and-wait (one
 * request, wait for the whole reply, next request) costs one RTT per file, which on a topic of thousands of small
 * images is slower than a single streaming connection as soon as there is any network latency.
 * After a dropped connection only the head file can have partial progress; it is resumed with CMD_RESUME and the
 * remaining in-flight files are simply requested again from byte 0. */
#define ENH_MAX_WINDOW 64

static int enh_window(void) {
    static int cached = 0;             /* atomics: called from every worker thread */
    int v = __atomic_load_n(&cached, __ATOMIC_RELAXED);
    if (!v) {
        const char *e = getenv("IT305_ENH_WINDOW");
        v = e ? atoi(e) : 8;
        if (v < 1) v = 1;
        if (v > ENH_MAX_WINDOW) v = ENH_MAX_WINDOW;
        __atomic_store_n(&cached, v, __ATOMIC_RELAXED);
    }
    return v;
}

/* next entry of the shared queue that is not already complete, or -1 */
static long enh_claim(EnhancedContext *ctx) {
    for (;;) {
        pthread_mutex_lock(&ctx->queue_mutex);
        if (__atomic_load_n(&ctx->failed, __ATOMIC_RELAXED) || ctx->next_entry >= ctx->entry_count) { pthread_mutex_unlock(&ctx->queue_mutex); return -1; }
        size_t i = ctx->next_entry++;
        pthread_mutex_unlock(&ctx->queue_mutex);
        pthread_mutex_lock(&ctx->entries[i].mutex);
        int done = ctx->entries[i].completed;
        pthread_mutex_unlock(&ctx->entries[i].mutex);
        if (!done) return (long)i;
    }
}

static int enh_send_request(EnhancedContext *ctx, EnhancedEntry *e, int sock) {
    pthread_mutex_lock(&e->mutex);
    uint64_t off = e->bytes_received;
    pthread_mutex_unlock(&e->mutex);
    return off ? send_resume(sock, ctx->topic, e->filename, off) : send_get_file(sock, ctx->topic, e->filename);
}

enum { ENH_DONE = 1, ENH_LOST = 0, ENH_FATAL = -1, ENH_RESTART = 2 };

/* Receive the reply for entry e (which must be the oldest outstanding request on this connection). */
static int enh_receive_file(EnhancedContext *ctx, EnhancedEntry *e, int sock) {
    pthread_mutex_lock(&e->mutex);
    uint64_t offset = e->bytes_received;
    pthread_mutex_unlock(&e->mutex);
    char path[PATH_MAX];
    if (it305_path_join(path, sizeof(path), ctx->topic_dir, e->filename) != 0) return ENH_FATAL;
    char filename[IT305_MAX_FILENAME + 1] = "";
    uint64_t file_size = 0, current_offset = offset;
    int file_fd = -1, got_header = 0, rc = ENH_LOST;
    for (;;) {
        It305Frame f;
        if (it305_recv_frame(sock, &f) != 1) { rc = ENH_LOST; break; }
        if (f.type == MSG_ERR_BAD_OFFSET) { it305_frame_free(&f); rc = ENH_RESTART; break; }
        if (f.type == MSG_ERR_NOT_FOUND || f.type == MSG_ERR_PROTOCOL) {
            fprintf(stderr, "Enhanced transfer error for %s\n", e->filename);
            it305_frame_free(&f); rc = ENH_FATAL; break;
        }
        if (f.type == MSG_FILE_COMPLETE) {
            it305_frame_free(&f);
            update_enhanced_entry(ctx, e, e->file_size, 1);
            rc = ENH_DONE; break;
        }
        if (f.type == MSG_FILE_HEADER) {
            if (parse_file_header(&f, filename, sizeof(filename), &file_size, &current_offset) != 0 ||
                strcmp(filename, e->filename) != 0 || file_size != e->file_size || current_offset != offset) {
                it305_frame_free(&f); rc = ENH_FATAL; break;
            }
            it305_frame_free(&f);
            if (file_fd >= 0) close(file_fd);
            file_fd = open(path, O_CREAT | O_RDWR | O_CLOEXEC, 0644);
            if (file_fd < 0) { perror("open enhanced output file"); rc = ENH_FATAL; break; }
            if (offset == 0 && ftruncate(file_fd, 0) != 0) { rc = ENH_FATAL; break; }
            got_header = 1;
            continue;
        }
        if (f.type == MSG_FILE_DATA) {
            if (!got_header || file_fd < 0 || f.length > IT305_CHUNK_SIZE || current_offset + f.length > file_size) {
                it305_frame_free(&f); rc = ENH_LOST; break;
            }
            if (f.length && write_at(file_fd, f.payload, f.length, current_offset) != 0) {
                it305_frame_free(&f); rc = ENH_FATAL; break;
            }
            current_offset += f.length;
            __sync_add_and_fetch(&ctx->received_payload_bytes, f.length);
            __sync_add_and_fetch(&ctx->useful_payload_bytes, f.length);
            update_enhanced_entry(ctx, e, current_offset, 0);
            it305_frame_free(&f);
            continue;
        }
        it305_frame_free(&f);
        rc = ENH_LOST; break;
    }
    if (file_fd >= 0) close(file_fd);
    return rc;
}

/* One line per lost connection, like the Case 1/Case 2 clients print, so a live demo shows the recovery. */
static void enh_note_loss(EnhancedContext *ctx, long head_index, int in_flight) {
    EnhancedEntry *e = &ctx->entries[head_index];
    pthread_mutex_lock(&e->mutex);
    unsigned long long off = e->bytes_received;
    pthread_mutex_unlock(&e->mutex);
    fprintf(stderr, "Connection interrupted; resuming %s at offset %llu (%d request(s) re-issued)\n",
            e->filename, off, in_flight);
}

static void enh_fail(EnhancedContext *ctx) {
    pthread_mutex_lock(&ctx->queue_mutex);
    __atomic_store_n(&ctx->failed, 1, __ATOMIC_RELAXED);      /* read without the lock by the other workers */
    pthread_mutex_unlock(&ctx->queue_mutex);
}

static void *enhanced_worker(void *arg) {
    EnhancedWorkerArgs *wa = (EnhancedWorkerArgs *)arg;
    EnhancedContext *ctx = wa->ctx;
    free(wa);
    const int W = enh_window();
    long ring[ENH_MAX_WINDOW];
    int head = 0, count = 0, sock = -1;
    unsigned long consecutive_losses = 0;
    for (;;) {
        if (__atomic_load_n(&ctx->failed, __ATOMIC_RELAXED)) break;
        /* keep the pipeline full: claim more files and, if connected, send their requests immediately */
        while (count < W) {
            long i = enh_claim(ctx);
            if (i < 0) break;
            ring[(head + count) % ENH_MAX_WINDOW] = i;
            count++;
            if (sock >= 0 && enh_send_request(ctx, &ctx->entries[i], sock) != 0) {
                /* with pipelining a dropped connection is usually noticed here, while sending, not while receiving */
                close(sock); sock = -1;
                __sync_add_and_fetch(&ctx->retries, 1);
                enh_note_loss(ctx, ring[head], count);
                break;
            }
        }
        if (count == 0) break;                       /* queue drained and nothing in flight */
        if (sock < 0) {                              /* (re)connect and re-issue every outstanding request in order */
            sock = connect_with_budget(ctx->server_ip, ctx->port, 1);
            if (sock < 0) { enh_fail(ctx); break; }       /* server gone for longer than the connection budget */
            for (int k = 0; k < count; ++k) {
                if (enh_send_request(ctx, &ctx->entries[ring[(head + k) % ENH_MAX_WINDOW]], sock) != 0) { close(sock); sock = -1; break; }
            }
            if (sock < 0) { __sync_add_and_fetch(&ctx->retries, 1); continue; }
        }
        EnhancedEntry *e = &ctx->entries[ring[head]];
        int rc = enh_receive_file(ctx, e, sock);
        if (rc == ENH_DONE) { head = (head + 1) % ENH_MAX_WINDOW; count--; consecutive_losses = 0; continue; }
        close(sock); sock = -1;
        if (rc == ENH_FATAL) { enh_fail(ctx); break; }
        if (rc == ENH_LOST) enh_note_loss(ctx, ring[head], count);
        if (rc == ENH_RESTART) {                     /* server rejected our resume offset: drop partial data, start the file over */
            char path[PATH_MAX];
            if (it305_path_join(path, sizeof(path), ctx->topic_dir, e->filename) == 0) {
                int fd = open(path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0644);
                if (fd >= 0) close(fd);
            }
            update_enhanced_entry(ctx, e, 0, 0);
        }
        __sync_add_and_fetch(&ctx->retries, 1);
        if (++consecutive_losses > MAX_RETRIES) { enh_fail(ctx); break; }
        sleep_ms(reconnect_delay_ms());
    }
    if (sock >= 0) close(sock);
    return NULL;
}

static int client_enhanced_mode(const char *host, int port, const char *topic,
                                const char *topic_dir, const char *checkpoint_path, int workers) {
    uint64_t started = monotonic_ns();
    EnhancedEntry *entries = NULL; size_t count = 0; int manifest_sock = -1;
    if (request_manifest(host, port, topic, &manifest_sock, &entries, &count) != 0) {
        fprintf(stderr, "Could not fetch topic manifest\n"); return 1;
    }
    close(manifest_sock);
    EnhancedContext ctx;
    memset(&ctx, 0, sizeof(ctx));
    snprintf(ctx.server_ip, sizeof(ctx.server_ip), "%s", host);
    ctx.port = port; snprintf(ctx.topic, sizeof(ctx.topic), "%s", topic);
    snprintf(ctx.topic_dir, sizeof(ctx.topic_dir), "%s", topic_dir);
    snprintf(ctx.checkpoint_path, sizeof(ctx.checkpoint_path), "%s", checkpoint_path);
    ctx.entries = entries; ctx.entry_count = count;
    pthread_mutex_init(&ctx.queue_mutex, NULL);
    pthread_mutex_init(&ctx.checkpoint_mutex, NULL);
    if (!load_enhanced_checkpoint(&ctx)) unlink(checkpoint_path);   /* ignore journals of another topic/run */
    open_enhanced_journal(&ctx);
    /* Reconcile checkpoint entries with the actual local file lengths before workers start. */
    for (size_t i = 0; i < count; ++i) {
        char path[PATH_MAX]; struct stat st;
        uint64_t actual = 0;
        if (it305_path_join(path, sizeof(path), topic_dir, entries[i].filename) == 0 && stat(path, &st) == 0 && st.st_size >= 0)
            actual = (uint64_t)st.st_size;
        pthread_mutex_lock(&entries[i].mutex);
        if (entries[i].bytes_received > actual) entries[i].bytes_received = actual;
        if (entries[i].bytes_received > entries[i].file_size) entries[i].bytes_received = 0;
        if (entries[i].completed && actual != entries[i].file_size) entries[i].completed = 0;
        pthread_mutex_unlock(&entries[i].mutex);
    }
    if (workers < 1) workers = 1;
    if ((size_t)workers > count && count > 0) workers = (int)count;
    if (count == 0) workers = 0;
    pthread_t *threads = calloc((size_t)(workers ? workers : 1), sizeof(*threads));
    int created = 0;
    for (int i = 0; i < workers; ++i) {
        EnhancedWorkerArgs *wa = malloc(sizeof(*wa));
        if (!wa) { ctx.failed = 1; break; }
        wa->ctx = &ctx;
        if (pthread_create(&threads[i], NULL, enhanced_worker, wa) != 0) { free(wa); ctx.failed = 1; break; }
        created++;
    }
    for (int i = 0; i < created; ++i) pthread_join(threads[i], NULL);
    free(threads);
    if (ctx.journal_fd >= 0) close(ctx.journal_fd);
    int failed = ctx.failed;
    if (!failed) unlink(checkpoint_path);
    uint64_t ended = monotonic_ns();
    double elapsed = ended > started ? (double)(ended - started) / 1e9 : 0.0;
    uint64_t total_source = 0;
    for (size_t i = 0; i < count; ++i) total_source += entries[i].file_size;
    uint64_t on_disk = 0; (void)sum_regular_files(topic_dir, &on_disk);
    uint64_t useful = ctx.received_payload_bytes < on_disk ? ctx.received_payload_bytes : on_disk;
    print_result("Part2_Case2_Enhanced", elapsed, ctx.received_payload_bytes, useful,
                 ctx.received_payload_bytes - useful, ctx.retries);
    if (!failed) printf("Transfer completed: topic=%s files=%zu source_bytes=%llu directory=%s\n",
                        topic, count, (unsigned long long)total_source, topic_dir);
    for (size_t i = 0; i < count; ++i) pthread_mutex_destroy(&entries[i].mutex);
    free(entries);
    pthread_mutex_destroy(&ctx.queue_mutex);
    pthread_mutex_destroy(&ctx.checkpoint_mutex);
    return failed ? 1 : 0;
}

int it305_client_main(int argc, char **argv, int case_id) {
    if (argc != 5 && !(case_id == CASE_ENHANCED && argc == 6)) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>%s\n", argv[0],
                case_id == CASE_ENHANCED ? " [parallel_connections]" : ""); return 2;
    }
    int port;
    if (parse_port(argv[2], &port) != 0) { fprintf(stderr, "server_port must be in range 1..65535\n"); return 2; }
    if (!it305_valid_component(argv[3]) || strlen(argv[3]) > IT305_MAX_TOPIC) { fprintf(stderr, "invalid topic name\n"); return 2; }
    int workers = 4;
    if (case_id == CASE_ENHANCED && argc == 6) {
        char *end = NULL; long v = strtol(argv[5], &end, 10);
        if (!argv[5][0] || !end || *end || v < 1 || v > 64) { fprintf(stderr, "parallel_connections must be 1..64\n"); return 2; }
        workers = (int)v;
    }
    char topic_dir[PATH_MAX], checkpoint_path[PATH_MAX];
    if (make_client_paths(argv[4], argv[3], topic_dir, sizeof(topic_dir), checkpoint_path, sizeof(checkpoint_path)) != 0) return 1;
    signal(SIGPIPE, SIG_IGN);
    if (case_id == CASE_PART1 || case_id == CASE_RESTART)
        return client_restart_mode(argv[1], port, argv[3], topic_dir, checkpoint_path, case_id);
    if (case_id == CASE_RESUME)
        return client_resume_mode(argv[1], port, argv[3], topic_dir, checkpoint_path);
    if (case_id == CASE_ENHANCED)
        return client_enhanced_mode(argv[1], port, argv[3], topic_dir, checkpoint_path, workers);
    return 2;
}
