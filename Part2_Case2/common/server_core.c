#define _POSIX_C_SOURCE 200809L
#define _FILE_OFFSET_BITS 64
#include "server_core.h"
#include "protocol.h"
#include "io_helpers.h"

#include <arpa/inet.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <signal.h>
#include <stdint.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define CASE_PART1 1
#define CASE_RESTART 2
#define CASE_RESUME 3
#define CASE_ENHANCED 4
#define REGISTRY_INITIAL_CAP 16u

typedef struct {
    char filename[IT305_MAX_FILENAME + 1];
    char absolute_path[PATH_MAX];
    uint64_t file_size;
} FileMetadata;

typedef struct {
    char topic_name[IT305_MAX_TOPIC + 1];
    char dir_path[PATH_MAX];
    FileMetadata *files;
    size_t file_count;
    size_t file_capacity;
    uint64_t total_size_bytes;
} TopicRecord;

typedef struct {
    TopicRecord *topics;
    size_t count;
    size_t capacity;
    size_t *hash_slots; /* stores topic index + 1; zero is empty */
    size_t hash_capacity;
    pthread_rwlock_t rwlock;
} TopicRegistry;

typedef struct {
    int sock;
    int case_id;
    double failure_probability;
    TopicRegistry *registry;
    unsigned int seed;
} WorkerArgs;

/* Flags set by signal handlers and read by the accept loop. They are accessed with atomics (C11 data-race freedom) and the
   signals are blocked in every worker thread, so only the main thread runs the handlers and accept() is the call that gets
   interrupted. */
static volatile sig_atomic_t g_stop = 0;
static volatile sig_atomic_t g_reload = 0;   /* set by SIGHUP; acted on by the accept loop */
static int g_listen_fd = -1;
static int g_enhanced_sendfile = 1; /* Enhanced only: 1 = sendfile() zero-copy, 0 = pread()+send (ablation baseline) */
static int g_active_workers = 0;   /* connections currently being served (for graceful shutdown) */

/* ---- structured event log -----------------------------------------------------------------------------------
 * "<timestamp ms> [thread] [client ip:port] ACTION details" on stderr and, if IT305_LOG_FILE is set, appended to
 * that file (line-flushed). IT305_LOG_LEVEL=debug additionally logs every connection and request. */
static pthread_mutex_t g_log_mu = PTHREAD_MUTEX_INITIALIZER;
static FILE *g_log_file = NULL;
static int g_log_debug = 0;

static void log_event(int debug_only, const char *action, int sock, const char *fmt, ...) {
    if (debug_only && !g_log_debug) return;
    char who[64] = "-";
    if (sock >= 0) {
        struct sockaddr_in in; socklen_t sl = sizeof(in);
        if (getpeername(sock, (struct sockaddr *)&in, &sl) == 0 && in.sin_family == AF_INET) {
            char ip[INET_ADDRSTRLEN];
            if (inet_ntop(AF_INET, &in.sin_addr, ip, sizeof(ip))) snprintf(who, sizeof(who), "%s:%u", ip, (unsigned)ntohs(in.sin_port));
        }
    }
    struct timespec ts; struct tm tmv; char stamp[40];
    clock_gettime(CLOCK_REALTIME, &ts);
    localtime_r(&ts.tv_sec, &tmv);
    strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%S", &tmv);
    char msg[512];
    va_list ap; va_start(ap, fmt); vsnprintf(msg, sizeof(msg), fmt, ap); va_end(ap);
    pthread_mutex_lock(&g_log_mu);
    fprintf(stderr, "%s.%03ld [thread %lx] [%s] %s %s\n", stamp, ts.tv_nsec / 1000000, (unsigned long)pthread_self(), who, action, msg);
    fflush(stderr);
    if (g_log_file) {
        fprintf(g_log_file, "%s.%03ld [thread %lx] [%s] %s %s\n", stamp, ts.tv_nsec / 1000000, (unsigned long)pthread_self(), who, action, msg);
        fflush(g_log_file);
    }
    pthread_mutex_unlock(&g_log_mu);
}

static uint64_t realtime_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_REALTIME, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t monotonic_ns(void) {
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) return 0;
    return (uint64_t)ts.tv_sec * 1000000000ull + (uint64_t)ts.tv_nsec;
}

static uint64_t hash_string(const char *s) {
    uint64_t h = 1469598103934665603ull;
    while (*s) { h ^= (unsigned char)*s++; h *= 1099511628211ull; }
    return h;
}

static int cmp_file_meta(const void *a, const void *b) {
    const FileMetadata *x = (const FileMetadata *)a;
    const FileMetadata *y = (const FileMetadata *)b;
    return strcmp(x->filename, y->filename);
}

static int cmp_topic_meta(const void *a, const void *b) {
    const TopicRecord *x = (const TopicRecord *)a;
    const TopicRecord *y = (const TopicRecord *)b;
    return strcmp(x->topic_name, y->topic_name);
}

static void free_registry(TopicRegistry *r) {
    if (!r) return;
    for (size_t i = 0; i < r->count; ++i) free(r->topics[i].files);
    free(r->topics);
    free(r->hash_slots);
    pthread_rwlock_destroy(&r->rwlock);
    memset(r, 0, sizeof(*r));
}

static int add_file(TopicRecord *t, const char *name, const char *path, uint64_t size) {
    if (strlen(name) > IT305_MAX_FILENAME) return 0; /* skip names the protocol cannot represent */
    if (t->file_count == t->file_capacity) {
        size_t cap = t->file_capacity ? t->file_capacity * 2 : 16;
        FileMetadata *tmp = realloc(t->files, cap * sizeof(*tmp));
        if (!tmp) return -1;
        t->files = tmp;
        t->file_capacity = cap;
    }
    FileMetadata *f = &t->files[t->file_count++];
    memset(f, 0, sizeof(*f));
    snprintf(f->filename, sizeof(f->filename), "%s", name);
    snprintf(f->absolute_path, sizeof(f->absolute_path), "%s", path);
    f->file_size = size;
    t->total_size_bytes += size;
    return 0;
}

static int scan_topic_directory(TopicRecord *t) {
    DIR *dir = opendir(t->dir_path);
    if (!dir) return -1;
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
        if (!it305_valid_component(de->d_name)) continue;
        char path[PATH_MAX];
        if (it305_path_join(path, sizeof(path), t->dir_path, de->d_name) != 0) continue;
        struct stat st;
        if (lstat(path, &st) != 0 || !S_ISREG(st.st_mode)) continue;
        if (st.st_size < 0) continue;
        int rc = add_file(t, de->d_name, path, (uint64_t)st.st_size);
        if (rc != 0) { closedir(dir); return -1; }
    }
    closedir(dir);
    if (t->file_count > 1) qsort(t->files, t->file_count, sizeof(*t->files), cmp_file_meta);   /* files is NULL for an empty topic: qsort(NULL,..) is undefined */
    return 0;
}

static int registry_load(TopicRegistry *r, const char *root) {
    char root_real[PATH_MAX];
    memset(r, 0, sizeof(*r));
    if (!realpath(root, root_real)) {
        fprintf(stderr, "ERROR: cannot resolve topics root '%s': %s\n", root, strerror(errno));
        return -1;
    }
    if (pthread_rwlock_init(&r->rwlock, NULL) != 0) return -1;
    DIR *dir = opendir(root_real);
    if (!dir) { perror("opendir topics root"); return -1; }
    struct dirent *de;
    while ((de = readdir(dir)) != NULL) {
        if (!strcmp(de->d_name, ".") || !strcmp(de->d_name, "..")) continue;
        if (!it305_valid_component(de->d_name) || strlen(de->d_name) > IT305_MAX_TOPIC) continue;
        char candidate[PATH_MAX], resolved[PATH_MAX];
        if (it305_path_join(candidate, sizeof(candidate), root_real, de->d_name) != 0) continue;
        struct stat st;
        if (lstat(candidate, &st) != 0 || !S_ISDIR(st.st_mode)) continue;
        if (!realpath(candidate, resolved)) continue;
        if (r->count == r->capacity) {
            size_t cap = r->capacity ? r->capacity * 2 : REGISTRY_INITIAL_CAP;
            TopicRecord *tmp = realloc(r->topics, cap * sizeof(*tmp));
            if (!tmp) { closedir(dir); return -1; }
            r->topics = tmp;
            r->capacity = cap;
        }
        TopicRecord *t = &r->topics[r->count];
        memset(t, 0, sizeof(*t));
        snprintf(t->topic_name, sizeof(t->topic_name), "%s", de->d_name);
        snprintf(t->dir_path, sizeof(t->dir_path), "%s", resolved);
        if (scan_topic_directory(t) != 0) {
            fprintf(stderr, "WARNING: failed to scan topic directory '%s'\n", resolved);
            free(t->files);
            memset(t, 0, sizeof(*t));
            continue;
        }
        r->count++;
    }
    closedir(dir);
    if (r->count > 1) qsort(r->topics, r->count, sizeof(*r->topics), cmp_topic_meta);
    size_t hcap = 16;
    while (hcap < r->count * 2 + 1) hcap <<= 1;
    r->hash_slots = calloc(hcap, sizeof(*r->hash_slots));
    if (!r->hash_slots) return -1;
    r->hash_capacity = hcap;
    for (size_t i = 0; i < r->count; ++i) {
        size_t slot = (size_t)(hash_string(r->topics[i].topic_name) & (hcap - 1));
        while (r->hash_slots[slot] != 0) slot = (slot + 1) & (hcap - 1);
        r->hash_slots[slot] = i + 1;
    }
    fprintf(stdout, "[STARTUP] topics=%zu root=%s\n", r->count, root_real);
    for (size_t i = 0; i < r->count; ++i)
        fprintf(stdout, "[TOPIC] name=%s files=%zu bytes=%llu\n", r->topics[i].topic_name,
                r->topics[i].file_count, (unsigned long long)r->topics[i].total_size_bytes);
    fflush(stdout);
    return 0;
}

/* ---- SIGHUP reload ---------------------------------------------------------------------------------------------
 * The topic directory is re-scanned and swapped in under the write lock. Transfers that are already running keep
 * pointers into the previous generation, so it must not be freed while any connection exists. Retired generations are
 * therefore released as soon as the server is idle (checked at every reload and at shutdown). */
typedef struct { TopicRecord *topics; size_t count; size_t *slots; } RetiredGen;
static RetiredGen *g_retired = NULL;
static size_t g_retired_n = 0;

static void free_retired(void) {
    for (size_t i = 0; i < g_retired_n; ++i) {
        for (size_t j = 0; j < g_retired[i].count; ++j) free(g_retired[i].topics[j].files);
        free(g_retired[i].topics); free(g_retired[i].slots);
    }
    free(g_retired); g_retired = NULL; g_retired_n = 0;
}

static int registry_reload(TopicRegistry *r, const char *root, int active_workers) {
    TopicRegistry fresh;
    if (registry_load(&fresh, root) != 0) return -1;     /* keep serving the old topics if the scan fails */
    pthread_rwlock_wrlock(&r->rwlock);
    RetiredGen *tmp = realloc(g_retired, (g_retired_n + 1) * sizeof(*g_retired));
    if (tmp) {
        g_retired = tmp;
        g_retired[g_retired_n++] = (RetiredGen){ r->topics, r->count, r->hash_slots };
    }                                                    /* if realloc failed the old generation simply leaks */
    r->topics = fresh.topics; r->count = fresh.count; r->capacity = fresh.capacity;
    r->hash_slots = fresh.hash_slots; r->hash_capacity = fresh.hash_capacity;
    pthread_rwlock_unlock(&r->rwlock);
    pthread_rwlock_destroy(&fresh.rwlock);
    if (active_workers == 0) free_retired();             /* nobody can still be using an old generation */
    return 0;
}

static TopicRecord *registry_find(TopicRegistry *r, const char *name) {
    TopicRecord *found = NULL;
    if (!r || !name || !r->hash_capacity) return NULL;
    pthread_rwlock_rdlock(&r->rwlock);
    size_t slot = (size_t)(hash_string(name) & (r->hash_capacity - 1));
    for (size_t seen = 0; seen < r->hash_capacity; ++seen) {
        size_t index = r->hash_slots[slot];
        if (index == 0) break;
        if (!strcmp(r->topics[index - 1].topic_name, name)) {
            found = &r->topics[index - 1];
            break;
        }
        slot = (slot + 1) & (r->hash_capacity - 1);
    }
    if (!found) {   /* exact match wins; otherwise fall back to a case-insensitive match (handout capitalises topic names) */
        for (size_t i = 0; i < r->count; ++i)
            if (strcasecmp(r->topics[i].topic_name, name) == 0) { found = &r->topics[i]; break; }
    }
    pthread_rwlock_unlock(&r->rwlock);
    return found;
}

static long find_file_index(const TopicRecord *t, const char *name) {
    size_t lo = 0, hi = t->file_count;
    while (lo < hi) {
        size_t mid = lo + (hi - lo) / 2;
        int c = strcmp(t->files[mid].filename, name);
        if (c == 0) return (long)mid;
        if (c < 0) lo = mid + 1; else hi = mid;
    }
    return -1;
}

static void send_error(int sock, uint8_t type, uint16_t status, const char *msg) {
    log_event(0, "ERROR_REPLY", sock, "type=0x%02x status=%u reason=\"%s\"", (unsigned)type, (unsigned)status, msg ? msg : "");
    size_t n = strlen(msg);
    if (n > 1024) n = 1024;
    (void)it305_send_frame(sock, type, 0, status, msg, (uint32_t)n);
}

static int fault_now(int sock, double p, unsigned int *seed, const char *filename, uint64_t offset) {
    if (p <= 0.0) return 0;
    double r = (double)rand_r(seed) / ((double)RAND_MAX + 1.0);
    if (r >= p) return 0;
    log_event(0, "FAULT_INJECTION", sock, "drop file=%s offset=%llu probability=%.6f",
              filename ? filename : "(none)", (unsigned long long)offset, p);
    struct linger sl;
    sl.l_onoff = 1;
    sl.l_linger = 0;
    (void)setsockopt(sock, SOL_SOCKET, SO_LINGER, &sl, sizeof(sl));
    shutdown(sock, SHUT_RDWR);
    return 1;
}

static int send_file_header(int sock, const FileMetadata *f, uint64_t start_offset) {
    size_t name_len = strlen(f->filename);
    uint32_t n = (uint32_t)(20 + name_len);
    unsigned char *p = malloc(n);
    if (!p) return -1;
    it305_put_u64(p, f->file_size);
    it305_put_u64(p + 8, start_offset);
    it305_put_u32(p + 16, (uint32_t)name_len);
    memcpy(p + 20, f->filename, name_len);
    int rc = it305_send_frame(sock, MSG_FILE_HEADER, 0, 0, p, n);
    free(p);
    return rc;
}

static int send_data_header_only(int sock, uint8_t flags, uint32_t length) {
    unsigned char h[IT305_HEADER_SIZE];
    h[0] = MSG_FILE_DATA; h[1] = flags; h[2] = 0; h[3] = 0;
    it305_put_u32(h + 4, length);
    return it305_send_all_more(sock, h, sizeof(h));   /* MSG_MORE: header is corked with the sendfile() payload */
}

static int stream_file_framed(int sock, const FileMetadata *f, uint64_t offset, int case_id,
                              double fail_p, unsigned int *seed, uint64_t *data_bytes) {
    int fd = open(f->absolute_path, O_RDONLY | O_CLOEXEC);
    if (fd < 0) return -1;
    struct stat st;
    if (fstat(fd, &st) != 0 || !S_ISREG(st.st_mode) || st.st_size < 0 ||
        (uint64_t)st.st_size != f->file_size || offset > f->file_size) { close(fd); errno = EINVAL; return -1; }
    if (send_file_header(sock, f, offset) != 0) { close(fd); return -1; }
    if (fault_now(sock, fail_p, seed, f->filename, offset)) { close(fd); return -2; }
    uint64_t current = offset;
    if (current == f->file_size) {
        close(fd);
        return it305_send_frame(sock, MSG_FILE_DATA, MSG_FLAG_FINAL_CHUNK, 0, NULL, 0) == 0 ? 0 : -1;
    }
    while (current < f->file_size) {
        if (fault_now(sock, fail_p, seed, f->filename, current)) { close(fd); return -2; }
        size_t wanted = (size_t)((f->file_size - current) > IT305_CHUNK_SIZE ? IT305_CHUNK_SIZE : (f->file_size - current));
        uint8_t flags = ((current + wanted) == f->file_size) ? MSG_FLAG_FINAL_CHUNK : 0;
        if (case_id == CASE_ENHANCED && g_enhanced_sendfile) {
            /* Keep the protocol's per-chunk 8-byte header, then use sendfile for the payload. */
            if (send_data_header_only(sock, flags, (uint32_t)wanted) != 0) { close(fd); return -1; }
            off_t file_off = (off_t)current;
            size_t remaining = wanted;
            while (remaining) {
                ssize_t n = sendfile(sock, fd, &file_off, remaining);
                if (n < 0 && errno == EINTR) continue;
                if (n <= 0) { close(fd); return -1; }
                remaining -= (size_t)n;
                current += (uint64_t)n;
                *data_bytes += (uint64_t)n;
            }
        } else {
            unsigned char *buf = malloc(wanted);
            if (!buf) { close(fd); return -1; }
            ssize_t n;
            do { n = pread(fd, buf, wanted, (off_t)current); } while (n < 0 && errno == EINTR);
            if (n <= 0 || (size_t)n != wanted) { free(buf); close(fd); return -1; }
            int rc = it305_send_frame(sock, MSG_FILE_DATA, flags, 0, buf, (uint32_t)wanted);
            free(buf);
            if (rc != 0) { close(fd); return -1; }
            current += wanted;
            *data_bytes += wanted;
        }
    }
    close(fd);
    return 0;
}

static int send_topic(int sock, TopicRecord *t, size_t start_index, uint64_t resume_offset,
                      int single_file, int case_id, double fail_p, unsigned int *seed,
                      uint64_t *data_bytes) {
    for (size_t i = start_index; i < t->file_count; ++i) {
        uint64_t offset = (i == start_index) ? resume_offset : 0;
        int rc = stream_file_framed(sock, &t->files[i], offset, case_id, fail_p, seed, data_bytes);
        if (rc == -2) return -2;
        if (rc != 0) return -1;
        if (single_file) return it305_send_frame(sock, MSG_FILE_COMPLETE, 0, 0, NULL, 0);
    }
    return it305_send_frame(sock, MSG_END_OF_TOPIC, 0, 0, NULL, 0);
}

static int send_manifest(int sock, TopicRecord *t) {
    uint64_t total = 4;
    for (size_t i = 0; i < t->file_count; ++i) total += 4 + strlen(t->files[i].filename) + 8;
    if (total > IT305_MAX_PAYLOAD) { errno = EOVERFLOW; return -1; }
    unsigned char *p = malloc((size_t)total);
    if (!p) return -1;
    it305_put_u32(p, (uint32_t)t->file_count);
    size_t off = 4;
    for (size_t i = 0; i < t->file_count; ++i) {
        uint32_t n = (uint32_t)strlen(t->files[i].filename);
        it305_put_u32(p + off, n); off += 4;
        memcpy(p + off, t->files[i].filename, n); off += n;
        it305_put_u64(p + off, t->files[i].file_size); off += 8;
    }
    int rc = it305_send_frame(sock, MSG_MANIFEST_DATA, 0, 0, p, (uint32_t)total);
    free(p);
    return rc;
}

static int copy_string_field(char *dst, size_t dst_size, const unsigned char *src, uint32_t n) {
    if (n == 0 || n >= dst_size || memchr(src, '\0', n) != NULL) return -1;
    memcpy(dst, src, n);
    dst[n] = '\0';
    return it305_valid_component(dst) ? 0 : -1;
}

static int handle_request(int sock, WorkerArgs *w, It305Frame *req, uint64_t *data_bytes) {
    if (req->type == MSG_CMD_GET) {
        char topic[IT305_MAX_TOPIC + 1];
        if (copy_string_field(topic, sizeof(topic), req->payload, req->length) != 0) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "invalid topic"); return -1;
        }
        TopicRecord *t = registry_find(w->registry, topic);
        if (!t) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "topic not found"); return -1; }
        int rc = send_topic(sock, t, 0, 0, 0, w->case_id, w->failure_probability, &w->seed, data_bytes);
        return rc == 0 ? 0 : -1;
    }
    if (req->type == MSG_CMD_RESUME) {
        if (w->case_id != CASE_RESUME && w->case_id != CASE_ENHANCED) {
            send_error(sock, MSG_ERR_PROTOCOL, EPROTO, "session resume is not supported by this server"); return -1;
        }
        if (req->length < 16) { send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "short resume payload"); return -1; }
        uint32_t topic_len = it305_get_u32(req->payload);
        uint32_t file_len = it305_get_u32(req->payload + 4);
        uint64_t offset = it305_get_u64(req->payload + 8);
        if ((uint64_t)16 + topic_len + file_len != req->length || topic_len == 0 || file_len == 0 ||
            topic_len > IT305_MAX_TOPIC || file_len > IT305_MAX_FILENAME) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "malformed resume payload"); return -1;
        }
        char topic[IT305_MAX_TOPIC + 1], filename[IT305_MAX_FILENAME + 1];
        if (copy_string_field(topic, sizeof(topic), req->payload + 16, topic_len) != 0 ||
            copy_string_field(filename, sizeof(filename), req->payload + 16 + topic_len, file_len) != 0) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "invalid resume path component"); return -1;
        }
        TopicRecord *t = registry_find(w->registry, topic);
        if (!t) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "topic not found"); return -1; }
        long idx = find_file_index(t, filename);
        if (idx < 0) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "file not found"); return -1; }
        struct stat st;
        if (stat(t->files[idx].absolute_path, &st) != 0 || st.st_size < 0 || offset > (uint64_t)st.st_size || offset > t->files[idx].file_size) {
            send_error(sock, MSG_ERR_BAD_OFFSET, EINVAL, "resume offset is outside file bounds"); return -1;
        }
        int one_file = (w->case_id == CASE_ENHANCED);
        int rc;
        log_event(0, "RESUME", sock, "topic=%s file=%s offset=%llu of %llu bytes", topic, filename,
                  (unsigned long long)offset, (unsigned long long)t->files[idx].file_size);
        if (offset == t->files[idx].file_size) {
            if (one_file) return it305_send_frame(sock, MSG_FILE_COMPLETE, 0, 0, NULL, 0);
            rc = send_topic(sock, t, (size_t)idx + 1, 0, 0, w->case_id, w->failure_probability, &w->seed, data_bytes);
        } else {
            rc = send_topic(sock, t, (size_t)idx, offset, one_file, w->case_id, w->failure_probability, &w->seed, data_bytes);
        }
        return rc == 0 ? 0 : -1;
    }
    if (req->type == MSG_CMD_MANIFEST) {
        if (w->case_id != CASE_ENHANCED) { send_error(sock, MSG_ERR_PROTOCOL, EPROTO, "manifest is only available in enhanced mode"); return -1; }
        char topic[IT305_MAX_TOPIC + 1];
        if (copy_string_field(topic, sizeof(topic), req->payload, req->length) != 0) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "invalid topic"); return -1;
        }
        TopicRecord *t = registry_find(w->registry, topic);
        if (!t) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "topic not found"); return -1; }
        return send_manifest(sock, t);
    }
    if (req->type == MSG_CMD_GET_FILE) {
        if (w->case_id != CASE_ENHANCED || req->length < 8) { send_error(sock, MSG_ERR_PROTOCOL, EPROTO, "GET_FILE not available"); return -1; }
        uint32_t topic_len = it305_get_u32(req->payload);
        uint32_t file_len = it305_get_u32(req->payload + 4);
        if ((uint64_t)8 + topic_len + file_len != req->length || !topic_len || !file_len ||
            topic_len > IT305_MAX_TOPIC || file_len > IT305_MAX_FILENAME) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "malformed GET_FILE payload"); return -1;
        }
        char topic[IT305_MAX_TOPIC + 1], filename[IT305_MAX_FILENAME + 1];
        if (copy_string_field(topic, sizeof(topic), req->payload + 8, topic_len) != 0 ||
            copy_string_field(filename, sizeof(filename), req->payload + 8 + topic_len, file_len) != 0) {
            send_error(sock, MSG_ERR_PROTOCOL, EINVAL, "invalid path component"); return -1;
        }
        TopicRecord *t = registry_find(w->registry, topic);
        if (!t) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "topic not found"); return -1; }
        long idx = find_file_index(t, filename);
        if (idx < 0) { send_error(sock, MSG_ERR_NOT_FOUND, ENOENT, "file not found"); return -1; }
        return send_topic(sock, t, (size_t)idx, 0, 1, w->case_id, w->failure_probability, &w->seed, data_bytes);
    }
    send_error(sock, MSG_ERR_PROTOCOL, EPROTO, "unknown message opcode");
    return -1;
}

static void *client_worker(void *arg) {
    WorkerArgs *w = (WorkerArgs *)arg;
    int sock = w->sock;
    int case_id = w->case_id;
    double p = w->failure_probability;
    TopicRegistry *registry = w->registry;
    unsigned int seed = w->seed;
    free(w);
    if (case_id == CASE_ENHANCED) (void)it305_set_socket_buffers(sock, 256 * 1024);
    uint64_t started = monotonic_ns();
    uint64_t bytes = 0;
    log_event(1, "CONNECT", sock, "case=%d", case_id);
    /* Active send window: first request received -> last reply finished. This is the "time required to send
       data to the requesting client" of the handout; it excludes idle time before the first request and while
       waiting for the client to close. */
    uint64_t first_req = 0, last_done = 0, first_req_wall = 0, last_done_wall = 0;
    for (;;) {
        It305Frame req;
        int rc = it305_recv_frame(sock, &req);
        if (rc <= 0) break;
        if (!first_req) { first_req = monotonic_ns(); first_req_wall = realtime_ns(); }
        log_event(1, "REQUEST", sock, "opcode=0x%02x payload=%u bytes", (unsigned)req.type, (unsigned)req.length);
        WorkerArgs local = { .sock = sock, .case_id = case_id, .failure_probability = p,
                             .registry = registry, .seed = seed };
        int hrc = handle_request(sock, &local, &req, &bytes);
        last_done = monotonic_ns(); last_done_wall = realtime_ns();
        seed = local.seed;
        uint8_t type = req.type;
        it305_frame_free(&req);
        /* Whole-topic replies (GET/RESUME in Part I, Case 1, Case 2) end with END_OF_TOPIC and the server then
           closes, so one stalled client cannot pin a worker (or the single-threaded server) forever. Only the
           enhanced per-file protocol keeps the connection open for the client's next request. */
        int persistent = (case_id == CASE_ENHANCED) &&
                         (type == MSG_CMD_MANIFEST || type == MSG_CMD_GET_FILE || type == MSG_CMD_RESUME);
        if (hrc != 0 || !persistent) break;
    }
    uint64_t ended = monotonic_ns();
    double elapsed = ended > started ? (double)(ended - started) / 1e9 : 0.0;
    log_event(1, "CLOSE", sock, "data_bytes=%llu connection_seconds=%.3f", (unsigned long long)bytes, elapsed);
    double send_sec = (first_req && last_done > first_req) ? (double)(last_done - first_req) / 1e9 : 0.0;
    fprintf(stdout, "SERVER_METRIC,case=%d,data_bytes=%llu,elapsed_sec=%.9f,send_sec=%.9f,start_unix_ns=%llu,end_unix_ns=%llu\n",
            case_id, (unsigned long long)bytes, elapsed, send_sec,
            (unsigned long long)first_req_wall, (unsigned long long)last_done_wall);
    fflush(stdout);
    close(sock);
    __atomic_sub_fetch(&g_active_workers, 1, __ATOMIC_SEQ_CST);
    return NULL;
}

static void reload_handler(int sig) { (void)sig; __atomic_store_n(&g_reload, 1, __ATOMIC_RELAXED); }

static void stop_handler(int sig) {
    (void)sig;
    __atomic_store_n(&g_stop, 1, __ATOMIC_RELAXED);
    if (g_listen_fd >= 0) close(g_listen_fd);
}

static int parse_port(const char *s, int *out) {
    char *end = NULL;
    long v = strtol(s, &end, 10);
    if (!s || !*s || !end || *end || v < 1 || v > 65535) return -1;
    *out = (int)v;
    return 0;
}

static int make_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;
    int yes = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons((uint16_t)port);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 || listen(fd, 128) != 0) {
        int saved = errno; close(fd); errno = saved; return -1;
    }
    return fd;
}

/* One independent PRNG seed per connection. The old scheme (time ^ pid ^ pthread_self() ^ fd) produced only a
   handful of distinct seeds because fd numbers are reused, which made fault patterns repeat. Here the wall clock
   in nanoseconds, the pid and a monotonically increasing connection counter are mixed with splitmix64. */
static unsigned int next_conn_seed(void) {
    static uint64_t counter = 0;
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t x = ((uint64_t)ts.tv_sec << 32) ^ (uint64_t)ts.tv_nsec ^ ((uint64_t)getpid() << 20) ^
                 (__atomic_add_fetch(&counter, 1, __ATOMIC_SEQ_CST) * 0x9E3779B97F4A7C15ull);
    x += 0x9E3779B97F4A7C15ull;
    x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ull;
    x = (x ^ (x >> 27)) * 0x94D049BB133111EBull;
    x ^= x >> 31;
    return (unsigned int)(x ^ (x >> 32));
}

int it305_server_main(int argc, char **argv, int case_id) {
    int port;
    const char *root;
    double fail_p = 0.0;
    int multi = 1;
    if (case_id == CASE_PART1) {
        if (argc != 3 && argc != 4) {
            fprintf(stderr, "Usage: %s <port> <topics_root_dir> [single|multi]\n", argv[0]); return 2;
        }
        if (argc == 4) {
            if (!strcmp(argv[3], "single")) multi = 0;
            else if (!strcmp(argv[3], "multi")) multi = 1;
            else { fprintf(stderr, "mode must be 'single' or 'multi'\n"); return 2; }
        }
        root = argv[2];
    } else {
        if (argc != 4 && !(case_id == CASE_ENHANCED && argc == 5)) {
            fprintf(stderr, "Usage: %s <port> <topics_root_dir> <failure_probability>%s\n", argv[0],
                    case_id == CASE_ENHANCED ? " [sendfile|buffered]" : ""); return 2;
        }
        if (argc == 5) {   /* optional, Enhanced only: choose the data path for the sendfile-vs-buffered comparison */
            if (!strcmp(argv[4], "buffered")) g_enhanced_sendfile = 0;
            else if (strcmp(argv[4], "sendfile")) { fprintf(stderr, "4th argument must be 'sendfile' or 'buffered'\n"); return 2; }
        }
        root = argv[2];
        char *end = NULL;
        fail_p = strtod(argv[3], &end);
        if (!argv[3][0] || !end || *end || fail_p < 0.0 || fail_p > 1.0) {
            fprintf(stderr, "failure_probability must be between 0.0 and 1.0\n"); return 2;
        }
    }
    if (parse_port(argv[1], &port) != 0) { fprintf(stderr, "port must be in range 1..65535\n"); return 2; }

    TopicRegistry registry;
    if (registry_load(&registry, root) != 0) {
        fprintf(stderr, "ERROR: failed to load topic registry\n"); return 1;
    }
    signal(SIGPIPE, SIG_IGN);
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = stop_handler;
    sigemptyset(&sa.sa_mask);
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    struct sigaction sh;                     /* SIGHUP: reload the topics directory (no SA_RESTART so accept() wakes up) */
    memset(&sh, 0, sizeof(sh));
    sh.sa_handler = reload_handler;
    sigemptyset(&sh.sa_mask);
    sigaction(SIGHUP, &sh, NULL);
    const char *log_path = getenv("IT305_LOG_FILE");
    if (log_path && *log_path) {
        g_log_file = fopen(log_path, "a");
        if (!g_log_file) fprintf(stderr, "WARNING: cannot open IT305_LOG_FILE '%s': %s\n", log_path, strerror(errno));
    }
    { const char *lv = getenv("IT305_LOG_LEVEL"); g_log_debug = lv && !strcasecmp(lv, "debug"); }

    g_listen_fd = make_listener(port);
    if (g_listen_fd < 0) { perror("listen socket"); free_registry(&registry); return 1; }
    fprintf(stdout, "[LISTENING] port=%d case=%d mode=%s failure_probability=%.6f%s\n",
            port, case_id, (case_id == CASE_PART1 && !multi) ? "single" : "multi", fail_p,
            case_id == CASE_ENHANCED ? (g_enhanced_sendfile ? " data_path=sendfile" : " data_path=buffered") : "");
    fflush(stdout);
    log_event(0, "SERVER_START", -1, "port=%d case=%d topics=%zu root=%s", port, case_id, registry.count, root);
    while (!__atomic_load_n(&g_stop, __ATOMIC_RELAXED)) {
        if (__atomic_load_n(&g_reload, __ATOMIC_RELAXED)) {
            __atomic_store_n(&g_reload, 0, __ATOMIC_RELAXED);
            if (registry_reload(&registry, root, __atomic_load_n(&g_active_workers, __ATOMIC_SEQ_CST)) == 0)
                log_event(0, "RELOAD", -1, "SIGHUP: topics=%zu (active connections keep the topics they started with)", registry.count);
            else
                log_event(0, "RELOAD_FAILED", -1, "SIGHUP: rescan of '%s' failed, still serving the previous topics", root);
        }
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        int client_fd = accept(g_listen_fd, (struct sockaddr *)&peer, &peer_len);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            if (__atomic_load_n(&g_stop, __ATOMIC_RELAXED) || errno == EBADF || errno == EINVAL) break;
            perror("accept"); continue;
        }
        (void)it305_set_nodelay(client_fd);
        if (case_id == CASE_ENHANCED) (void)it305_set_socket_buffers(client_fd, 256 * 1024);
        __atomic_add_fetch(&g_active_workers, 1, __ATOMIC_SEQ_CST);
        if (case_id == CASE_PART1 && !multi) {
            WorkerArgs *w = calloc(1, sizeof(*w));
            if (!w) { close(client_fd); __atomic_sub_fetch(&g_active_workers, 1, __ATOMIC_SEQ_CST); continue; }
            w->sock = client_fd; w->case_id = case_id; w->registry = &registry;
            w->failure_probability = 0.0;
            w->seed = next_conn_seed();
            client_worker(w);
        } else {
            WorkerArgs *w = calloc(1, sizeof(*w));
            if (!w) { close(client_fd); __atomic_sub_fetch(&g_active_workers, 1, __ATOMIC_SEQ_CST); continue; }
            w->sock = client_fd; w->case_id = case_id; w->registry = &registry;
            w->failure_probability = fail_p;
            w->seed = next_conn_seed();
            pthread_t tid;
            sigset_t blocked, previous;                  /* the new thread inherits this mask: workers never run the handlers */
            sigemptyset(&blocked);
            sigaddset(&blocked, SIGINT); sigaddset(&blocked, SIGTERM); sigaddset(&blocked, SIGHUP);
            pthread_sigmask(SIG_BLOCK, &blocked, &previous);
            int create_rc = pthread_create(&tid, NULL, client_worker, w);
            pthread_sigmask(SIG_SETMASK, &previous, NULL);
            if (create_rc != 0) {
                close(client_fd); free(w); __atomic_sub_fetch(&g_active_workers, 1, __ATOMIC_SEQ_CST); continue;
            }
            pthread_detach(tid);
        }
    }
    if (g_listen_fd >= 0) { close(g_listen_fd); g_listen_fd = -1; }
    /* Graceful shutdown: stop accepting (done above), then let in-flight transfers finish (max ~10 s). */
    fprintf(stdout, "[SHUTDOWN] draining %d active connection(s)\n", __atomic_load_n(&g_active_workers, __ATOMIC_SEQ_CST));
    fflush(stdout);
    for (int i = 0; i < 100 && __atomic_load_n(&g_active_workers, __ATOMIC_SEQ_CST) > 0; ++i) {
        struct timespec nap = {0, 100 * 1000 * 1000};
        nanosleep(&nap, NULL);
    }
    fprintf(stdout, "[SHUTDOWN] server stopped\n");
    fflush(stdout);
    log_event(0, "SERVER_STOP", -1, "active_connections_left=%d", __atomic_load_n(&g_active_workers, __ATOMIC_SEQ_CST));
    if (__atomic_load_n(&g_active_workers, __ATOMIC_SEQ_CST) == 0) free_retired();
    if (g_log_file) { fclose(g_log_file); g_log_file = NULL; }
    return 0;
}
