#define _POSIX_C_SOURCE 200809L
#include "io_helpers.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

#ifndef MSG_MORE
#define MSG_MORE 0x8000
#endif

int it305_send_all(int fd, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

int it305_send_all_more(int fd, const void *buf, size_t len) {
    const unsigned char *p = (const unsigned char *)buf;
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, p + sent, len - sent, MSG_NOSIGNAL | MSG_MORE);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

int it305_set_nodelay(int fd) {
    int one = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

int it305_recv_all(int fd, void *buf, size_t len) {
    unsigned char *p = (unsigned char *)buf;
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, p + got, len - got, 0);
        if (n < 0 && errno == EINTR) continue;
        if (n == 0) return 1;
        if (n < 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

void it305_put_u32(unsigned char *p, uint32_t v) {
    uint32_t x = htonl(v);
    memcpy(p, &x, sizeof(x));
}

void it305_put_u64(unsigned char *p, uint64_t v) {
    it305_put_u32(p, (uint32_t)(v >> 32));
    it305_put_u32(p + 4, (uint32_t)(v & 0xffffffffu));
}

uint32_t it305_get_u32(const unsigned char *p) {
    uint32_t x;
    memcpy(&x, p, sizeof(x));
    return ntohl(x);
}

uint64_t it305_get_u64(const unsigned char *p) {
    return ((uint64_t)it305_get_u32(p) << 32) | (uint64_t)it305_get_u32(p + 4);
}

/* Header and payload go out in ONE sendmsg() call. Two separate send() calls (the old code) trigger the classic
   write-write-read stall: Nagle holds the small second segment until the peer's delayed ACK for the first. */
int it305_send_frame(int fd, uint8_t type, uint8_t flags, uint16_t status,
                     const void *payload, uint32_t length) {
    unsigned char h[IT305_HEADER_SIZE];
    h[0] = type;
    h[1] = flags;
    h[2] = (unsigned char)((status >> 8) & 0xffu);
    h[3] = (unsigned char)(status & 0xffu);
    it305_put_u32(h + 4, length);
    size_t total = (size_t)IT305_HEADER_SIZE + length, off = 0;
    while (off < total) {
        struct iovec iov[2];
        int cnt = 0;
        if (off < IT305_HEADER_SIZE) {
            iov[cnt].iov_base = h + off; iov[cnt].iov_len = IT305_HEADER_SIZE - off; cnt++;
            if (length) { iov[cnt].iov_base = (void *)payload; iov[cnt].iov_len = length; cnt++; }
        } else {
            iov[cnt].iov_base = (unsigned char *)payload + (off - IT305_HEADER_SIZE); iov[cnt].iov_len = total - off; cnt++;
        }
        struct msghdr m;
        memset(&m, 0, sizeof(m));
        m.msg_iov = iov; m.msg_iovlen = (size_t)cnt;
        ssize_t n = sendmsg(fd, &m, MSG_NOSIGNAL);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        off += (size_t)n;
    }
    return 0;
}

int it305_recv_frame(int fd, It305Frame *out) {
    unsigned char h[IT305_HEADER_SIZE];
    int rc = it305_recv_all(fd, h, sizeof(h));
    if (rc == 1) return 0;
    if (rc != 0) return -1;
    memset(out, 0, sizeof(*out));
    out->type = h[0];
    out->flags = h[1];
    out->status = (uint16_t)(((uint16_t)h[2] << 8) | h[3]);
    out->length = it305_get_u32(h + 4);
    if (out->length > IT305_MAX_PAYLOAD) { errno = EMSGSIZE; return -1; }
    if (out->length) {
        out->payload = (unsigned char *)malloc(out->length);
        if (!out->payload) return -1;
        rc = it305_recv_all(fd, out->payload, out->length);
        if (rc != 0) {
            free(out->payload);
            out->payload = NULL;
            return -1;
        }
    }
    return 1;
}

void it305_frame_free(It305Frame *frame) {
    if (!frame) return;
    free(frame->payload);
    memset(frame, 0, sizeof(*frame));
}

int it305_set_socket_buffers(int fd, int bytes) {
    if (setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bytes, sizeof(bytes)) != 0) return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bytes, sizeof(bytes)) != 0) return -1;
    return 0;
}

int it305_mkdir_p(const char *path, mode_t mode) {
    char tmp[PATH_MAX];
    size_t n;
    if (!path || !*path) { errno = EINVAL; return -1; }
    n = strlen(path);
    if (n >= sizeof(tmp)) { errno = ENAMETOOLONG; return -1; }
    memcpy(tmp, path, n + 1);
    if (n > 1 && tmp[n - 1] == '/') tmp[n - 1] = '\0';
    for (char *p = tmp + (tmp[0] == '/'); *p; ++p) {
        if (*p == '/') {
            *p = '\0';
            if (mkdir(tmp, mode) != 0 && errno != EEXIST) return -1;
            *p = '/';
        }
    }
    if (mkdir(tmp, mode) != 0 && errno != EEXIST) return -1;
    struct stat st;
    if (stat(tmp, &st) != 0 || !S_ISDIR(st.st_mode)) { errno = ENOTDIR; return -1; }
    return 0;
}

int it305_path_join(char *out, size_t out_size, const char *a, const char *b) {
    int n;
    if (!out || !a || !b) { errno = EINVAL; return -1; }
    n = snprintf(out, out_size, "%s/%s", a, b);
    if (n < 0 || (size_t)n >= out_size) { errno = ENAMETOOLONG; return -1; }
    return 0;
}

int it305_valid_component(const char *s) {
    if (!s || !*s || strcmp(s, ".") == 0 || strcmp(s, "..") == 0) return 0;
    if (strchr(s, '/') || strchr(s, '\\') || strchr(s, '\t') || strchr(s, '\n') || strchr(s, '\r')) return 0;
    return 1;
}
