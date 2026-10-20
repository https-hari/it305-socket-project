#ifndef IT305_IO_HELPERS_H
#define IT305_IO_HELPERS_H

#include "protocol.h"
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

/* send_all/recv_all return 0 on success, -1 on error; recv_all returns 1 on EOF. */
int it305_send_all(int fd, const void *buf, size_t len);
int it305_recv_all(int fd, void *buf, size_t len);
/* like send_all but tells the kernel more data follows (MSG_MORE): used for a frame header that is
   immediately followed by sendfile() so header and payload leave in the same segments. */
int it305_send_all_more(int fd, const void *buf, size_t len);
int it305_set_nodelay(int fd);
int it305_send_frame(int fd, uint8_t type, uint8_t flags, uint16_t status,
                     const void *payload, uint32_t length);
/* recv_frame: 1 frame, 0 clean EOF before a header, -1 error/truncated frame. */
int it305_recv_frame(int fd, It305Frame *out);
void it305_frame_free(It305Frame *frame);
void it305_put_u32(unsigned char *p, uint32_t v);
void it305_put_u64(unsigned char *p, uint64_t v);
uint32_t it305_get_u32(const unsigned char *p);
uint64_t it305_get_u64(const unsigned char *p);
int it305_set_socket_buffers(int fd, int bytes);
int it305_mkdir_p(const char *path, mode_t mode);
int it305_path_join(char *out, size_t out_size, const char *a, const char *b);
int it305_valid_component(const char *s);

#endif
