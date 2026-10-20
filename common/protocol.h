#ifndef IT305_PROTOCOL_H
#define IT305_PROTOCOL_H

#include <stdint.h>

/* The wire header is exactly 8 bytes; never send this C struct directly. */
#define IT305_HEADER_SIZE 8u
#define IT305_CHUNK_SIZE (64u * 1024u)
#define IT305_MAX_PAYLOAD (64u * 1024u * 1024u)
#define IT305_MAX_TOPIC 127u
#define IT305_MAX_FILENAME 255u

/* Message opcodes from the submitted Group 095/120 design document. */
enum it305_msg_type {
    MSG_CMD_GET        = 0x01,
    MSG_CMD_RESUME     = 0x02,
    MSG_FILE_HEADER    = 0x03,
    MSG_FILE_DATA      = 0x04,
    MSG_END_OF_TOPIC   = 0x05,
    MSG_ACK            = 0x06,
    MSG_CMD_MANIFEST   = 0x07,
    MSG_MANIFEST_DATA  = 0x08,
    MSG_CMD_GET_FILE   = 0x09,
    MSG_FILE_COMPLETE  = 0x0A,
    MSG_ERR_NOT_FOUND  = 0x10,
    MSG_ERR_BAD_OFFSET = 0x11,
    MSG_ERR_PROTOCOL   = 0x12
};

#define MSG_FLAG_FINAL_CHUNK 0x01u

typedef struct {
    uint8_t type;
    uint8_t flags;
    uint16_t status;
    uint32_t length;
    unsigned char *payload;
} It305Frame;

#endif
