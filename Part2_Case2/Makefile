# Builds exactly two executables in this folder: `server` and `client`.
# Shared code is taken from ./common (self-contained submission layout) or, if that does not exist,
# from ../common (development layout).
CC ?= gcc
CFLAGS ?= -O2 -std=c11 -Wall -Wextra -Wpedantic -D_FILE_OFFSET_BITS=64
COMMON := $(firstword $(wildcard common ../common))
CPPFLAGS += -I$(COMMON) -D_XOPEN_SOURCE=700
LDLIBS += -pthread

SERVER_SRC = server.c $(COMMON)/server_core.c $(COMMON)/io_helpers.c
CLIENT_SRC = client.c $(COMMON)/client_core.c $(COMMON)/io_helpers.c
HEADERS    = $(wildcard $(COMMON)/*.h)

.PHONY: all clean
all: server client

server: $(SERVER_SRC) $(HEADERS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $(SERVER_SRC) $(LDLIBS)

client: $(CLIENT_SRC) $(HEADERS)
	$(CC) $(CPPFLAGS) $(CFLAGS) -o $@ $(CLIENT_SRC) $(LDLIBS)

clean:
	rm -f server client *.o
