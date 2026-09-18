PS5_PAYLOAD_SDK ?= $(HOME)/ps5-payload-sdk

TARGET = ps5-net-downloader.elf
SRCS   = src/main.c src/server.c src/downloader.c
OBJS   = $(SRCS:.c=.o)

CC      ?= clang
CFLAGS  ?= -target x86_64-unknown-freebsd12.0 -Wall -Wextra -O2 -Isrc -fPIC -pthread
LDFLAGS ?= -target x86_64-unknown-freebsd12.0 -fuse-ld=lld -pthread

ifneq ($(wildcard $(PS5_PAYLOAD_SDK)/Makefile.rules),)
include $(PS5_PAYLOAD_SDK)/Makefile.rules
else
all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) $(LDFLAGS) -o $@

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
endif
