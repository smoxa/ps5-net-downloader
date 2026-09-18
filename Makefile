PS5_PAYLOAD_SDK ?= $(HOME)/ps5-payload-sdk

TARGET = ps5-net-downloader.elf
CFILES = $(wildcard src/*.c)
OBJS   = $(CFILES:.c=.o)
CFLAGS += -Isrc -Wall -Wextra -O2

ifneq ($(wildcard $(PS5_PAYLOAD_SDK)/toolchain/prospero.mk),)
include $(PS5_PAYLOAD_SDK)/toolchain/prospero.mk
all: $(TARGET)
else
CC     ?= clang
CFLAGS += -target x86_64-unknown-freebsd12.0 -fPIC -pthread
LDFLAGS ?= -target x86_64-unknown-freebsd12.0 -fuse-ld=lld -pthread

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(OBJS) $(LDFLAGS) -o $@

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f $(OBJS) $(TARGET)

.PHONY: all clean
endif
