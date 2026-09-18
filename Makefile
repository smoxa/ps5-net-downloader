PS5_PAYLOAD_SDK ?= $(HOME)/ps5-payload-sdk

ifdef PS5_PAYLOAD_SDK
    include $(PS5_PAYLOAD_SDK)/toolchain/prospero.mk
else
    $(error PS5_PAYLOAD_SDK is undefined)
endif

ELF  := ps5-net-downloader.elf
SRCS := src/main.c src/server.c src/downloader.c

CFLAGS += -Wall -Wextra -O2 -Isrc -lpthread

all: $(ELF)

$(ELF): $(SRCS)
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f $(ELF)

.PHONY: all clean
