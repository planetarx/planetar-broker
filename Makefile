CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -Wpedantic -Wno-unused-parameter -std=c11 -D_GNU_SOURCE
LDFLAGS ?=

BIN      := planetar-broker
TOOLS    := shm-consumer

.PHONY: all clean run debug

all: $(BIN) $(TOOLS)

$(BIN): planetar-broker.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

shm-consumer: shm-consumer.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

debug: CFLAGS := -O0 -g3 -Wall -Wextra -Wpedantic -Wno-unused-parameter -std=c11 -D_GNU_SOURCE -fsanitize=address,undefined
debug: clean all

run: $(BIN)
	./$(BIN)

clean:
	rm -f $(BIN) $(TOOLS)
