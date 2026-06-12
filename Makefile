CC      ?= cc
CFLAGS  ?= -O2 -Wall -Wextra -Wpedantic -Wno-unused-parameter -std=c11 -D_GNU_SOURCE
LDFLAGS ?=

BIN      := planetar-broker
TOOLS    := shm-consumer

# Test binaries (see tests/ and the Testing section of README.md).
TEST_CLIENTS := tests/pub-tcp tests/pub-udp tests/pub-shm \
                tests/sub-tcp tests/sub-shm tests/ring-hop
TEST_UNIT    := tests/test-unit
TEST_HDRS    := tests/zmesg-test.h tests/latency-stats.h

.PHONY: all clean run debug test test-unit test-functional perf

all: $(BIN) $(TOOLS)

$(BIN): planetar-broker.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

shm-consumer: shm-consumer.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

debug: CFLAGS := -O0 -g3 -Wall -Wextra -Wpedantic -Wno-unused-parameter -std=c11 -D_GNU_SOURCE -fsanitize=address,undefined
debug: clean all

run: $(BIN)
	./$(BIN)

# ---------------- tests ----------------

# The unit test binary #includes planetar-broker.c (with main
# renamed), so it rebuilds whenever the broker source changes.
$(TEST_UNIT): tests/test-unit.c planetar-broker.c $(TEST_HDRS)
	$(CC) $(CFLAGS) -o $@ tests/test-unit.c $(LDFLAGS)

# Static pattern rule for the four protocol test clients.
$(TEST_CLIENTS): tests/%: tests/%.c $(TEST_HDRS)
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)

# Unit tests only (fast, no broker process needed).
test-unit: $(TEST_UNIT)
	./$(TEST_UNIT)

# Functional/integration suite (starts its own broker; aborts if a
# broker is already listening on 12001).
test-functional: all $(TEST_CLIENTS)
	./tests/test-functional.sh

# The full suite: unit, then functional.
test: all $(TEST_UNIT) $(TEST_CLIENTS)
	./$(TEST_UNIT)
	./tests/test-functional.sh

# End-to-end latency/throughput benchmark.
#   make perf            -> 100k msgs/path
#   make perf N=1000000  -> proposal-grade run
N ?= 100000
perf: all $(TEST_CLIENTS)
	./tests/test-perf.sh $(N)

clean:
	rm -f $(BIN) $(TOOLS) $(TEST_UNIT) $(TEST_CLIENTS)
