CC = gcc
CFLAGS = -std=c99 -O3 -Wall -Wextra -Iinclude
LDFLAGS = -lpthread

SRC_DIR = src
OBJ_DIR = obj
BIN_DIR = bin
TEST_DIR = tests
BENCH_DIR = benchmarks

TARGET = $(BIN_DIR)/lsm_engine
TEST_TARGET = $(BIN_DIR)/run_tests
BENCH_TARGET = $(BIN_DIR)/run_bench

SRCS = $(wildcard $(SRC_DIR)/*.c)
OBJS = $(patsubst $(SRC_DIR)/%.c, $(OBJ_DIR)/%.o, $(SRCS))

TEST_SRCS = $(wildcard $(TEST_DIR)/*.c)
TEST_OBJS = $(patsubst $(TEST_DIR)/%.c, $(OBJ_DIR)/test_%.o, $(TEST_SRCS))

BENCH_SRCS = $(wildcard $(BENCH_DIR)/*.c)
BENCH_OBJS = $(patsubst $(BENCH_DIR)/%.c, $(OBJ_DIR)/bench_%.o, $(BENCH_SRCS))

CORE_OBJS = $(filter-out $(OBJ_DIR)/main.o, $(OBJS))

.PHONY: all build test benchmark clean directories

all: directories build test benchmark

directories:
	@mkdir -p $(BIN_DIR) $(OBJ_DIR)

build: directories $(TARGET)

$(TARGET): $(OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(OBJ_DIR)/%.o: $(SRC_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

test: directories $(TEST_TARGET)
	@if [ -x $(TEST_TARGET) ]; then \
		$(TEST_TARGET); \
	else \
		echo "No test executables found or built."; \
	fi

$(TEST_TARGET): $(CORE_OBJS) $(TEST_OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(OBJ_DIR)/test_%.o: $(TEST_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

benchmark: directories $(BENCH_TARGET)
	@if [ -x $(BENCH_TARGET) ]; then \
		$(BENCH_TARGET); \
	else \
		echo "No benchmark executables found or built."; \
	fi

$(BENCH_TARGET): $(CORE_OBJS) $(BENCH_OBJS)
	$(CC) $(CFLAGS) $^ -o $@ $(LDFLAGS)

$(OBJ_DIR)/bench_%.o: $(BENCH_DIR)/%.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	@rm -rf $(OBJ_DIR) $(BIN_DIR)