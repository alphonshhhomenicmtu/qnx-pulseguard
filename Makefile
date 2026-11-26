# QNX-PulseGuard Dual-Target Makefile (Host POSIX/macOS/Linux + QNX Neutrino 8.0)

CXX ?= clang++
QNX_TARGET ?= x86_64
CXXFLAGS_COMMON = -std=c++20 -O2 -Wall -Wextra -Werror -Iinclude -Isrc
LDFLAGS_COMMON = -lpthread

ifeq ($(QNX),1)
  ifeq ($(QNX_TARGET),aarch64)
    CXX = q++ -Vgcc_ntoaarch64le
  else
    CXX = q++ -Vgcc_ntox86_64
  endif
  CXXFLAGS_COMMON = -std=c++20 -O2 -Wall -Wextra -Werror -Iinclude -Isrc
  LDFLAGS_COMMON =
endif

BUILD_DIR = build
RESMGR_OBJ = $(BUILD_DIR)/resmgr_pulseguard.o
DAEMON_BIN = $(BUILD_DIR)/pulseguard_daemon
INJECTOR_BIN = $(BUILD_DIR)/fault_injector
TEST_BIN = $(BUILD_DIR)/test_pulseguard

HEADERS = \
	include/pulseguard/clock.hpp \
	include/pulseguard/ring_buffer.hpp \
	include/pulseguard/histogram.hpp \
	include/pulseguard/watchdog.hpp \
	include/pulseguard/telemetry_json.hpp \
	include/pulseguard/resmgr.hpp \
	include/pulseguard/engine.hpp \
	src/qnx_ipc_bridge.hpp

.PHONY: all test run clean qnx-x86_64 qnx-aarch64

all: $(DAEMON_BIN) $(INJECTOR_BIN) $(TEST_BIN)

$(BUILD_DIR):
	@mkdir -p $(BUILD_DIR)

$(RESMGR_OBJ): src/resmgr_pulseguard.cpp $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS_COMMON) -c $< -o $@

$(DAEMON_BIN): src/pulseguard_daemon.cpp $(RESMGR_OBJ) $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS_COMMON) $< $(RESMGR_OBJ) -o $@ $(LDFLAGS_COMMON)

$(INJECTOR_BIN): src/fault_injector.cpp $(RESMGR_OBJ) $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS_COMMON) $< $(RESMGR_OBJ) -o $@ $(LDFLAGS_COMMON)

$(TEST_BIN): tests/test_pulseguard.cpp $(RESMGR_OBJ) $(HEADERS) | $(BUILD_DIR)
	$(CXX) $(CXXFLAGS_COMMON) $< $(RESMGR_OBJ) -o $@ $(LDFLAGS_COMMON)

test: $(TEST_BIN)
	./$(TEST_BIN)

run: $(DAEMON_BIN)
	./$(DAEMON_BIN) --duration-ms 1200 --inject-at-ms 500

qnx-x86_64:
	$(MAKE) QNX=1 QNX_TARGET=x86_64 all

qnx-aarch64:
	$(MAKE) QNX=1 QNX_TARGET=aarch64 all

clean:
	rm -rf $(BUILD_DIR) pulseguard_dev.json pulseguard_dev.bin pulseguard_telemetry.jsonl
