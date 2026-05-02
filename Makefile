BUILD_DIR        = build
BUILD_DIR_TSAN   = build-tsan
BUILD_DIR_ASAN   = build-asan

# On macOS Homebrew OpenSSL is not on the default linker search path.
ifeq ($(shell uname -s),Darwin)
    OPENSSL_PREFIX := $(shell brew --prefix openssl@3 2>/dev/null)
    ifneq ($(OPENSSL_PREFIX),)
        CMAKE_FLAGS += -DOPENSSL_ROOT_DIR=$(OPENSSL_PREFIX)
    endif
endif

build:
	cmake -S . -B $(BUILD_DIR) $(CMAKE_FLAGS)
	cmake --build $(BUILD_DIR)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

test-tsan:
	cmake -S . -B $(BUILD_DIR_TSAN) $(CMAKE_FLAGS) -DC_PIPE_TSAN=ON -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR_TSAN)
	cd $(BUILD_DIR_TSAN) && ctest --output-on-failure

test-asan:
	cmake -S . -B $(BUILD_DIR_ASAN) $(CMAKE_FLAGS) -DC_PIPE_ASAN=ON -DCMAKE_BUILD_TYPE=Debug
	cmake --build $(BUILD_DIR_ASAN)
	cd $(BUILD_DIR_ASAN) && ctest --output-on-failure

test-all: test test-tsan test-asan

clean:
	rm -rf $(BUILD_DIR) $(BUILD_DIR_TSAN) $(BUILD_DIR_ASAN)

.PHONY: build test test-tsan test-asan test-all clean