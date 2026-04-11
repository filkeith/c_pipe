BUILD_DIR = build

# Build everything.
build:
	cmake -S . -B $(BUILD_DIR)
	cmake --build $(BUILD_DIR)

# Build test/
test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

# Clean
clean:
	rm -rf $(BUILD_DIR)

.PHONY: build test clean