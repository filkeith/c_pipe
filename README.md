# c_pipe

A thread-safe, high-performance pipeline library for C, inspired by Go channels and the `io.Reader` / `io.Writer` interfaces. Built for use in backup/restore tooling on top of [Aerospike](https://aerospike.com).

---

## Overview

`c_pipe` connects N **Readers** to M **Writers** through buffered, thread-safe channels. Each reader and writer runs on its own thread. The pipeline handles all synchronisation, cancellation, and graceful shutdown internally.

```
Reader 0 ──┐                     ┌── Writer 0
Reader 1 ──┤──► Channel(s) ──────┤── Writer 1
Reader N ──┘                     └── Writer M
```

Two topologies are supported:

| Condition | Topology | Channels |
|-----------|----------|----------|
| N == M | 1-to-1 paired | One dedicated channel per reader/writer pair |
| N != M | Fan-in / fan-out | Single shared channel for all readers and writers |

---

## Project Structure

```
c_pipe/
├── include/
│   └── c_pipe/
│       ├── chan.h          # Buffered channel public API
│       ├── pipe.h          # Pipeline public API (Reader, Writer interfaces)
│       └── as_reader.h     # Aerospike partition reader
├── src/
│   ├── chan.c              # Channel implementation (ring buffer + mutex + cond vars)
│   ├── pipe.c              # Pipeline implementation
│   ├── as_reader.c         # Aerospike scan reader implementation
│   └── main.c
├── tests/
│   ├── test_chan.c          # Channel unit tests (Unity)
│   └── test_pipe.c         # Pipeline unit tests (Unity)
├── vendor/
│   ├── aerospike-client-c/ # Aerospike C client (git submodule)
│   └── Unity/              # Unity test framework (git submodule)
├── CMakeLists.txt
└── Makefile
```

---

## Dependencies

| Dependency | Version | Notes |
|------------|---------|-------|
| CMake | ≥ 3.20 | Build system |
| C17 | — | `_Atomic`, VLAs, `//` comments |
| pthreads | — | Threading and synchronisation |
| [aerospike-client-c](https://github.com/aerospike/aerospike-client-c) | latest | Managed via git submodule |
| [Unity](https://github.com/ThrowTheSwitch/Unity) | latest | Test framework, managed via git submodule |

---

## Building

### 1. Clone with submodules

```bash
git clone --recurse-submodules https://github.com/filkeith/c_pipe
cd c_pipe
```

Or if already cloned:

```bash
git submodule update --init --recursive
```

### 2. Build Aerospike client

The Aerospike C client must be compiled before building the project:

```bash
cd vendor/aerospike-client-c
git submodule update --init --recursive
make
cd ../..
```

### 3. Build the project

```bash
make build
```

### 4. Run tests

```bash
make test
```

### 5. Clean

```bash
make clean        # removes build/
make clean-all    # also cleans vendor/aerospike-client-c
```

---

## Core Concepts

### Channel

A bounded, thread-safe FIFO queue backed by a ring buffer. Provides blocking send/receive semantics identical to Go channels.

```c
Channel *ch = channel_new(64);

// Producer thread
channel_send(ch, my_data);

// Consumer thread
void *data = NULL;
channel_receive(ch, &data);

channel_close(ch);
channel_destroy(ch);
```

### Reader / Writer interfaces

Implement these two structs to plug any data source or sink into the pipeline:

```c
typedef struct {
    int (*read)(void *ctx, void **data);  // PIPE_OK / PIPE_EOF / PIPE_ERR
    int (*close)(void *ctx);
    void *ctx;                            // your context, passed through opaquely
} Reader;

typedef struct {
    int (*write)(void *ctx, void **data); // PIPE_OK / PIPE_ERR
    int (*close)(void *ctx);
    void *ctx;
} Writer;
```

**Return codes:**

| Code | Value | Meaning |
|------|-------|---------|
| `PIPE_OK` | 0 | Success |
| `PIPE_EOF` | 1 | No more data, normal termination |
| `PIPE_ERR` | -1 | Unrecoverable error, pipeline will be cancelled |

### Pipeline

```c
Reader readers[2] = {
    { .read = my_read, .close = my_close, .ctx = &ctx_a },
    { .read = my_read, .close = my_close, .ctx = &ctx_b },
};
Writer writers[2] = {
    { .write = my_write, .close = my_close, .ctx = &ctx_c },
    { .write = my_write, .close = my_close, .ctx = &ctx_d },
};

Pipe *p = pipe_new(readers, 2, writers, 2);
pipe_run(p);    // blocks until all threads complete
pipe_destroy(p);
```

---

## Aerospike Reader

`AerospikeReader` wraps `aerospike_scan_partitions` (push model) into the pull-based `Reader` interface by running the scan on a background thread and bridging records through an internal channel.

```c
// Connect to Aerospike.
aerospike as;
as_config cfg;
as_config_init(&cfg);
as_config_add_host(&cfg, "127.0.0.1", 3000);
aerospike_init(&as, &cfg);

as_error err;
aerospike_connect(&as, &err);

// Create reader for all partitions.
as_partition_filter pf;
as_partition_filter_set_all(&pf);

AerospikeReader *ar = as_reader_new(&as, "my_namespace", "my_set", pf);

// Start the background scan thread.
as_reader_start(ar);

// Wire into pipeline.
Reader r = {
    .read  = as_reader_read,
    .close = as_reader_close,
    .ctx   = ar,
};

// Writer is responsible for calling as_record_destroy(*data) after processing.
Writer w = { .write = my_writer_write, .close = my_writer_close, .ctx = &my_ctx };

Pipe *p = pipe_new(&r, 1, &w, 1);
pipe_run(p);
pipe_destroy(p);

as_reader_destroy(ar);
aerospike_close(&as, &err);
aerospike_destroy(&as);
```

> **Ownership:** `as_record*` pointers received by the writer must be freed with `as_record_destroy()` after processing. The pipeline does not free them.

---

## Cancellation

Any reader or writer returning `PIPE_ERR` sets a shared `cancelled` flag that causes all other threads in the pipeline to stop at their next iteration. Channels are then closed to unblock any waiting threads.

---

## Thread Safety

| Component | Thread-safe |
|-----------|-------------|
| `Channel` send/receive/close | ✅ mutex + condition variables |
| `Pipe` cancelled flag | ✅ `_Atomic int` |
| `AerospikeReader` error/cancelled | ✅ `_Atomic int` |
| `Reader` / `Writer` ctx | ⚠️ caller's responsibility |

---

## License

MIT