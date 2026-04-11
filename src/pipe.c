#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "../include/c_pipe/chan.h"

#define CHANNEL_SIZE 64

/**
 * @brief Abstract reader interface.
 *
 * Implementations provide a @c read function that produces items one at a time
 * and a @c close function to release underlying resources.
 */
// Interface for reader.
typedef struct {
    int (*read)(void **data);  /**< @brief Read one item into @p *data. Returns 0 on success, non-zero on EOF/error. */
    int (*close)();            /**< @brief Release resources held by the reader. */
} Reader;

/**
 * @brief Abstract writer interface.
 *
 * Implementations provide a @c write function that consumes items one at a time
 * and a @c close function to flush and release underlying resources.
 */
// Interface for writer.
typedef struct {
    int (*write)(void **data); /**< @brief Write one item from @p *data. Returns 0 on success, non-zero on error. */
    int (*close)();            /**< @brief Flush and release resources held by the writer. */
} Writer;

/**
 * @brief Binds a @ref Reader to an output @ref Channel and runs it on a thread.
 *
 * Reads items from @c reader and pushes them into @c output until
 * either the reader signals EOF/error or @c cancelled is set to non-zero.
 */
// Wrapper for reader.
typedef struct {
    Reader *reader;          /**< @brief Source of data items. */
    Channel *output;         /**< @brief Channel to push items into. */
    atomic_int *cancelled;   /**< @brief Shared cancellation flag; set to 1 to stop the pipeline. */
} ReadChain;

/**
 * @brief Allocates and initialises a new @ref ReadChain.
 *
 * @param[in] reader     Reader to pull data from. Must not be @c NULL.
 * @param[in] output     Channel to push items into. Must not be @c NULL.
 * @param[in] cancelled  Shared cancellation flag. Must not be @c NULL.
 * @return               Pointer to the new @ref ReadChain, or @c NULL on allocation failure.
 */
// Create new chain for reader.
ReadChain *read_chain_new(Reader *reader, Channel *output, atomic_int *cancelled) {
    ReadChain *rc = malloc(sizeof(ReadChain));
    if (rc == NULL) {
        return NULL;
    }

    rc->reader = reader;
    rc->output = output;
    rc->cancelled = cancelled;

    return rc;
}

/**
 * @brief Frees a @ref ReadChain.
 *
 * Does not destroy the @ref Reader or @ref Channel — those are owned externally.
 *
 * @param[in] rc  Chain to free. No-op if @c NULL.
 */
void read_chain_destroy(ReadChain *rc) {
    if (rc == NULL) return;

    free(rc);
}

/**
 * @brief Thread entry point for a @ref ReadChain.
 *
 * Reads from @c rc->reader and sends each item to @c rc->output.
 * Stops when the reader returns a non-zero code, the channel is closed,
 * or @c cancelled is set. Sets @c cancelled on any error so the rest
 * of the pipeline tears down gracefully.
 *
 * @param[in] arg  Pointer to a @ref ReadChain. Must not be @c NULL.
 * @return         Always @c NULL.
 */
static void *read_chain_run(void *arg) {
    ReadChain *rc = arg;

    void *data = NULL;

    int code = 0;
    while (atomic_load(rc->cancelled) == 0 && (code = rc->reader->read(&data)) == 0) {
        if (channel_send(rc->output, data) != 0) {
            free(data);
            atomic_store(rc->cancelled, 1);
            break;
        }
    }

    if (code != 0) {
        atomic_store(rc->cancelled, 1);
    }

    rc->reader->close();

    return NULL;
}

/**
 * @brief Binds a @ref Writer to an input @ref Channel and runs it on a thread.
 *
 * Receives items from @c input and passes them to @c writer until
 * the channel is closed and drained, the writer signals an error,
 * or @c cancelled is set to non-zero.
 */
typedef struct {
    Writer *writer;          /**< @brief Sink for data items. */
    Channel *input;          /**< @brief Channel to receive items from. */
    atomic_int *cancelled;   /**< @brief Shared cancellation flag; set to 1 to stop the pipeline. */
} WriteChain;

/**
 * @brief Allocates and initialises a new @ref WriteChain.
 *
 * @param[in] writer     Writer to push data into. Must not be @c NULL.
 * @param[in] input      Channel to receive items from. Must not be @c NULL.
 * @param[in] cancelled  Shared cancellation flag. Must not be @c NULL.
 * @return               Pointer to the new @ref WriteChain, or @c NULL on allocation failure.
 */
// Create new chain for writer.
WriteChain *write_chain_new(Writer *writer, Channel *input, atomic_int *cancelled) {
    WriteChain *wc = malloc(sizeof(WriteChain));
    if (wc == NULL) {
        return NULL;
    }

    wc->writer = writer;
    wc->input = input;
    wc->cancelled = cancelled;

    return wc;
}

/**
 * @brief Frees a @ref WriteChain.
 *
 * Does not destroy the @ref Writer or @ref Channel — those are owned externally.
 *
 * @param[in] wc  Chain to free. No-op if @c NULL.
 */
void write_chain_destroy(WriteChain *wc) {
    if (wc == NULL) return;

    free(wc);
}

/**
 * @brief Thread entry point for a @ref WriteChain.
 *
 * Receives items from @c wc->input and writes each one via @c wc->writer.
 * Stops when the channel is closed and drained, the writer returns a non-zero
 * code, or @c cancelled is set. Sets @c cancelled on any error.
 *
 * @param[in] arg  Pointer to a @ref WriteChain. Must not be @c NULL.
 * @return         Always @c NULL.
 */
static void *write_chain_run(void *arg) {
    WriteChain *wc = arg;

    void *data = NULL;

    int code = 0;
    while (atomic_load(wc->cancelled) == 0 && (code = channel_receive(wc->input, &data)) == 0) {
        if (wc->writer->write(&data) != 0) {
            free(data);
            atomic_store(wc->cancelled, 1);
            break;
        }
    }

    if (code != 0) {
        atomic_store(wc->cancelled, 1);
    }

    //TODO: process close error ?
    wc->writer->close();

    return NULL;
}

/**
 * @brief Top-level pipeline that fans data from N readers to M writers.
 *
 * Topology is determined at construction time by @ref pipe_new:
 * - **N == M**: each reader is paired with its own writer through a dedicated channel.
 * - **N != M**: all readers and all writers share a single channel (fan-in / fan-out).
 *
 * All threads share a single @c cancelled flag so any failure tears down
 * the whole pipeline.
 */
// Main pipeline definition.
typedef struct {
    ReadChain **readers_chain;            /**< @brief Array of reader chain wrappers. */
    size_t readers_count;                 /**< @brief Total number of readers requested. */
    size_t readers_created;              /**< @brief Number of reader chains successfully initialised (used for partial cleanup). */

    WriteChain **writers_chain;           /**< @brief Array of writer chain wrappers. */
    size_t writers_count;                 /**< @brief Total number of writers requested. */
    size_t writers_created;              /**< @brief Number of writer chains successfully initialised (used for partial cleanup). */

    Channel **chans;                      /**< @brief Array of channels connecting readers to writers. */
    size_t chans_count;                   /**< @brief Total number of channels to allocate. */
    size_t chans_created;                /**< @brief Number of channels successfully initialised (used for partial cleanup). */

    // If at least one chain fails, we exit others.
    atomic_int cancelled;                 /**< @brief Shared cancellation flag; set to 1 by any failing chain. */
} Pipe;

/**
 * @brief Destroys a @ref Pipe and releases all associated resources.
 *
 * Frees all reader chains, writer chains, channels, and the pipe struct itself.
 * Safe to call with a partially initialised pipe (uses @c *_created counters).
 *
 * @param[in] pipe  Pipe to destroy. No-op if @c NULL.
 *
 * @warning Must only be called after all threads have exited (i.e. after
 *          @ref pipe_run returns or after manual @c pthread_join on all threads).
 */
void pipe_destroy(Pipe *pipe) {
    if (pipe == NULL) return;

    if (pipe->readers_chain != NULL) {
        for (size_t i = 0; i < pipe->readers_created; i++) {
            read_chain_destroy(pipe->readers_chain[i]);
        }
    }

    free(pipe->readers_chain);

    if (pipe->writers_chain != NULL) {
        for (size_t i = 0; i < pipe->writers_created; i++) {
            write_chain_destroy(pipe->writers_chain[i]);
        }
    }

    free(pipe->writers_chain);

    for (size_t i = 0; i < pipe->chans_created; i++) {
        channel_destroy(pipe->chans[i]);
    }

    free(pipe->chans);

    free(pipe);
}

/**
 * @brief Allocates and wires up a new @ref Pipe.
 *
 * Determines the channel topology, allocates all @ref ReadChain and
 * @ref WriteChain instances, and links them to the appropriate channels.
 * On any failure the partially constructed pipe is destroyed and @c NULL is returned.
 *
 * @param[in] readers        Array of @ref Reader instances. Must not be @c NULL.
 * @param[in] readers_count  Number of readers. Must be > 0.
 * @param[in] writers        Array of @ref Writer instances. Must not be @c NULL.
 * @param[in] writers_count  Number of writers. Must be > 0.
 * @return                   Pointer to the new @ref Pipe, or @c NULL on failure.
 *
 * @note The caller must eventually call @ref pipe_destroy to free all resources.
 */
Pipe *pipe_new(Reader *readers, size_t readers_count, Writer *writers, size_t writers_count) {
    if (readers_count == 0 || writers_count == 0) {
        return NULL;
    }

    // Allocate pipe with 0 values.
    Pipe *pipe = calloc(1, sizeof(Pipe));
    if (pipe == NULL) {
        return NULL;
    }

    pipe->readers_count = readers_count;
    pipe->writers_count = writers_count;
    atomic_init(&pipe->cancelled, 0);

    pipe->readers_chain = malloc(sizeof(ReadChain *) * readers_count);
    if (pipe->readers_chain == NULL) {
        goto cleanup;
    }

    pipe->writers_chain = malloc(sizeof(WriteChain *) * writers_count);
    if (pipe->writers_chain == NULL) {
        goto cleanup;
    }

    // Calculate the number of channels.
    if (readers_count == writers_count) {
        pipe->chans_count = readers_count;
    } else {
        pipe->chans_count = 1;
    }

    pipe->chans = malloc(sizeof(Channel *) * pipe->chans_count);
    if (pipe->chans == NULL) {
        goto cleanup;
    }

    // Init channels.
    if (readers_count == writers_count) {
        // Set everything in a loop.
        for (size_t i = 0; i < readers_count; i++) {
            // Channel.
            Channel *ch = channel_new(CHANNEL_SIZE);
            if (ch == NULL) {
                goto cleanup;
            }
            pipe->chans[i] = ch;
            pipe->chans_created++;

            // Reader.
            ReadChain *rc = read_chain_new(&readers[i], pipe->chans[i], &pipe->cancelled);
            if (rc == NULL) {
                goto cleanup;
            }
            pipe->readers_chain[i] = rc;
            pipe->readers_created++;

            // Writer.
            WriteChain *wc = write_chain_new(&writers[i], pipe->chans[i], &pipe->cancelled);
            if (wc == NULL) {
                goto cleanup;
            }
            pipe->writers_chain[i] = wc;
            pipe->writers_created++;
        }
    } else {
        pipe->chans[0] = channel_new(CHANNEL_SIZE);
        if (pipe->chans[0] == NULL) {
            goto cleanup;
        }
        pipe->chans_created++;

        // Set readers in a loop.
        for (size_t i = 0; i < readers_count; i++) {
            ReadChain *rc = read_chain_new(&readers[i], pipe->chans[0], &pipe->cancelled);
            if (pipe->readers_chain[i] == NULL) {
                goto cleanup;
            }
            pipe->readers_chain[i] = rc;
            pipe->readers_created++;
        }
        // Set writers in a loop.
        for (size_t i = 0; i < writers_count; i++) {
            WriteChain *wc = write_chain_new(&writers[i], pipe->chans[0], &pipe->cancelled);
            if (pipe->writers_chain[i] == NULL) {
                goto cleanup;
            }
            pipe->writers_chain[i] = wc;
            pipe->writers_created++;
        }
    }

    return pipe;

cleanup:
    pipe_destroy(pipe);
    return NULL;
}

/**
 * @brief Starts all pipeline threads and waits for them to complete.
 *
 * Launch order: writers first, then readers. This ensures writers are ready
 * to consume before readers start producing, avoiding a full-buffer stall
 * on startup.
 *
 * Shutdown sequence on success:
 * 1. Join all reader threads (they exit when the reader signals EOF).
 * 2. Close all channels (unblocks writers waiting on an empty channel).
 * 3. Join all writer threads.
 *
 * On any @c pthread_create or @c pthread_join failure, jumps to @c cleanup
 * which closes all channels and joins all already-started threads before
 * returning @c -1.
 *
 * @param[in] pipe  Pipe to run. Must not be @c NULL and must be fully initialised.
 * @return  @c 0  if all threads completed without error.
 * @return  @c -1 on thread creation or join failure.
 */
int pipe_run(Pipe *pipe) {
    pthread_t read_threads[pipe->readers_count];
    pthread_t write_threads[pipe->writers_count];

    size_t readers_started = 0;
    size_t writers_started = 0;

    // Start workers, writers then readers.
    for (size_t i = 0; i < pipe->writers_count; i++) {
        if (pthread_create(&write_threads[i], NULL, write_chain_run, pipe->writers_chain[i]) != 0) {
            goto cleanup;
        }
        writers_started++;
    }

    for (size_t i = 0; i < pipe->readers_count; i++) {
        if (pthread_create(&read_threads[i], NULL, read_chain_run, pipe->readers_chain[i]) != 0) {
            goto cleanup;
        }
        readers_started++;
    }

    // Wait for workers.
    for (size_t i = 0; i < pipe->readers_count; i++) {
        if (pthread_join(read_threads[i], NULL) != 0) {
            goto cleanup;
        }
    }

    // Close channels.
    for (size_t i = 0; i < pipe->chans_count; i++) {
        channel_close(pipe->chans[i]);
    }

    for (size_t i = 0; i < pipe->writers_count; i++) {
        if (pthread_join(write_threads[i], NULL) != 0) {
            goto cleanup;
        }
    }

    return 0;

cleanup:
    for (size_t i = 0; i < pipe->chans_count; i++) {
        channel_close(pipe->chans[i]);
    }

    for (size_t i = 0; i < readers_started; i++) {
        pthread_join(read_threads[i], NULL);
    }
    for (size_t i = 0; i < writers_started; i++) {
        pthread_join(write_threads[i], NULL);
    }

    return -1;
}