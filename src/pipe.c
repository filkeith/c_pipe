#include "c_pipe/pipe.h"

#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "c_pipe/chan.h"

#define CHANNEL_SIZE 64

/**
 * @brief Binds a @ref Reader to an output @ref Channel and runs it on a thread.
 *
 * Reads items from @c reader and pushes them into @c output until either the
 * reader signals EOF/error or @c cancelled is set.
 */
typedef struct {
    Reader *reader;          /**< @brief Source of data items. */
    void *ctx;
    Channel *output;         /**< @brief Channel to push items into. */
    atomic_int *cancelled;   /**< @brief Shared cancellation flag; set to 1 to stop the pipeline. */
} ReadChain;

/** @brief Allocates and initialises a new @ref ReadChain. */
static ReadChain *read_chain_new(Reader *reader, void *ctx, Channel *output, atomic_int *cancelled) {
    ReadChain *rc = malloc(sizeof(ReadChain));
    if (rc == NULL) return NULL;

    rc->reader    = reader;
    rc->ctx       = ctx;
    rc->output    = output;
    rc->cancelled = cancelled;

    return rc;
}

/** @brief Frees a @ref ReadChain. Does not touch the wrapped reader/channel. */
static void read_chain_destroy(ReadChain *rc) {
    if (rc == NULL) return;
    free(rc);
}

/**
 * @brief Thread entry point for a @ref ReadChain.
 *
 * Reads from @c rc->reader and sends each item to @c rc->output. Stops when
 * @c read returns a non-OK code, @c channel_send fails (channel closed), or
 * @c cancelled is raised. On exit due to cancellation closes its output
 * channel so any peer blocked on receive wakes up.
 *
 * @param[in] arg  Pointer to a @ref ReadChain. Must not be @c NULL.
 * @return         Always @c NULL.
 */
static void *read_chain_run(void *arg) {
    ReadChain *rc = arg;

    void *data = NULL;
    int code = PIPE_OK;

    while (atomic_load(rc->cancelled) == 0
           && (code = rc->reader->read(rc->ctx, &data)) == PIPE_OK) {
        if (channel_send(rc->output, data) != 0) {
            // Channel was closed downstream — drop the in-flight item.
            rc->reader->destroy_item(data);
            atomic_store(rc->cancelled, 1);
            break;
        }
    }

    if (code == PIPE_ERR) {
        atomic_store(rc->cancelled, 1);
    }

    // On cancellation, close output to unblock any peer waiting on send/receive.
    // On normal EOF the channel is closed by pipe_run after all readers join.
    if (atomic_load(rc->cancelled)) {
        channel_close(rc->output);
    }

    rc->reader->close(rc->ctx);
    return NULL;
}

/**
 * @brief Binds a @ref Writer to an input @ref Channel and runs it on a thread.
 */
typedef struct {
    Writer *writer;          /**< @brief Sink for data items. */
    void *ctx;
    Channel *input;          /**< @brief Channel to receive items from. */
    atomic_int *cancelled;   /**< @brief Shared cancellation flag; set to 1 to stop the pipeline. */
} WriteChain;

/** @brief Allocates and initialises a new @ref WriteChain. */
static WriteChain *write_chain_new(Writer *writer, void *ctx, Channel *input, atomic_int *cancelled) {
    WriteChain *wc = malloc(sizeof(WriteChain));
    if (wc == NULL) return NULL;

    wc->writer    = writer;
    wc->ctx       = ctx;
    wc->input     = input;
    wc->cancelled = cancelled;

    return wc;
}

/** @brief Frees a @ref WriteChain. Does not touch the wrapped writer/channel. */
static void write_chain_destroy(WriteChain *wc) {
    if (wc == NULL) return;
    free(wc);
}

/**
 * @brief Thread entry point for a @ref WriteChain.
 *
 * Receives items from @c wc->input and writes each via @c wc->writer. Stops
 * when the channel is closed and drained, the writer signals an error, or
 * @c cancelled is raised. On error, closes its input channel so any peer
 * blocked on send wakes up.
 *
 * @param[in] arg  Pointer to a @ref WriteChain. Must not be @c NULL.
 * @return         Always @c NULL.
 */
static void *write_chain_run(void *arg) {
    WriteChain *wc = arg;

    void *data = NULL;
    int code = PIPE_OK;

    while (atomic_load(wc->cancelled) == 0
           && (code = channel_receive(wc->input, &data)) == 0) {
        if (wc->writer->write(wc->ctx, &data) != PIPE_OK) {
            // Writer reports fatal — drop the in-flight item it didn't take.
            wc->writer->destroy_item(data);
            atomic_store(wc->cancelled, 1);
            break;
        }
    }

    // Whole-batch error from writer — already handled above. The local `code`
    // here only signals receive outcome (0 = ok, -1 = closed-and-drained).

    // On cancellation, close input to unblock any peer waiting on send.
    if (atomic_load(wc->cancelled)) {
        channel_close(wc->input);
    }

    wc->writer->close(wc->ctx);
    return NULL;
}

/**
 * @brief Top-level pipeline that fans data from N readers to M writers.
 *
 * Topology is determined at construction time by @ref pipe_new:
 * - **N == M**: each reader is paired with its own writer through a dedicated channel.
 * - **N != M**: all readers and all writers share a single channel (fan-in / fan-out).
 */
struct Pipe {
    ReadChain **readers_chain;
    size_t readers_count;
    size_t readers_created;

    WriteChain **writers_chain;
    size_t writers_count;
    size_t writers_created;

    Channel **chans;
    size_t chans_count;
    size_t chans_created;

    atomic_int cancelled;
};

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

    if (pipe->chans != NULL) {
        for (size_t i = 0; i < pipe->chans_created; i++) {
            channel_destroy(pipe->chans[i]);
        }
    }
    free(pipe->chans);

    free(pipe);
}

/**
 * @brief Validates that every reader/writer in the input arrays has all
 *        required callbacks set.
 */
static int validate_endpoints(Reader *readers, size_t rc, Writer *writers, size_t wc) {
    if (readers == NULL || writers == NULL) return -1;

    for (size_t i = 0; i < rc; i++) {
        if (readers[i].read == NULL
            || readers[i].close == NULL
            || readers[i].destroy_item == NULL) return -1;
    }
    for (size_t i = 0; i < wc; i++) {
        if (writers[i].write == NULL
            || writers[i].close == NULL
            || writers[i].destroy_item == NULL) return -1;
    }
    return 0;
}

Pipe *pipe_new(Reader *readers, size_t readers_count,
               Writer *writers, size_t writers_count) {
    if (readers_count == 0 || writers_count == 0) return NULL;
    if (validate_endpoints(readers, readers_count, writers, writers_count) != 0) return NULL;

    Pipe *pipe = calloc(1, sizeof(Pipe));
    if (pipe == NULL) return NULL;

    pipe->readers_count = readers_count;
    pipe->writers_count = writers_count;
    atomic_init(&pipe->cancelled, 0);

    // calloc — zero-initialised slots survive partial init without garbage.
    pipe->readers_chain = calloc(readers_count, sizeof(ReadChain *));
    if (pipe->readers_chain == NULL) goto cleanup;

    pipe->writers_chain = calloc(writers_count, sizeof(WriteChain *));
    if (pipe->writers_chain == NULL) goto cleanup;

    pipe->chans_count = (readers_count == writers_count) ? readers_count : 1;

    pipe->chans = calloc(pipe->chans_count, sizeof(Channel *));
    if (pipe->chans == NULL) goto cleanup;

    if (readers_count == writers_count) {
        // Paired topology — one channel per reader/writer pair.
        for (size_t i = 0; i < readers_count; i++) {
            Channel *ch = channel_new(CHANNEL_SIZE);
            if (ch == NULL) goto cleanup;
            pipe->chans[i] = ch;
            pipe->chans_created++;

            ReadChain *rc = read_chain_new(&readers[i], readers[i].ctx,
                                           pipe->chans[i], &pipe->cancelled);
            if (rc == NULL) goto cleanup;
            pipe->readers_chain[i] = rc;
            pipe->readers_created++;

            WriteChain *wc = write_chain_new(&writers[i], writers[i].ctx,
                                             pipe->chans[i], &pipe->cancelled);
            if (wc == NULL) goto cleanup;
            pipe->writers_chain[i] = wc;
            pipe->writers_created++;
        }
    } else {
        // Fan-in / fan-out — one shared channel.
        Channel *ch = channel_new(CHANNEL_SIZE);
        if (ch == NULL) goto cleanup;
        pipe->chans[0] = ch;
        pipe->chans_created++;

        for (size_t i = 0; i < readers_count; i++) {
            ReadChain *rc = read_chain_new(&readers[i], readers[i].ctx,
                                           pipe->chans[0], &pipe->cancelled);
            if (rc == NULL) goto cleanup;
            pipe->readers_chain[i] = rc;
            pipe->readers_created++;
        }
        for (size_t i = 0; i < writers_count; i++) {
            WriteChain *wc = write_chain_new(&writers[i], writers[i].ctx,
                                             pipe->chans[0], &pipe->cancelled);
            if (wc == NULL) goto cleanup;
            pipe->writers_chain[i] = wc;
            pipe->writers_created++;
        }
    }

    return pipe;

cleanup:
    pipe_destroy(pipe);
    return NULL;
}

int pipe_run(Pipe *pipe) {
    pthread_t *read_threads  = malloc(sizeof(pthread_t) * pipe->readers_count);
    pthread_t *write_threads = malloc(sizeof(pthread_t) * pipe->writers_count);
    if (read_threads == NULL || write_threads == NULL) {
        free(read_threads);
        free(write_threads);
        return -1;
    }

    size_t readers_started = 0;
    size_t writers_started = 0;
    size_t readers_joined  = 0;
    size_t writers_joined  = 0;
    int rc = 0;

    // Writers first — consumers ready before producers fill the channel.
    for (size_t i = 0; i < pipe->writers_count; i++) {
        if (pthread_create(&write_threads[i], NULL,
                           write_chain_run, pipe->writers_chain[i]) != 0) {
            rc = -1;
            goto cleanup;
        }
        writers_started++;
    }

    for (size_t i = 0; i < pipe->readers_count; i++) {
        if (pthread_create(&read_threads[i], NULL,
                           read_chain_run, pipe->readers_chain[i]) != 0) {
            rc = -1;
            goto cleanup;
        }
        readers_started++;
    }

    // Wait for readers.
    for (size_t i = 0; i < pipe->readers_count; i++) {
        if (pthread_join(read_threads[i], NULL) != 0) {
            rc = -1;
            goto cleanup;
        }
        readers_joined++;
    }

    // Close channels — wakes up writers waiting on empty channels.
    for (size_t i = 0; i < pipe->chans_count; i++) {
        channel_close(pipe->chans[i]);
    }

    for (size_t i = 0; i < pipe->writers_count; i++) {
        if (pthread_join(write_threads[i], NULL) != 0) {
            rc = -1;
            goto cleanup;
        }
        writers_joined++;
    }

    free(read_threads);
    free(write_threads);
    return 0;

cleanup:
    // Force-close so any thread blocked on send/receive can exit.
    for (size_t i = 0; i < pipe->chans_count; i++) {
        channel_close(pipe->chans[i]);
    }

    // Join only what hasn't been joined yet — avoids double-join UB.
    for (size_t i = readers_joined; i < readers_started; i++) {
        pthread_join(read_threads[i], NULL);
    }
    for (size_t i = writers_joined; i < writers_started; i++) {
        pthread_join(write_threads[i], NULL);
    }

    free(read_threads);
    free(write_threads);
    return rc;
}