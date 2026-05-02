#ifndef C_PIPE_PIPE_H
#define C_PIPE_PIPE_H

#include <stddef.h>

/**
 * @file pipe.h
 * @brief Thread-safe fan-in / fan-out pipeline over @ref Channel.
 *
 * A @ref Pipe connects N @ref Reader instances to M @ref Writer instances
 * through one or more buffered channels:
 *
 * - **N == M**: each reader is paired with its own writer (1-to-1, dedicated channels).
 * - **N != M**: all readers and writers share a single channel (fan-in / fan-out).
 *
 * Items flowing through the channel are heap-allocated by the reader and
 * consumed by the writer. The pipeline is type-agnostic: items are passed as
 * @c void* and freed via the @c destroy_item callback supplied by each side.
 *
 * Typical usage:
 * @code
 *   static void int_destroy(void *p) { free(p); }
 *
 *   Reader readers[2] = {
 *       { my_read, my_close, int_destroy, &ctx_a },
 *       { my_read, my_close, int_destroy, &ctx_b },
 *   };
 *   Writer writers[2] = {
 *       { my_write, my_close, int_destroy, &ctx_c },
 *       { my_write, my_close, int_destroy, &ctx_d },
 *   };
 *
 *   Pipe *p = pipe_new(readers, 2, writers, 2);
 *   pipe_run(p);
 *   pipe_destroy(p);
 * @endcode
 */

/** @brief Return codes for @ref Reader and @ref Writer function pointers. */
#define PIPE_OK   (0)   /**< Success, item produced/consumed. */
#define PIPE_EOF  (1)   /**< End of data, normal termination. */
#define PIPE_ERR  (-1)  /**< Unrecoverable error, pipeline will be cancelled. */

/* -------------------------------------------------------------------------
 * Reader / Writer interfaces
 * ---------------------------------------------------------------------- */

/**
 * @brief Abstract reader interface.
 *
 * The caller supplies concrete function pointers and an execution context.
 * @c read produces one item per call; @c close releases the reader's
 * resources; @c destroy_item is used by the pipeline to free items it could
 * not deliver to a writer.
 *
 * @note @p ctx is owned and managed entirely by the caller. The pipeline
 *       passes it through opaquely and never frees it.
 */
typedef struct {
    /**
     * @brief Produce one item.
     *
     * @param[in]  ctx   Caller-supplied execution context. May be @c NULL.
     * @param[out] data  Set to a heap-allocated pointer on success.
     *                   Ownership transfers to the pipeline; the pipeline
     *                   passes it to a @ref Writer which is responsible for
     *                   freeing it. On non-OK return @p *data is left untouched.
     * @return  @c PIPE_OK   item produced successfully.
     * @return  @c PIPE_EOF  no more items, normal termination.
     * @return  @c PIPE_ERR  unrecoverable error, pipeline will be cancelled.
     */
    int (*read)(void *ctx, void **data);

    /**
     * @brief Release resources held by the reader.
     *
     * Called once by the pipeline after @c read returns @c PIPE_EOF or
     * @c PIPE_ERR, or when the pipeline is cancelled.
     *
     * @param[in] ctx  Same context pointer passed to @c read.
     * @return  @c 0 on success, non-zero on error.
     */
    int (*close)(void *ctx);

    /**
     * @brief Free a single item produced by @c read.
     *
     * Required (must not be @c NULL). Invoked by the pipeline if a produced
     * item could not be delivered downstream — for example when the output
     * channel was closed before @ref channel_send completed. For simple
     * heap-allocated payloads this can simply wrap @c free.
     *
     * @param[in] data  Item previously produced by @c read. Must not be @c NULL.
     */
    void (*destroy_item)(void *data);

    /**< @brief Caller-supplied context passed to @c read and @c close. May be @c NULL. */
    void *ctx;
} Reader;

/**
 * @brief Abstract writer interface.
 *
 * The caller supplies concrete function pointers and an execution context.
 * @c write consumes one item per call; @c close flushes and releases
 * resources; @c destroy_item is used by the pipeline to free items it
 * fetched from the channel but could not hand to @c write.
 *
 * @note @p ctx is owned and managed entirely by the caller. The pipeline
 *       passes it through opaquely and never frees it.
 */
typedef struct {
    /**
     * @brief Consume one item.
     *
     * On @c PIPE_OK return the writer has taken ownership of @c *data and is
     * responsible for freeing it. On @c PIPE_ERR the pipeline frees @c *data
     * via @c destroy_item; the writer must not free it itself in that path.
     *
     * @param[in]     ctx   Caller-supplied execution context. May be @c NULL.
     * @param[in,out] data  Pointer to the item to consume.
     * @return  @c PIPE_OK   item consumed successfully.
     * @return  @c PIPE_ERR  unrecoverable error, pipeline will be cancelled.
     */
    int (*write)(void *ctx, void **data);

    /**
     * @brief Flush and release resources held by the writer.
     *
     * Called once after the input channel is drained or the pipeline is
     * cancelled.
     *
     * @param[in] ctx  Same context pointer passed to @c write.
     * @return  @c 0 on success, non-zero on error.
     */
    int (*close)(void *ctx);

    /**
     * @brief Free a single item that was fetched but not delivered.
     *
     * Required (must not be @c NULL). Invoked by the pipeline if @c write
     * returns @c PIPE_ERR, so the in-flight item is not leaked.
     *
     * @param[in] data  Item previously fetched from the channel. Must not be @c NULL.
     */
    void (*destroy_item)(void *data);

    /**< @brief Caller-supplied context passed to @c write and @c close. May be @c NULL. */
    void *ctx;
} Writer;

/* -------------------------------------------------------------------------
 * Pipe — opaque handle
 * ---------------------------------------------------------------------- */

/**
 * @brief Opaque pipeline handle.
 *
 * Internal layout is private to @c pipe.c. Always allocate via @ref pipe_new
 * and free via @ref pipe_destroy.
 */
typedef struct Pipe Pipe;

/* -------------------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------------- */

/**
 * @brief Allocates and wires up a new pipeline.
 *
 * Every @ref Reader and @ref Writer must define all four function pointers,
 * including @c destroy_item.
 *
 * @param[in] readers        Array of @ref Reader instances. Must not be @c NULL.
 * @param[in] readers_count  Number of readers. Must be > 0.
 * @param[in] writers        Array of @ref Writer instances. Must not be @c NULL.
 * @param[in] writers_count  Number of writers. Must be > 0.
 * @return  Pointer to the new @ref Pipe, or @c NULL on invalid arguments or
 *          allocation failure.
 *
 * @note The caller must eventually call @ref pipe_destroy to free all resources.
 */
Pipe *pipe_new(Reader *readers, size_t readers_count,
               Writer *writers, size_t writers_count);

/**
 * @brief Starts all pipeline threads and blocks until they complete.
 *
 * Writers are launched before readers to ensure consumers are ready before
 * producers start filling the channel.
 *
 * Shutdown sequence on success:
 * 1. Join all reader threads (exit when reader signals EOF/error).
 * 2. Close all channels (unblocks writers blocked on an empty channel).
 * 3. Join all writer threads.
 *
 * On any reader/writer error the shared @c cancelled flag is raised; chains
 * that exit due to cancellation also close their I/O channel so peers blocked
 * on @ref channel_send / @ref channel_receive cannot deadlock.
 *
 * On thread creation/join failure all channels are closed and all started
 * threads are joined before returning.
 *
 * @param[in] pipe  Fully initialised pipeline. Must not be @c NULL.
 * @return  @c 0  all threads completed successfully.
 * @return  @c -1 thread creation or join failure.
 */
int pipe_run(Pipe *pipe);

/**
 * @brief Destroys the pipeline and releases all associated resources.
 *
 * Frees reader chains, writer chains, channels, and the @ref Pipe struct.
 * Safe to call on a partially initialised pipe.
 *
 * @param[in] pipe  Pipeline to destroy. No-op if @c NULL.
 *
 * @warning Must only be called after all threads have exited, i.e. after
 *          @ref pipe_run returns.
 */
void pipe_destroy(Pipe *pipe);

#endif /* C_PIPE_PIPE_H */