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
 * Typical usage:
 * @code
 *   Reader readers[2] = { {my_read, my_close}, {my_read, my_close} };
 *   Writer writers[2] = { {my_write, my_close}, {my_write, my_close} };
 *
 *   Pipe *p = pipe_new(readers, 2, writers, 2);
 *   pipe_run(p);
 *   pipe_destroy(p);
 * @endcode
 */


/** @brief Return codes for @ref Reader.read and @ref Writer.write. */
#define PIPE_OK    0   /**< Success. */
#define PIPE_EOF   1   /**< End of data, normal termination. */
#define PIPE_ERR  -1   /**< Error, pipeline should be cancelled. */

/* -------------------------------------------------------------------------
 * Reader / Writer interfaces
 * ---------------------------------------------------------------------- */

/**
 * @brief Abstract reader interface.
 *
 * The caller supplies concrete function pointers. @c read produces one item
 * per call; @c close releases any underlying resources (file, socket, etc.).
 */
typedef struct {
    /**
     * @brief Produce one item.
     *
     * @param[out] data  Set to a heap-allocated pointer owned by the caller
     *                   after a successful read.
     * @return  @c 0 on success, non-zero on EOF or error.
     */
    int (*read)(void **data);

    /**
     * @brief Release resources held by the reader.
     * @return  @c 0 on success, non-zero on error.
     */
    int (*close)();
} Reader;

/**
 * @brief Abstract writer interface.
 *
 * The caller supplies concrete function pointers. @c write consumes one item
 * per call; @c close flushes and releases any underlying resources.
 */
typedef struct {
    /**
     * @brief Consume one item.
     *
     * @param[in] data  Pointer to the item to write.
     * @return  @c 0 on success, non-zero on error.
     */
    int (*write)(void **data);

    /**
     * @brief Flush and release resources held by the writer.
     * @return  @c 0 on success, non-zero on error.
     */
    int (*close)();
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
 * On failure all channels are closed and all started threads are joined
 * before returning.
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