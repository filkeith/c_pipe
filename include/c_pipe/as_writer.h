#ifndef C_PIPE_AS_WRITER_H
#define C_PIPE_AS_WRITER_H

#include <stdint.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>

/**
 * @file as_writer.h
 * @brief Buffered batch writer that satisfies the @ref Writer interface.
 *
 * Records pushed via @ref as_writer_write are accumulated in an internal
 * fixed-size buffer; once full (or on @ref as_writer_close) the writer
 * flushes the buffer through @c aerospike_batch_write. Per-record failures
 * are retried up to @c max_retries times, then counted as permanent and
 * the pipeline keeps going — single record failures never fail the pipeline.
 *
 * @note The writer takes ownership of every @c as_record* passed in and
 *       calls @c as_record_destroy on it, regardless of success or failure.
 *
 * @note Single-threaded by design. The pipeline guarantees that one
 *       @ref Writer instance is driven from exactly one thread; no
 *       internal locking is performed on the buffer.
 */

/** @brief Opaque writer handle. */
typedef struct AerospikeWriter AerospikeWriter;

/**
 * @brief Writer configuration. Zero-fields take defaults.
 *
 * @note @c ns and @c set are stored by reference. Their underlying storage
 *       must outlive the writer.
 */
typedef struct {
    const char *ns;        /**< Target namespace. Must not be @c NULL. */
    const char *set;       /**< Target set. Must not be @c NULL. */
    uint32_t batch_size;   /**< Records per flush. 0 = default (128). */
    uint32_t max_retries;  /**< Retry attempts for failed records. 0 = no retry. */
} AerospikeWriterConfig;

/**
 * @brief Allocates and initialises a new writer.
 *
 * @param[in] as   Connected aerospike client. Must not be @c NULL.
 *                 Caller retains ownership; the writer never closes it.
 * @param[in] cfg  Writer configuration. @c ns and @c set must outlive the writer.
 * @return         Pointer to the new writer, or @c NULL on invalid input
 *                 or allocation failure.
 */
AerospikeWriter *as_writer_new(aerospike *as, AerospikeWriterConfig cfg);

/**
 * @brief @ref Writer.write implementation — buffers one record; flushes when full.
 *
 * Takes ownership of @c *data immediately and sets @c *data to @c NULL so the
 * pipeline cannot accidentally @c free it.
 *
 * @param[in]     ctx   Pointer to @ref AerospikeWriter.
 * @param[in,out] data  Pointer to @c as_record*. Ownership transfers to the writer.
 * @return  @c PIPE_OK   record buffered (and possibly flushed).
 * @return  @c PIPE_ERR  contract violation: @c *data was @c NULL.
 */
int as_writer_write(void *ctx, void **data);

/**
 * @brief @ref Writer.close implementation — flushes any buffered records.
 *
 * @param[in] ctx  Pointer to @ref AerospikeWriter.
 * @return  Always @c 0.
 */
int as_writer_close(void *ctx);

/**
 * @brief Destroys the writer and frees all resources.
 *
 * Defensive: destroys any records still left in the buffer (should be empty
 * if @ref as_writer_close was called).
 *
 * @param[in] w  Writer. No-op if @c NULL.
 */
void as_writer_destroy(AerospikeWriter *w);

/* ------------------------------------------------------------------ *
 * Diagnostics — safe to call only after @ref pipe_run has returned.  *
 * ------------------------------------------------------------------ */

/** @brief Number of records successfully written. */
uint64_t as_writer_inserted(AerospikeWriter *w);

/** @brief Number of records that exhausted retries and were dropped. */
uint64_t as_writer_failed(AerospikeWriter *w);

/**
 * @brief Copies the first error encountered by the writer, if any.
 *
 * Only the first error is preserved across the lifetime of the writer.
 * Subsequent errors are ignored to keep diagnostic state stable.
 *
 * @param[in]  w    Writer.
 * @param[out] out  Destination. Untouched if no error was recorded.
 */
void as_writer_last_error(AerospikeWriter *w, as_error *out);

/**
 * @brief @ref Writer.destroy_item implementation — wraps @c as_record_destroy.
 *
 * Pass as the @c destroy_item function pointer in a @ref Writer struct.
 * Used by the pipeline to release in-flight records that were fetched from
 * the channel but rejected by @ref as_writer_write (i.e. on @c PIPE_ERR
 * return — currently only the NULL-record contract violation).
 *
 * @note On the happy path this is never invoked: the writer takes ownership
 *       of every accepted record and destroys it itself during flush.
 *
 * @param[in] data  @c as_record* previously fetched from the channel.
 *                  Must not be @c NULL.
 */
void as_writer_destroy_item(void *data);

#endif /* C_PIPE_AS_WRITER_H */