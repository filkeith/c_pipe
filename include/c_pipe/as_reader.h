#ifndef C_PIPE_AS_READER_H
#define C_PIPE_AS_READER_H

#include <aerospike/aerospike.h>
#include <aerospike/as_partition_filter.h>

/**
 * @file as_reader.h
 * @brief Aerospike partition scanner exposed as a pull-based @ref Reader.
 *
 * Wraps @c aerospike_scan_partitions (push model — AS calls a callback for
 * every record) into the pull-based @ref Reader interface required by the
 * pipeline. The scan runs on a background thread and pushes records into
 * an internal @ref Channel; @ref as_reader_read drains that channel.
 *
 * Diagnostic getters (@ref as_writer_failed-style accessors) are not added
 * here yet — query them by reading public fields after @ref pipe_run returns.
 */

/** @brief Opaque reader handle. */
typedef struct AerospikeReader AerospikeReader;

/**
 * @brief Reader configuration.
 *
 * @note @c ns and @c set are stored by reference. Their underlying storage
 *       must outlive the reader.
 */
typedef struct {
    const char *ns;             /**< Namespace. Must not be @c NULL. */
    const char *set;            /**< Set. Must not be @c NULL. */
    as_partition_filter pf;     /**< Partition range to scan. */
} AerospikeReaderConfig;

/**
 * @brief Allocates and initialises a new reader.
 *
 * @param[in] as   Connected aerospike client. Must not be @c NULL.
 *                 Caller retains ownership; the reader never closes it.
 * @param[in] cfg  Reader configuration. @c ns and @c set must outlive the reader.
 * @return         Pointer to the new reader, or @c NULL on invalid input
 *                 or allocation failure.
 */
AerospikeReader *as_reader_new(aerospike *as, AerospikeReaderConfig cfg);

/**
 * @brief Starts the background scan thread.
 *
 * Must be called before passing the reader to @ref pipe_run, otherwise
 * @ref as_reader_read will block forever.
 *
 * @param[in] r  Reader. Must not be @c NULL.
 * @return  @c 0 on success, @c -1 if the thread could not be created.
 */
int as_reader_start(AerospikeReader *r);

/**
 * @brief @ref Reader.read implementation — pulls one record from the scan.
 *
 * Blocks until a record is available, the scan completes, or an error occurs.
 * Pass as the @c read function pointer in a @ref Reader struct, with the
 * @ref AerospikeReader pointer as @c ctx.
 *
 * @param[in]  ctx   Pointer to @ref AerospikeReader.
 * @param[out] data  Set to @c as_record* on success. Caller (the writer) owns
 *                   it and must call @c as_record_destroy after processing.
 * @return  @c PIPE_OK   record produced.
 * @return  @c PIPE_EOF  scan complete, no more records.
 * @return  @c PIPE_ERR  scan or parse error; see @ref as_reader_last_error.
 */
int as_reader_read(void *ctx, void **data);

/**
 * @brief @ref Reader.close implementation — requests early scan termination.
 *
 * Idempotent. Called by the pipeline both on normal EOF and on cancellation.
 * On normal EOF the scan thread has already exited, so this is a no-op cancel.
 *
 * @param[in] ctx  Pointer to @ref AerospikeReader.
 * @return  Always @c 0.
 */
int as_reader_close(void *ctx);

/**
 * @brief Destroys the reader and frees all resources.
 *
 * Joins the background scan thread before freeing. Does NOT close or destroy
 * the @c aerospike client — the caller owns it.
 *
 * @param[in] r  Reader to destroy. No-op if @c NULL.
 */
void as_reader_destroy(AerospikeReader *r);

/**
 * @brief @ref Reader.destroy_item implementation — wraps @c as_record_destroy.
 *
 * Pass as the @c destroy_item function pointer in a @ref Reader struct.
 * Used by the pipeline to release records that were produced by the reader
 * but could not be delivered downstream (e.g. when a writer aborted and
 * the channel was closed mid-send).
 *
 * @param[in] data  @c as_record* previously produced by @ref as_reader_read.
 *                  Must not be @c NULL.
 */
void as_reader_destroy_item(void *data);

#endif /* C_PIPE_AS_READER_H */