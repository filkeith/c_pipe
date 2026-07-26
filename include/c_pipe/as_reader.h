#ifndef C_PIPE_AS_READER_H
#define C_PIPE_AS_READER_H

#include <stdint.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
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
 * Diagnostics (@ref as_reader_error, @ref as_reader_scanned) are safe to
 * query only after the pipeline has finished, i.e. after @ref pipe_run
 * returns.
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
 * Self-sufficient: closes the bridge channel (unblocking a scan thread stuck
 * on a full channel), joins the background scan thread, drains and destroys
 * any records that never reached a consumer, then frees the reader. Safe to
 * call without a prior @ref as_reader_close. Does NOT close or destroy the
 * @c aerospike client — the caller owns it.
 *
 * @param[in] r  Reader to destroy. No-op if @c NULL.
 */
void as_reader_destroy(AerospikeReader *r);

/* ------------------------------------------------------------------ *
 * Diagnostics — safe to call only after @ref pipe_run has returned.  *
 * ------------------------------------------------------------------ */

/**
 * @brief Reports whether the scan failed, copying the first error seen.
 *
 * @param[in]  r    Reader. May be @c NULL (returns 0).
 * @param[out] out  Destination for the error. Untouched if no error was
 *                  recorded. May be @c NULL if only the flag is needed.
 * @return  @c 1 if the scan failed (transport or parse error), @c 0 otherwise.
 *
 * @note An early termination requested via @ref as_reader_close is not
 *       counted as an error.
 */
int as_reader_error(AerospikeReader *r, as_error *out);

/**
 * @brief Number of records the scan produced (pushed into the pipeline).
 *
 * Useful as a migration checksum: on a clean run
 * @c scanned == inserted + skipped + failed.
 */
uint64_t as_reader_scanned(AerospikeReader *r);

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