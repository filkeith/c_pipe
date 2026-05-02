#ifndef C_PIPE_AS_WRITER_H
#define C_PIPE_AS_WRITER_H

#include <stddef.h>
#include <stdint.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_error.h>
#include <aerospike/aerospike_batch.h>
#include <aerospike/as_batch.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_val.h>
#include <aerospike/as_vector.h>

/**
 * @brief Opaque Aerospike batch writer handle.
 *
 * Buffers @c as_record* items received via @ref as_writer_write into an
 * internal fixed-size buffer; flushes via @c aerospike_batch_write once the
 * buffer is full or @ref as_writer_close is called. Failed records are
 * retried up to @c max_retries times per flush, then counted as permanently
 * failed and the pipeline keeps going.
 *
 * @note The writer takes ownership of every record passed in and calls
 *       @c as_record_destroy on each, regardless of success or failure.
 */
typedef struct AerospikeWriter AerospikeWriter;

/** @brief Writer config. Zero-fields take defaults. */
typedef struct {
    uint32_t batch_size;   /**< Records per flush. 0 = default (128). */
    uint32_t max_retries;  /**< Per-batch retry attempts for failed records. 0 = no retry. */
    const char *ns;
    const char *set;
} AerospikeWriterConfig;

/**
 * @brief Allocates and initialises a new AerospikeWriter.
 *
 * @param[in] as   Connected aerospike client. Must not be @c NULL.
 *                 Caller retains ownership — writer never closes it.
 * @param[in] ns   Target namespace. Lifetime must outlive the writer.
 * @param[in] set  Target set. Lifetime must outlive the writer.
 * @param[in] cfg  Writer configuration. Pass @c {0,0} for defaults.
 * @return         Pointer to the new writer, or @c NULL on failure.
 */
AerospikeWriter *as_writer_new(aerospike *as, AerospikeWriterConfig cfg);

/**
 * @brief @ref Writer.write implementation — buffers one record; flushes when full.
 *
 * @param[in]     ctx   Pointer to @ref AerospikeWriter.
 * @param[in,out] data  Pointer to @c as_record*. Writer takes ownership and
 *                      sets @c *data to @c NULL before returning.
 * @return  Always @c PIPE_OK; per-record errors are recorded in stats.
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
 * @param[in] w  Writer to destroy. No-op if @c NULL.
 */
void as_writer_destroy(AerospikeWriter *w);

/* Diagnostics — safe to call only after @ref pipe_run has returned. */

uint64_t as_writer_inserted(AerospikeWriter *w);
uint64_t as_writer_failed(AerospikeWriter *w);
void     as_writer_last_error(AerospikeWriter *w, as_error *out);

#endif /* C_PIPE_AS_WRITER_H */