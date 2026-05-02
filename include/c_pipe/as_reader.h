#ifndef C_PIPE_AS_READER_H
#define C_PIPE_AS_READER_H

#include <aerospike/aerospike.h>
#include <aerospike/aerospike_scan.h>
#include <aerospike/as_partition_filter.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>


#include "c_pipe/pipe.h"

/**
 * @brief Opaque Aerospike partition reader handle.
 *
 * Wraps @c aerospike_scan_partitions (push model) into a pull-based
 * @ref Reader by running the scan on a background thread and bridging
 * records through an internal @ref Channel.
 */
typedef struct AerospikeReader AerospikeReader;

typedef struct {
    const char *ns;
    const char *set;
    as_partition_filter pf;
} AerospikeReaderConfig;


/**
 * @brief Allocates and initialises a new AerospikeReader.
 *
 * @param[in] as   Connected aerospike client. Must not be @c NULL.
 *                 Caller retains ownership — reader never closes it.
 * @param cfg
 * @return         Pointer to the new reader, or @c NULL on failure.
 */
AerospikeReader *as_reader_new(aerospike *as, AerospikeReaderConfig cfg);

/**
 * @brief Starts the background scan thread.
 *
 * Must be called before passing the reader to @ref pipe_run.
 *
 * @param[in] r  Reader to start. Must not be @c NULL.
 * @return  @c 0 on success, @c -1 if the thread could not be created.
 */
int as_reader_start(AerospikeReader *r);

/**
 * @brief @ref Reader.read implementation — pulls one @c as_record from the scan.
 *
 * Blocks until a record is available, the scan is complete, or an error occurs.
 * Pass this as the @c read function pointer in a @ref Reader struct,
 * and the @ref AerospikeReader pointer as @c ctx.
 *
 * @param[in]  ctx   Pointer to @ref AerospikeReader.
 * @param[out] data  Set to @c as_record* on success. Caller must call
 *                   @c as_record_destroy() after processing.
 * @return  @c PIPE_OK   record available in @p *data.
 * @return  @c PIPE_EOF  scan complete, no more records.
 * @return  @c PIPE_ERR  scan error.
 */
int as_reader_read(void *ctx, void **data);

/**
 * @brief @ref Reader.close implementation — no-op.
 *
 * Real cleanup is done in @ref as_reader_destroy.
 * Pass this as the @c close function pointer in a @ref Reader struct.
 *
 * @param[in] ctx  Unused.
 * @return  Always @c 0.
 */
int as_reader_close(void *ctx);

/**
 * @brief Destroys the reader and frees all resources.
 *
 * Joins the background scan thread before freeing. Does NOT close
 * or destroy the @c aerospike client — caller owns it.
 *
 * @param[in] r  Reader to destroy. No-op if @c NULL.
 */
void as_reader_destroy(AerospikeReader *r);

#endif //C_PIPE_AS_READER_H

