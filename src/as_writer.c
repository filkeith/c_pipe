#include <assert.h>
#include <stdbool.h>
#include <stdlib.h>
#include <stdatomic.h>

#include <aerospike/aerospike_batch.h>
#include <aerospike/as_key.h>
#include <aerospike/as_operations.h>
#include <aerospike/as_policy.h>
#include <aerospike/as_record.h>
#include <aerospike/as_val.h>
#include <aerospike/as_vector.h>

#include "c_pipe/as_writer.h"
#include "c_pipe/pipe.h"

#define AS_WRITER_DEFAULT_BATCH 128

/**
 * @brief Single-threaded buffered batch writer.
 *
 * Pipe contract guarantees one writer is driven from one thread, so the
 * buffer needs no locking. Stats and the error flag are atomic to allow
 * external diagnostics during pipeline run.
 *
 * @c last_error stores only the FIRST error seen; subsequent errors are
 * counted but not overwritten so diagnostic state stays stable.
 */
struct AerospikeWriter {
    // Client, inited outside. Not owned.
    aerospike *as;

    // Pending records buffer. Owned by the writer until destroyed or sent.
    as_record **buf;
    size_t buf_count;

    // Stats.
    atomic_uint_fast64_t inserted;
    atomic_uint_fast64_t failed;
    atomic_int error_set;

    AerospikeWriterConfig cfg;
    // Pre-built policies, reused on every flush.
    as_policy_batch       batch_policy;
    as_policy_batch_write write_policy;

    // First-error capture. Written under the error_set guard.
    as_error last_error;
};

/**
 * @brief Records the first error seen; later errors are dropped.
 *
 * Single-threaded write context, but @c error_set is read by external
 * diagnostics — keep the store atomic.
 */
static void capture_error(AerospikeWriter *w, const as_error *err) {
    if (atomic_exchange(&w->error_set, 1) == 0) {
        w->last_error = *err;
    }
}

/**
 * @brief Builds an @c as_operations list with WRITE ops for every bin of @p rec.
 *
 * Each bin's @c as_val refcount is reserved so the ops can outlive @p rec.
 *
 * @return  New @c as_operations, or @c NULL on allocation failure.
 */
static as_operations *build_write_ops(const as_record *rec) {
    uint16_t n = rec->bins.size;
    as_operations *ops = as_operations_new(n);
    if (ops == NULL) return NULL;

    for (uint16_t i = 0; i < n; i++) {
        as_bin *b = &rec->bins.entries[i];
        if (b->valuep == NULL) continue;

        // Reserve — as_operations_destroy will decref the val.
        as_val_reserve((as_val *)b->valuep);
        as_operations_add_write(ops, b->name, (as_bin_value *)b->valuep);
    }

    // Carry source TTL across.
    ops->ttl = rec->ttl;
    return ops;
}

/**
 * @brief Attaches one record to a batch as an @c as_batch_write_record slot.
 *
 * Builds ops BEFORE reserving the slot so a build failure leaves @p recs
 * untouched (no half-initialised slots that as_batch_records_destroy would
 * choke on).
 */
static int attach_record(AerospikeWriter *w, as_batch_records *recs, as_record *rec) {
    // Build ops first — on failure, no slot is taken.
    as_operations *ops = build_write_ops(rec);
    if (ops == NULL) return PIPE_ERR;

    as_batch_write_record *bw = as_batch_write_reserve(recs);
    if (bw == NULL) {
        as_operations_destroy(ops);
        return PIPE_ERR;
    }

    if (rec->key.valuep != NULL) {
        as_key_init_value(&bw->key, w->cfg.ns, w->cfg.set, rec->key.valuep);
    } else {
        as_key_init_digest(&bw->key, w->cfg.ns, w->cfg.set, rec->key.digest.value);
    }

    bw->ops    = ops;
    bw->policy = &w->write_policy;
    return PIPE_OK;
}

/**
 * @brief Flushes @c w->buf — one batch_write call plus retries for failed records.
 *
 * In-place compaction: survivors of each pass are packed at the front of
 * @c w->buf and retried. After @c max_retries exhausted attempts, leftovers
 * are destroyed and counted in @c failed. Build failures (e.g. OOM in
 * @ref attach_record) are treated as fatal for the remaining records — no
 * retry, since OOM is unlikely to clear on the next pass.
 */
static void flush(AerospikeWriter *w) {
    if (w->buf_count == 0) return;

    size_t n = w->buf_count;

    for (uint32_t attempt = 0; attempt <= w->cfg.max_retries && n > 0; attempt++) {
        as_batch_records recs;
        as_batch_records_init(&recs, n);

        // Build the batch.
        bool build_ok = true;
        for (size_t i = 0; i < n; i++) {
            if (attach_record(w, &recs, w->buf[i]) != PIPE_OK) {
                build_ok = false;
                break;
            }
        }

        if (!build_ok) {
            as_batch_records_destroy(&recs);
            // Bail out of retry — survivors get destroyed at the end of flush.
            break;
        }

        // Execute.
        as_error err;
        as_status st = aerospike_batch_write(w->as, &err, &w->batch_policy, &recs);

        size_t survivors = 0;

        if (st != AEROSPIKE_OK) {
            // Whole-batch transport failure — retry everything as-is.
            capture_error(w, &err);
            survivors = n;
        } else {
            // Per-record inspection. Compact survivors in place.
            for (size_t i = 0; i < n; i++) {
                as_batch_write_record *br =
                    (as_batch_write_record *)as_vector_get(&recs.list, (uint32_t)i);

                if (br->result == AEROSPIKE_OK) {
                    atomic_fetch_add(&w->inserted, 1);
                    as_record_destroy(w->buf[i]);
                    w->buf[i] = NULL;
                } else {
                    as_error rerr;
                    as_error_setall(&rerr, br->result,
                                    "batch record failed", "", "", 0);
                    capture_error(w, &rerr);

                    // Compact survivor at the front.
                    if (survivors != i) {
                        w->buf[survivors] = w->buf[i];
                        w->buf[i] = NULL;
                    }
                    survivors++;
                }
            }
        }

        as_batch_records_destroy(&recs);
        n = survivors;
    }

    // Anything left after retries is permanently failed.
    for (size_t i = 0; i < n; i++) {
        atomic_fetch_add(&w->failed, 1);
        as_record_destroy(w->buf[i]);
        w->buf[i] = NULL;
    }

    w->buf_count = 0;
}

AerospikeWriter *as_writer_new(aerospike *as, AerospikeWriterConfig cfg) {
    if (as == NULL || cfg.ns == NULL || cfg.set == NULL) return NULL;

    // Apply defaults to the incoming config before storing it.
    if (cfg.batch_size == 0) cfg.batch_size = AS_WRITER_DEFAULT_BATCH;

    // Init writer with 0's.
    AerospikeWriter *w = calloc(1, sizeof(AerospikeWriter));
    if (w == NULL) return NULL;

    w->as  = as;
    w->cfg = cfg;
    atomic_init(&w->inserted,  0);
    atomic_init(&w->failed,    0);
    atomic_init(&w->error_set, 0);

    // Allocate pending buffer.
    w->buf = malloc(sizeof(as_record *) * w->cfg.batch_size);
    if (w->buf == NULL) goto cleanup;

    // Pre-build policies — used as-is on every flush.
    as_policy_batch_init(&w->batch_policy);
    as_policy_batch_write_init(&w->write_policy);

    // Overwrite if exists, create otherwise. Generation conflicts ignored.
    w->write_policy.exists = AS_POLICY_EXISTS_IGNORE;
    w->write_policy.gen    = AS_POLICY_GEN_IGNORE;

    return w;

cleanup:
    free(w);
    return NULL;
}

void as_writer_destroy(AerospikeWriter *w) {
    if (w == NULL) return;

    // Defensive: should be empty if close() was called.
    for (size_t i = 0; i < w->buf_count; i++) {
        as_record_destroy(w->buf[i]);
    }

    free(w->buf);
    free(w);
}

int as_writer_write(void *ctx, void **data) {
    AerospikeWriter *w = ctx;

    // Take ownership immediately — pipe.c must not touch it after this.
    // (Defends against the free(*data) in write_chain_run on error.)
    as_record *rec = (as_record *)*data;
    *data = NULL;

    // Contract: reader must produce a non-NULL record on PIPE_OK.
    assert(rec != NULL);
    if (rec == NULL) {
        as_error err;
        as_error_setall(&err, AEROSPIKE_ERR_PARAM,
                        "writer received NULL record", "", "", 0);
        capture_error(w, &err);
        return PIPE_ERR;
    }

    // Buffer, flush if full.
    w->buf[w->buf_count++] = rec;
    if (w->buf_count >= w->cfg.batch_size) {
        flush(w);
    }

    return PIPE_OK;
}

int as_writer_close(void *ctx) {
    AerospikeWriter *w = ctx;
    flush(w);
    return 0;
}

void as_writer_destroy_item(void *data) {
    as_record_destroy((as_record *)data);
}

uint64_t as_writer_inserted(AerospikeWriter *w) {
    return atomic_load(&w->inserted);
}

uint64_t as_writer_failed(AerospikeWriter *w) {
    return atomic_load(&w->failed);
}

void as_writer_last_error(AerospikeWriter *w, as_error *out) {
    if (out != NULL && atomic_load(&w->error_set)) {
        *out = w->last_error;
    }
}