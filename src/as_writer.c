#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "c_pipe/as_writer.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_WRITER_DEFAULT_BATCH 128

struct AerospikeWriter {
    // Client that is inited outside.
    aerospike *as;
    // Last error, for clear logging. Not protected, should be read after evrything is closed.
    as_error last_error;
    // Errors processing.
    atomic_int error;

    AerospikeWriterConfig cfg;

    // Policies.
    as_policy_batch       batch_policy;
    as_policy_batch_write write_policy;

    // Buffer to accumulate batch.
    as_record **buf;
    size_t buf_count;

    // Stats.
    atomic_uint_fast64_t inserted;
    atomic_uint_fast64_t failed;
};

AerospikeWriter *as_writer_new(aerospike *as, AerospikeWriterConfig cfg) {
    if (as == NULL || cfg.ns == NULL || cfg.set == NULL) return NULL;

    // Init writer with 0's.
    AerospikeWriter *w = calloc(1, sizeof(AerospikeWriter));
    if (w == NULL) return NULL;

    w->as  = as;
    w->cfg = cfg;
    atomic_init(&w->inserted, 0);
    atomic_init(&w->failed,   0);
    atomic_init(&w->error,    0);

    // Config with defaults.
    w->cfg.batch_size  = cfg.batch_size  ? cfg.batch_size : AS_WRITER_DEFAULT_BATCH;

    // Allocate pending buffer.
    w->buf = malloc(sizeof(as_record *) * w->cfg.batch_size);
    if (w->buf == NULL) goto cleanup;

    // Init polices.
    as_policy_batch_init(&w->batch_policy);
    as_policy_batch_write_init(&w->write_policy);

    // Overwrite if exists, create otherwise.
    w->write_policy.exists = AS_POLICY_EXISTS_IGNORE;
    w->write_policy.gen    = AS_POLICY_GEN_IGNORE;

    return w;

cleanup:
    free(w);
    return NULL;
}

void as_writer_destroy(AerospikeWriter *w) {
    if (w == NULL) return;

    // Clean buffer.
    for (size_t i = 0; i < w->buf_count; i++) {
        as_record_destroy(w->buf[i]);
    }

    free(w->buf);
    free(w);
}

static as_operations *build_write_ops(const as_record *rec) {
    uint16_t n = rec->bins.size;
    as_operations *ops = as_operations_new(n);
    if (ops == NULL) return NULL;

    for (uint16_t i = 0; i < n; i++) {
        as_bin *b = &rec->bins.entries[i];
        if (b->valuep == NULL) continue;

        // Reserve, as_operations_destroy will decref the val.
        as_val_reserve((as_val *)b->valuep);
        as_operations_add_write(ops, b->name, (as_bin_value *)b->valuep);
    }

    ops->ttl = rec->ttl;

    return ops;
}

static int attach_record(AerospikeWriter *w, as_batch_records *recs, as_record *rec) {
    as_batch_write_record *bw = as_batch_write_reserve(recs);
    if (bw == NULL) return PIPE_ERR;

    if (rec->key.valuep != NULL) {
        as_key_init_value(&bw->key, w->cfg.ns, w->cfg.set, rec->key.valuep);
    } else {
        as_key_init_digest(&bw->key, w->cfg.ns, w->cfg.set, rec->key.digest.value);
    }

    as_operations *ops = build_write_ops(rec);
    if (ops == NULL) return PIPE_ERR;

    bw->ops    = ops;
    bw->policy = &w->write_policy;

    return PIPE_OK;
}

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
            break;
        }

        // Execute.
        as_error err;
        as_status st = aerospike_batch_write(w->as, &err, &w->batch_policy, &recs);

        size_t survivors = 0;

        if (st != AEROSPIKE_OK) {
            // All failed, retry whole batch.
            w->last_error = err;
            atomic_store(&w->error, 1);
            survivors = n;
        } else {
            // Inspect records in the batch.
            for (size_t i = 0; i < n; i++) {
                as_batch_write_record *br =
                    (as_batch_write_record *)as_vector_get(&recs.list, (uint32_t)i);
                // Check ech record on by one.
                if (br->result == AEROSPIKE_OK) {
                    atomic_fetch_add(&w->inserted, 1);
                    as_record_destroy(w->buf[i]);
                    w->buf[i] = NULL;
                } else {
                    // On batch record error.
                    as_error_setall(&w->last_error, br->result,
                                    "batch record failed", "", "", 0);
                    atomic_store(&w->error, 1);

                    // Repack array.
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

    // Anything left after retries count as failed.
    for (size_t i = 0; i < n; i++) {
        atomic_fetch_add(&w->failed, 1);
        as_record_destroy(w->buf[i]);
        w->buf[i] = NULL;
    }

    w->buf_count = 0;
}

int as_writer_write(void *ctx, void **data) {
    AerospikeWriter *w = ctx;

    // TODO: protection from free(data) in PIPE, need to rewrtrie this part.
    as_record *rec = (as_record *)*data;
    *data = NULL;

    if (rec == NULL) {
        as_error_setall(&w->last_error, AEROSPIKE_ERR_PARAM,
                        "writer received NULL record", "", "", 0);
        atomic_store(&w->error, 1);

        return PIPE_ERR;
    }

    // Put record to buffer.
    w->buf[w->buf_count++] = rec;
    // If buffer full, flush.
    if (w->buf_count >= w->cfg.batch_size) {
        flush(w);
    }


    return PIPE_OK;
}

int as_writer_close(void *ctx) {
    AerospikeWriter *w = ctx;
    flush(w);

    return PIPE_OK;
}