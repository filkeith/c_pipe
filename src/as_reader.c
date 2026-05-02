#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "c_pipe/as_reader.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_READER_QUEUE_SIZE 64

struct AerospikeReader {
    // Client that is inited outside.
    aerospike *as;
    // Last error, for clear logging. Not protected, should be read after evrything is closed.
    as_error last_error;

    // Scan config and partition range.
    as_scan scan;

    AerospikeReaderConfig cfg;

    // Internal chan to process results from as.
    Channel *chan;

    // Background thread to process results from as.
    pthread_t thread;

    // Errors processing.
    atomic_int error;
    atomic_int cancelled;

    atomic_int started;

    // Stats.
    atomic_uint_fast64_t scanned;

    //TODO: scan config?
};

AerospikeReader *as_reader_new(aerospike *as, AerospikeReaderConfig cfg) {
    if (as == NULL || cfg.ns == NULL || cfg.set == NULL) return NULL;

    // Init reader with 0's.
    AerospikeReader *r = calloc(1, sizeof(AerospikeReader));
    if (r == NULL) return NULL;

    r->as = as;
    r->cfg = cfg;
    atomic_init(&r->error, 0);
    atomic_init(&r->cancelled, 0);
    atomic_init(&r->started, 0);
    atomic_init(&r->scanned, 0);

    // Init scan.
    as_scan_init(&r->scan, r->cfg.ns, r->cfg.set);

    // Create channel for results.
    Channel *ch = channel_new(AS_READER_QUEUE_SIZE);
    if (ch == NULL) goto cleanup;

    r->chan = ch;

    return r;

cleanup:
    as_scan_destroy(&r->scan);
    free(r);
    return NULL;
}

void as_reader_destroy(AerospikeReader *r) {
    if (r == NULL) return;

    // Wait for scan thread to finish.
    if (atomic_load(&r->started)) {
        pthread_join(r->thread, NULL);
    }

    channel_destroy(r->chan);
    as_scan_destroy(&r->scan);
    free(r);
}

// Processing records.
static bool as_scan_callback(const as_val *val, void *arg) {
    AerospikeReader *r = arg;

    if (!val) {
        // Scan complete
        channel_close(r->chan);
        return true;
    }

    // Copy record.
    as_record *rec = as_record_fromval(val);
    if (rec == NULL) {
        atomic_store(&r->cancelled, 1);

        return false;
    }
    // Reserver after success only.
    as_val_reserve((as_val *)val);

    // Send record to chan.
    if (channel_send(r->chan, rec) != 0) {
        as_record_destroy(rec);
        atomic_store(&r->cancelled, 1);

        return false;
    }

    // Increase scan counter.
    atomic_fetch_add(&r->scanned, 1);

    return true;
}

// Thread task.
static void *scan_run(void *arg) {
    AerospikeReader *r = arg;

    as_error err;
    as_status status = aerospike_scan_partitions(r->as, &err, NULL, &r->scan, &r->cfg.pf, as_scan_callback, r);

    if (status != AEROSPIKE_OK && !atomic_load(&r->cancelled)) {
        r->last_error = err;
        atomic_store(&r->error, 1);
    }

    // Close chan at the end of scan.
    channel_close(r->chan);

    return NULL;
}

int as_reader_start(AerospikeReader *r) {
    if (r == NULL) return -1;
    if (pthread_create(&r->thread, NULL, scan_run, r) != 0) return -1;
    atomic_store(&r->started, 1);

    return PIPE_OK;
}


int as_reader_read(void *ctx, void **data) {
    AerospikeReader *r = ctx;

    if (channel_receive(r->chan, data) != 0) {
        return atomic_load(&r->error) ? PIPE_ERR : PIPE_EOF;
    }

    return PIPE_OK;
}

int as_reader_close(void *ctx) {
    AerospikeReader *r = ctx;
    atomic_store(&r->cancelled, 1);
    channel_close(r->chan);

    return PIPE_OK;
}