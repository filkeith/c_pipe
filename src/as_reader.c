#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "c_pipe/as_reader.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_READER_QUEUE_SIZE 256

struct AerospikeReader {
    // Client that is inited outside.
    aerospike *as;

    // Scan config and partition range.
    as_scan scan;
    as_partition_filter pf;

    // Internal chan to process results from as.
    Channel *chan;

    // Background thread to process results from as.
    pthread_t thread;

    // Errors processing.
    atomic_int error;
    atomic_int cancelled;

    int started;
};

AerospikeReader *as_reader_new(aerospike *as, const char *ns, const char *set, as_partition_filter pf) {
    if (as == NULL || ns == NULL || set == NULL) return NULL;

    // Init reader.
    AerospikeReader *r = malloc(sizeof(AerospikeReader));
    if (r == NULL) return NULL;

    r->as = as;
    r->pf = pf;
    atomic_init(&r->error, 0);
    atomic_init(&r->cancelled, 0);

    // Init scan.
    as_scan_init(&r->scan, ns, set);

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
    if (r->started) {
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
    as_val_reserve((as_val *)val);
    as_record *rec = as_record_fromval(val);

    // Send record to chan.
    if (channel_send(r->chan, rec) != 0) {
        as_record_destroy(rec);
        atomic_store(&r->cancelled, 1);
        return false;
    }

    return true;
}

// Thread task.
static void *scan_run(void *arg) {
    AerospikeReader *r = arg;

    as_error err;
    as_status status = aerospike_scan_partitions(r->as, &err, NULL, &r->scan, &r->pf, as_scan_callback, r);

    if (status != AEROSPIKE_OK && !atomic_load(&r->cancelled)) {
        atomic_store(&r->error, 1);
        channel_close(r->chan);
    }

    return NULL;
}

int as_reader_start(AerospikeReader *r) {
    if (r == NULL) return -1;

    r->started = 1;

    return pthread_create(&r->thread, NULL, scan_run, r) == 0 ? 0 : -1;
}


int as_reader_read(void *ctx, void **data) {
    AerospikeReader *r = ctx;

    if (channel_receive(r->chan, data) != 0) {
        return atomic_load(&r->error) ? PIPE_ERR : PIPE_EOF;
    }

    return PIPE_OK;
}

int as_reader_close(void *ctx) {
    return 0;
}