#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include "c_pipe/as_reader.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_READER_QUEUE_SIZE 256

struct AerospikeReader {
    // Client that is initied outside.
    aerospike *as;

    // Scan config and partition range.
    as_scan scan;
    as_partition_filter pf;

    // Internal chan to process results from as.
    Channel *chan;

    // Background thread to process results from as.
    pthread_t thread;

    // Errors processing.
    int error;
};

AerospikeReader *as_reader_new(aerospike *as, const char *ns, const char *set, as_partition_filter pf) {
    if (as == NULL || ns == NULL || set == NULL) return NULL;

    // Init reader.
    AerospikeReader *r = malloc(sizeof(AerospikeReader));
    if (r == NULL) return NULL;

    r->as = as;
    r->pf = pf;

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