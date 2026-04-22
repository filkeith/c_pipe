#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include "c_pipe/as_writer.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_INSERT_QUEUE_SIZE 64

struct AerospikeWriter {
    // Client that is inited outside.
    aerospike *as;
    // Last error, for clear logging. Not protected, should be read after evrything is closed.
    as_error last_error;



    // Internal chan to process inserts to as.
    Channel *chan;

    // Background thread to process inserts to as.
    pthread_t thread;

    // Errors processing.
    atomic_int error;
    atomic_int cancelled;

    atomic_int started;

    // Stats.
    atomic_uint_fast64_t inserted;

    //TODO: insert config?
};

AerospikeWriter *as_writer_new(aerospike *as, const char *ns, const char *set) {
    if (as == NULL || ns == NULL || set == NULL) return NULL;

    AerospikeWriter *w = calloc(1, sizeof(AerospikeWriter));
    if (w == NULL) return NULL;

    w->as = as;
    atomic_init(&w->error, 0);
    atomic_init(&w->cancelled, 0);
    atomic_init(&w->started, 0);
    atomic_init(&w->inserted, 0);

    // Create channel for results.
    Channel *ch = channel_new(AS_INSERT_QUEUE_SIZE);
    if (ch == NULL) goto cleanup;

    w->chan = ch;

    return w;

cleanup:
    free(w);
    return NULL;
}

void as_writer_destroy(AerospikeWriter *w) {
    if (w == NULL) return;

    // Wait for scan thread to finish.
    if (atomic_load(&w->started)) {
        pthread_join(w->thread, NULL);
    }

    channel_destroy(w->chan);
    free(w);
}

static void *insert_run(void *arg) {
    AerospikeWriter *w = (AerospikeWriter *)arg;
    return NULL;
}

int as_writer_start(AerospikeWriter *w) {
    if (w == NULL) return -1;
    if (pthread_create(&w->thread, NULL, insert_run, w) != 0) return -1;
    atomic_store(&w->started, 1);
}

int as_writer_write(void *ctx, void *data) {
    AerospikeWriter *w = ctx;

    return PIPE_OK;
}

int as_writer_close(void *ctx) {
    AerospikeWriter *w = ctx;
    atomic_store(&w->cancelled, 1);
    channel_close(w->chan);

    return PIPE_OK;
}