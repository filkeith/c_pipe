#include <stdbool.h>
#include <stdlib.h>
#include <pthread.h>
#include <stdatomic.h>

#include <aerospike/aerospike_scan.h>
#include <aerospike/as_record.h>
#include <aerospike/as_scan.h>
#include <aerospike/as_val.h>

#include "c_pipe/as_reader.h"
#include "c_pipe/chan.h"
#include "c_pipe/pipe.h"

#define AS_READER_QUEUE_SIZE 64

/**
 * @brief Background-scan reader. Bridges AS push callbacks to a pull-based
 *        Reader through a bounded channel.
 *
 * @c last_error is written from the scan thread without locking; readers must
 * wait until the thread has been joined (via @ref as_reader_destroy) before
 * inspecting it.
 */
struct AerospikeReader {
    // Client, inited outside. Not owned.
    aerospike *as;

    // Push-to-pull bridge.
    Channel *chan;

    // Background scan thread.
    pthread_t thread;

    // Stats.
    atomic_uint_fast64_t scanned;

    // State flags.
    atomic_int error;       // set on scan/parse failure
    atomic_int cancelled;   // set on external close()
    atomic_int started;     // set after pthread_create succeeds

    // Scan handle and config (config carries ns/set/partition filter).
    AerospikeReaderConfig cfg;
    as_scan scan;

    // Last error, populated by scan_run on failure.
    // Read only after as_reader_destroy joins the scan thread.
    as_error last_error;
};

AerospikeReader *as_reader_new(aerospike *as, AerospikeReaderConfig cfg) {
    if (as == NULL || cfg.ns == NULL || cfg.set == NULL) return NULL;

    // Init reader with 0's.
    AerospikeReader *r = calloc(1, sizeof(AerospikeReader));
    if (r == NULL) return NULL;

    r->as  = as;
    r->cfg = cfg;
    atomic_init(&r->error,     0);
    atomic_init(&r->cancelled, 0);
    atomic_init(&r->started,   0);
    atomic_init(&r->scanned,   0);

    // Init scan from cfg.
    as_scan_init(&r->scan, r->cfg.ns, r->cfg.set);

    // Bridge channel.
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

/**
 * @brief AS scan callback — invoked by the client for every record.
 *
 * Builds an @c as_record from the @c as_val, reserves the val (refcount),
 * pushes the record onto the bridge channel. Returns @c true to continue,
 * @c false to abort the scan.
 */
static bool as_scan_callback(const as_val *val, void *arg) {
    AerospikeReader *r = arg;

    // val == NULL marks end-of-scan; channel will be closed by scan_run.
    if (!val) return true;

    as_record *rec = as_record_fromval(val);
    if (rec == NULL) {
        // Parse failure — record this as a real error so read() returns PIPE_ERR.
        atomic_store(&r->error, 1);
        return false;
    }
    // Reserve only after parse success — keeps refcount balanced.
    as_val_reserve((as_val *)val);

    if (channel_send(r->chan, rec) != 0) {
        // Channel was closed externally → cancellation.
        as_record_destroy(rec);
        atomic_store(&r->cancelled, 1);
        return false;
    }

    atomic_fetch_add(&r->scanned, 1);
    return true;
}

/**
 * @brief Background thread entry point.
 *
 * Drives @c aerospike_scan_partitions to completion or failure. Always closes
 * the channel on exit so any reader blocked on @ref channel_receive is released.
 */
static void *scan_run(void *arg) {
    AerospikeReader *r = arg;

    as_error err;
    as_status status = aerospike_scan_partitions(
        r->as, &err, NULL, &r->scan, &r->cfg.pf, as_scan_callback, r);

    // Record only transport-level failures here. Per-record parse errors are
    // already flagged from the callback.
    if (status != AEROSPIKE_OK
        && !atomic_load(&r->cancelled)
        && !atomic_load(&r->error)) {
        r->last_error = err;
        atomic_store(&r->error, 1);
    }

    // Always close — idempotent, unblocks any waiting reader.
    channel_close(r->chan);

    return NULL;
}

int as_reader_start(AerospikeReader *r) {
    if (r == NULL) return -1;
    if (pthread_create(&r->thread, NULL, scan_run, r) != 0) return -1;
    atomic_store(&r->started, 1);
    return 0;
}

int as_reader_read(void *ctx, void **data) {
    AerospikeReader *r = ctx;

    if (channel_receive(r->chan, data) != 0) {
        // Channel drained AND closed. Distinguish error from clean EOF.
        // Note: cancelled is set by external close() too — that path is
        // not an error from the pipeline's POV, so we only check `error`.
        return atomic_load(&r->error) ? PIPE_ERR : PIPE_EOF;
    }
    return PIPE_OK;
}

int as_reader_close(void *ctx) {
    AerospikeReader *r = ctx;

    // Idempotent. Also called on normal EOF, where the scan thread has
    // already exited — these stores are then harmless no-ops.
    atomic_store(&r->cancelled, 1);
    channel_close(r->chan);

    return 0;
}

void as_reader_destroy_item(void *data) {
    as_record_destroy((as_record *)data);
}