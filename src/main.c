#define _POSIX_C_SOURCE 200809L

#include <getopt.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <aerospike/aerospike.h>
#include <aerospike/as_config.h>
#include <aerospike/as_error.h>
#include <aerospike/as_partition_filter.h>

#include "c_pipe/as_reader.h"
#include "c_pipe/as_writer.h"
#include "c_pipe/pipe.h"

/** Aerospike fixes the partition count at 4096 per namespace. */
#define AS_PARTITIONS         4096
#define DEFAULT_PARALLEL      10
#define DEFAULT_BATCH_SIZE    128
#define DEFAULT_MAX_RETRIES   3
#define MAX_PARALLEL          AS_PARTITIONS  /* one reader per partition is the upper bound */

/* ------------------------------------------------------------------------- *
 * Configuration parsed from CLI.
 * ------------------------------------------------------------------------- */
typedef struct {
    const char *src_host;
    int         src_port;
    const char *dst_host;
    int         dst_port;
    const char *ns;
    const char *set;
    const char *user;
    const char *password;
    uint32_t    parallel;
    uint32_t    batch_size;
    uint32_t    max_retries;
} AppConfig;

/* ------------------------------------------------------------------------- *
 * CLI parsing.
 * ------------------------------------------------------------------------- */

static void usage(const char *prog) {
    fprintf(stderr,
        "Usage: %s --source HOST:PORT --target HOST:PORT --namespace NS --set SET\n"
        "          [--parallel N] [--batch-size N] [--max-retries N]\n"
        "          [--user USER] [--password PASS]\n"
        "\n"
        "Migrates all records of NS.SET from source cluster to target cluster.\n"
        "Splits the 4096 partitions across N workers (paired reader/writer).\n",
        prog);
}

/**
 * @brief Splits "host:port" into separate strings.
 *
 * Mutates @p s in place — replaces ':' with '\0'. Returns -1 on malformed input.
 */
static int split_host_port(char *s, const char **host_out, int *port_out) {
    if (s == NULL || *s == '\0') return -1;

    char *colon = strrchr(s, ':');
    if (colon == NULL || colon == s || *(colon + 1) == '\0') return -1;

    *colon = '\0';
    *host_out = s;

    char *end;
    long port = strtol(colon + 1, &end, 10);
    if (*end != '\0' || port <= 0 || port > 65535) return -1;
    *port_out = (int)port;

    return 0;
}

static int parse_uint(const char *s, uint32_t *out) {
    if (s == NULL) return -1;
    char *end;
    unsigned long v = strtoul(s, &end, 10);
    if (*end != '\0' || v == 0 || v > UINT32_MAX) return -1;
    *out = (uint32_t)v;
    return 0;
}

static int parse_args(int argc, char **argv, AppConfig *cfg) {
    static struct option opts[] = {
        { "source",      required_argument, NULL, 's' },
        { "target",      required_argument, NULL, 't' },
        { "namespace",   required_argument, NULL, 'n' },
        { "set",         required_argument, NULL, 'S' },
        { "parallel",    required_argument, NULL, 'p' },
        { "batch-size",  required_argument, NULL, 'b' },
        { "max-retries", required_argument, NULL, 'r' },
        { "user",        required_argument, NULL, 'u' },
        { "password",    required_argument, NULL, 'P' },
        { "help",        no_argument,       NULL, 'h' },
        { NULL, 0, NULL, 0 }
    };

    cfg->parallel    = DEFAULT_PARALLEL;
    cfg->batch_size  = DEFAULT_BATCH_SIZE;
    cfg->max_retries = DEFAULT_MAX_RETRIES;

    char *src_arg = NULL;
    char *dst_arg = NULL;

    int c;
    while ((c = getopt_long(argc, argv, "s:t:n:S:p:b:r:u:P:h", opts, NULL)) != -1) {
        switch (c) {
            case 's': src_arg      = optarg; break;
            case 't': dst_arg      = optarg; break;
            case 'n': cfg->ns      = optarg; break;
            case 'S': cfg->set     = optarg; break;
            case 'u': cfg->user    = optarg; break;
            case 'P': cfg->password = optarg; break;
            case 'p':
                if (parse_uint(optarg, &cfg->parallel) != 0
                    || cfg->parallel > MAX_PARALLEL) {
                    fprintf(stderr, "Invalid --parallel: %s (must be 1..%d)\n",
                            optarg, MAX_PARALLEL);
                    return -1;
                }
                break;
            case 'b':
                if (parse_uint(optarg, &cfg->batch_size) != 0) {
                    fprintf(stderr, "Invalid --batch-size: %s\n", optarg);
                    return -1;
                }
                break;
            case 'r':
                if (parse_uint(optarg, &cfg->max_retries) != 0) {
                    fprintf(stderr, "Invalid --max-retries: %s\n", optarg);
                    return -1;
                }
                break;
            case 'h':
                usage(argv[0]);
                return 1;
            default:
                usage(argv[0]);
                return -1;
        }
    }

    if (src_arg == NULL || dst_arg == NULL || cfg->ns == NULL || cfg->set == NULL) {
        fprintf(stderr, "Missing required arg.\n\n");
        usage(argv[0]);
        return -1;
    }

    if (split_host_port(src_arg, &cfg->src_host, &cfg->src_port) != 0) {
        fprintf(stderr, "Invalid --source format. Expected host:port.\n");
        return -1;
    }
    if (split_host_port(dst_arg, &cfg->dst_host, &cfg->dst_port) != 0) {
        fprintf(stderr, "Invalid --target format. Expected host:port.\n");
        return -1;
    }

    // user/password — both or neither.
    if ((cfg->user == NULL) != (cfg->password == NULL)) {
        fprintf(stderr, "--user and --password must be specified together.\n");
        return -1;
    }

    return 0;
}

/* ------------------------------------------------------------------------- *
 * Aerospike connection helpers.
 * ------------------------------------------------------------------------- */

/**
 * @brief Connects to an AS cluster. Returns 0 on success.
 *
 * @p as is initialised in place — caller must call @c aerospike_destroy on it
 * once @c aerospike_close has succeeded.
 */
static int as_connect(aerospike *as, const char *host, int port,
                      const char *user, const char *password) {
    as_config cfg;
    as_config_init(&cfg);

    if (!as_config_add_hosts(&cfg, host, (uint16_t)port)) {
        fprintf(stderr, "as_config_add_hosts failed for %s:%d\n", host, port);
        return -1;
    }

    if (user != NULL) {
        if (!as_config_set_user(&cfg, user, password)) {
            fprintf(stderr, "as_config_set_user failed\n");
            return -1;
        }
    }

    aerospike_init(as, &cfg);

    as_error err;
    if (aerospike_connect(as, &err) != AEROSPIKE_OK) {
        fprintf(stderr, "Connect to %s:%d failed: %d %s\n",
                host, port, err.code, err.message);
        aerospike_destroy(as);
        return -1;
    }

    return 0;
}

static void as_close_destroy(aerospike *as) {
    as_error err;
    aerospike_close(as, &err);
    aerospike_destroy(as);
}

/* ------------------------------------------------------------------------- *
 * Worker setup — paired readers/writers per partition range.
 * ------------------------------------------------------------------------- */

/**
 * @brief Splits 4096 partitions across @p parallel workers.
 *
 * Worker i gets partitions [i * base + min(i, rem), ...) where rem records
 * are spread one-per-worker over the first `rem` workers.
 */
static void partition_range(uint32_t parallel, uint32_t i,
                             uint32_t *start, uint32_t *count) {
    uint32_t base = AS_PARTITIONS / parallel;
    uint32_t rem  = AS_PARTITIONS % parallel;

    if (i < rem) {
        *count = base + 1;
        *start = i * (*count);
    } else {
        *count = base;
        *start = rem * (base + 1) + (i - rem) * base;
    }
}

/**
 * @brief Worker pair — owns the AerospikeReader and AerospikeWriter for one
 *        partition range, plus the corresponding pipe Reader/Writer adapters.
 */
typedef struct {
    AerospikeReader *reader;
    AerospikeWriter *writer;
} Worker;

/**
 * @brief Allocates and starts all workers. On any failure, cleans up what
 *        was created and returns -1.
 *
 * On success: @p workers, @p readers, @p writers are populated and reader
 * background threads are running.
 */
static int workers_setup(const AppConfig *cfg,
                         aerospike *src, aerospike *dst,
                         Worker *workers, Reader *readers, Writer *writers) {
    uint32_t i;
    for (i = 0; i < cfg->parallel; i++) {
        uint32_t start, count;
        partition_range(cfg->parallel, i, &start, &count);

        // Reader.
        AerospikeReaderConfig rcfg = { .ns = cfg->ns, .set = cfg->set };
        as_partition_filter_set_range(&rcfg.pf, (uint16_t)start, (uint16_t)count);

        AerospikeReader *r = as_reader_new(src, rcfg);
        if (r == NULL) {
            fprintf(stderr, "as_reader_new failed (worker %u)\n", i);
            goto fail;
        }
        workers[i].reader = r;

        // Writer.
        AerospikeWriterConfig wcfg = {
            .ns          = cfg->ns,
            .set         = cfg->set,
            .batch_size  = cfg->batch_size,
            .max_retries = cfg->max_retries,
        };
        AerospikeWriter *w = as_writer_new(dst, wcfg);
        if (w == NULL) {
            fprintf(stderr, "as_writer_new failed (worker %u)\n", i);
            goto fail;
        }
        workers[i].writer = w;

        // Adapter structs for pipe.
        readers[i] = (Reader){
            .read         = as_reader_read,
            .close        = as_reader_close,
            .destroy_item = as_reader_destroy_item,
            .ctx          = r,
        };
        writers[i] = (Writer){
            .write        = as_writer_write,
            .close        = as_writer_close,
            .destroy_item = as_writer_destroy_item,
            .ctx          = w,
        };

        // Start the background scan now — pipe_run expects the reader
        // to already be producing.
        if (as_reader_start(r) != 0) {
            fprintf(stderr, "as_reader_start failed (worker %u)\n", i);
            i++;  // count this worker so cleanup destroys it
            goto fail;
        }
    }

    return 0;

fail:
    for (uint32_t j = 0; j < i; j++) {
        if (workers[j].reader) {
            // Ask the scan thread to exit, then destroy joins it.
            as_reader_close(workers[j].reader);
            as_reader_destroy(workers[j].reader);
        }
        if (workers[j].writer) {
            as_writer_destroy(workers[j].writer);
        }
    }
    return -1;
}

/**
 * @brief Sums per-worker stats and prints the migration summary.
 *
 * @return  number of permanently failed records across all workers.
 */
static uint64_t print_summary(const Worker *workers, uint32_t parallel) {
    uint64_t total_inserted = 0;
    uint64_t total_failed   = 0;
    bool     any_error      = false;
    as_error first_error    = { .code = 0 };

    for (uint32_t i = 0; i < parallel; i++) {
        total_inserted += as_writer_inserted(workers[i].writer);
        total_failed   += as_writer_failed(workers[i].writer);

        if (!any_error) {
            as_error e = { .code = 0 };
            as_writer_last_error(workers[i].writer, &e);
            if (e.code != 0) {
                first_error = e;
                any_error = true;
            }
        }
    }

    fprintf(stdout,
        "----------------------------------------\n"
        "Migration summary:\n"
        "  inserted: %llu\n"
        "  failed:   %llu\n"
        "  workers:  %u\n",
        (unsigned long long)total_inserted,
        (unsigned long long)total_failed,
        parallel);

    if (any_error) {
        fprintf(stdout, "  first error: %d %s\n",
                first_error.code, first_error.message);
    }
    fprintf(stdout, "----------------------------------------\n");

    return total_failed;
}

/* ------------------------------------------------------------------------- *
 * main.
 * ------------------------------------------------------------------------- */

int main(int argc, char **argv) {
    AppConfig cfg = {0};

    int parse_rc = parse_args(argc, argv, &cfg);
    if (parse_rc == 1) return 0;     /* --help */
    if (parse_rc != 0) return 2;

    fprintf(stdout,
        "c_pipe: %s.%s   %s:%d -> %s:%d   parallel=%u batch=%u retries=%u\n",
        cfg.ns, cfg.set,
        cfg.src_host, cfg.src_port,
        cfg.dst_host, cfg.dst_port,
        cfg.parallel, cfg.batch_size, cfg.max_retries);

    /* Connect both clusters. */
    aerospike src, dst;
    bool src_connected = false, dst_connected = false;
    int rc = 2;

    if (as_connect(&src, cfg.src_host, cfg.src_port, cfg.user, cfg.password) != 0) {
        goto done;
    }
    src_connected = true;

    if (as_connect(&dst, cfg.dst_host, cfg.dst_port, cfg.user, cfg.password) != 0) {
        goto done;
    }
    dst_connected = true;

    /* Allocate workers + adapter arrays. */
    Worker *workers = calloc(cfg.parallel, sizeof(Worker));
    Reader *readers = calloc(cfg.parallel, sizeof(Reader));
    Writer *writers = calloc(cfg.parallel, sizeof(Writer));
    if (workers == NULL || readers == NULL || writers == NULL) {
        fprintf(stderr, "OOM\n");
        free(workers); free(readers); free(writers);
        goto done;
    }

    /* Build readers/writers, start scan threads. */
    if (workers_setup(&cfg, &src, &dst, workers, readers, writers) != 0) {
        free(workers); free(readers); free(writers);
        goto done;
    }

    /* Wire pipe and run. */
    Pipe *pipe = pipe_new(readers, cfg.parallel, writers, cfg.parallel);
    if (pipe == NULL) {
        fprintf(stderr, "pipe_new failed\n");
        rc = 2;
    } else {
        int prc = pipe_run(pipe);
        pipe_destroy(pipe);

        if (prc != 0) {
            fprintf(stderr, "pipe_run failed (thread create/join error)\n");
            rc = 2;
        } else {
            uint64_t failed = print_summary(workers, cfg.parallel);
            rc = (failed == 0) ? 0 : 1;
        }
    }

    /* Destroy workers — joins any still-alive scan threads. */
    for (uint32_t i = 0; i < cfg.parallel; i++) {
        if (workers[i].reader) as_reader_destroy(workers[i].reader);
        if (workers[i].writer) as_writer_destroy(workers[i].writer);
    }
    free(workers);
    free(readers);
    free(writers);

done:
    if (src_connected) as_close_destroy(&src);
    if (dst_connected) as_close_destroy(&dst);
    return rc;
}