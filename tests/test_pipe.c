#include <stdlib.h>
#include <stdatomic.h>

#include "../vendor/Unity/src/unity.h"
#include "../include/c_pipe/pipe.h"

// Utils for test.

#define MESSAGE_COUNT 10
#define PARALLEL      3

// Global test state
static atomic_int g_read_index;
static atomic_int g_read_limit;
static atomic_int g_total_sent;
static atomic_int g_total_received;
static atomic_int g_checksum_received;

void setUp(void) {
    atomic_store(&g_read_index,       0);
    atomic_store(&g_read_limit,       MESSAGE_COUNT);
    atomic_store(&g_total_sent,       0);
    atomic_store(&g_total_received,   0);
    atomic_store(&g_checksum_received, 0);
}

void tearDown(void) {}

// Reader for tests.
static int test_read(void **data) {
    int idx = atomic_fetch_add(&g_read_index, 1);
    if (idx >= atomic_load(&g_read_limit)) {
        return PIPE_EOF; // EOF
    }

    int *val = malloc(sizeof(int));
    if (val == NULL) {
        return PIPE_ERR;
    }

    *val = idx;
    atomic_fetch_add(&g_total_sent, 1);
    *data = val;

    return PIPE_OK;
}

// Writer fo4r tests.
static int test_write(void **data) {
    int val = *(int *)*data;
    free(*data);

    atomic_fetch_add(&g_total_received,    1);
    atomic_fetch_add(&g_checksum_received, val);

    return PIPE_OK;
}

static int test_close_noop(void) { return 0; }


// Calc checksum.
static int expected_checksum(int n) {
    return n * (n - 1) / 2;
}

// pipe_new returns NULL when readers_count == 0.
void test_pipe_new_null_on_zero_readers(void) {
    Writer w = { test_write, test_close_noop };
    Pipe *p = pipe_new(NULL, 0, &w, 1);
    TEST_ASSERT_NULL(p);
}

// pipe_new returns NULL when writers_count == 0.
void test_pipe_new_null_on_zero_writers(void) {
    Reader r = { test_read, test_close_noop };
    Pipe *p = pipe_new(&r, 1, NULL, 0);
    TEST_ASSERT_NULL(p);
}

// pipe_new returns valid pointer for correct arguments.
void test_pipe_new_not_null(void) {
    Reader r = { test_read, test_close_noop };
    Writer w = { test_write, test_close_noop };

    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    pipe_destroy(p);
}

// One reader to one writer.
void test_pipe_run_single(void) {
    Reader r = { test_read, test_close_noop };
    Writer w = { test_write, test_close_noop };

    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));

    // All messages sent and received.
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_received));
    // sum(0..9) == 45
    TEST_ASSERT_EQUAL_INT(expected_checksum(MESSAGE_COUNT), atomic_load(&g_checksum_received));

    pipe_destroy(p);
}

// PARALLEL readers to PARALLEL writers.
void test_pipe_run_multi_paired(void) {
    int total = PARALLEL * MESSAGE_COUNT;
    atomic_store(&g_read_limit, total);

    Reader readers[PARALLEL];
    Writer writers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) {
        readers[i] = (Reader){ test_read, test_close_noop };
        writers[i] = (Writer){ test_write, test_close_noop };
    }

    Pipe *p = pipe_new(readers, PARALLEL, writers, PARALLEL);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(total), atomic_load(&g_checksum_received));

    pipe_destroy(p);
}

// PARALLEL readers to one writer
void test_pipe_run_fan_in(void) {
    int total = PARALLEL * MESSAGE_COUNT;
    atomic_store(&g_read_limit, total);

    Reader readers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) {
        readers[i] = (Reader){ test_read, test_close_noop };
    }
    Writer w = { test_write, test_close_noop };

    Pipe *p = pipe_new(readers, PARALLEL, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(total), atomic_load(&g_checksum_received));

    pipe_destroy(p);
}

// One reader to PARALLEL writers.
void test_pipe_run_fan_out(void) {
    Reader r = { test_read, test_close_noop };

    Writer writers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) {
        writers[i] = (Writer){ test_write, test_close_noop };
    }

    Pipe *p = pipe_new(&r, 1, writers, PARALLEL);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(MESSAGE_COUNT), atomic_load(&g_checksum_received));

    pipe_destroy(p);
}

// Run the tests.
int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_pipe_new_null_on_zero_readers);
    RUN_TEST(test_pipe_new_null_on_zero_writers);
    RUN_TEST(test_pipe_new_not_null);
    RUN_TEST(test_pipe_run_single);
    RUN_TEST(test_pipe_run_multi_paired);
    RUN_TEST(test_pipe_run_fan_in);
    RUN_TEST(test_pipe_run_fan_out);

    return UNITY_END();
}