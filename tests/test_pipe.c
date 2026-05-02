#include <stdlib.h>
#include <stdatomic.h>

#include "unity.h"
#include "c_pipe/pipe.h"

#define MESSAGE_COUNT 10
#define PARALLEL      3

// Global test state.
static atomic_int g_read_index;
static atomic_int g_read_limit;
static atomic_int g_total_sent;
static atomic_int g_total_received;
static atomic_int g_checksum_received;
static atomic_int g_destroyed;   // increments when destroy_item is called

void setUp(void) {
    atomic_store(&g_read_index,        0);
    atomic_store(&g_read_limit,        MESSAGE_COUNT);
    atomic_store(&g_total_sent,        0);
    atomic_store(&g_total_received,    0);
    atomic_store(&g_checksum_received, 0);
    atomic_store(&g_destroyed,         0);
}

void tearDown(void) {}

// Reader for tests.
static int test_read(void *ctx, void **data) {
    int idx = atomic_fetch_add(&g_read_index, 1);
    if (idx >= atomic_load(&g_read_limit)) {
        return PIPE_EOF;
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

// Writer for tests.
static int test_write(void *ctx, void **data) {
    int val = *(int *)*data;
    free(*data);

    atomic_fetch_add(&g_total_received,    1);
    atomic_fetch_add(&g_checksum_received, val);

    return PIPE_OK;
}

static int test_close_noop(void *ctx) { return 0; }

// destroy_item for int* payload — tracks calls so we can verify nothing leaks.
static void test_destroy_int(void *data) {
    atomic_fetch_add(&g_destroyed, 1);
    free(data);
}

// Calc checksum.
static int expected_checksum(int n) {
    return n * (n - 1) / 2;
}

// Helper: build a fully populated Reader/Writer.
static Reader make_reader(void) {
    return (Reader){ test_read, test_close_noop, test_destroy_int, NULL };
}
static Writer make_writer(void) {
    return (Writer){ test_write, test_close_noop, test_destroy_int, NULL };
}

// pipe_new returns NULL when readers_count == 0.
void test_pipe_new_null_on_zero_readers(void) {
    Writer w = make_writer();
    Pipe *p = pipe_new(NULL, 0, &w, 1);
    TEST_ASSERT_NULL(p);
}

// pipe_new returns NULL when writers_count == 0.
void test_pipe_new_null_on_zero_writers(void) {
    Reader r = make_reader();
    Pipe *p = pipe_new(&r, 1, NULL, 0);
    TEST_ASSERT_NULL(p);
}

// pipe_new rejects readers without a destroy_item callback.
void test_pipe_new_null_on_missing_reader_destroy(void) {
    Reader r = { test_read, test_close_noop, NULL, NULL };
    Writer w = make_writer();
    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NULL(p);
}

// pipe_new rejects writers without a destroy_item callback.
void test_pipe_new_null_on_missing_writer_destroy(void) {
    Reader r = make_reader();
    Writer w = { test_write, test_close_noop, NULL, NULL };
    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NULL(p);
}

// pipe_new returns a valid pointer for correct arguments.
void test_pipe_new_not_null(void) {
    Reader r = make_reader();
    Writer w = make_writer();

    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NOT_NULL(p);
    pipe_destroy(p);
}

// One reader to one writer.
void test_pipe_run_single(void) {
    Reader r = make_reader();
    Writer w = make_writer();

    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));

    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(MESSAGE_COUNT),
                          atomic_load(&g_checksum_received));
    // Happy path — destroy_item is for failure paths, must not be called.
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&g_destroyed));

    pipe_destroy(p);
}

// PARALLEL readers to PARALLEL writers.
void test_pipe_run_multi_paired(void) {
    int total = PARALLEL * MESSAGE_COUNT;
    atomic_store(&g_read_limit, total);

    Reader readers[PARALLEL];
    Writer writers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) {
        readers[i] = make_reader();
        writers[i] = make_writer();
    }

    Pipe *p = pipe_new(readers, PARALLEL, writers, PARALLEL);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(total),
                          atomic_load(&g_checksum_received));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&g_destroyed));

    pipe_destroy(p);
}

// PARALLEL readers to one writer.
void test_pipe_run_fan_in(void) {
    int total = PARALLEL * MESSAGE_COUNT;
    atomic_store(&g_read_limit, total);

    Reader readers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) readers[i] = make_reader();
    Writer w = make_writer();

    Pipe *p = pipe_new(readers, PARALLEL, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(total, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(total),
                          atomic_load(&g_checksum_received));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&g_destroyed));

    pipe_destroy(p);
}

// One reader to PARALLEL writers.
void test_pipe_run_fan_out(void) {
    Reader r = make_reader();
    Writer writers[PARALLEL];
    for (int i = 0; i < PARALLEL; i++) writers[i] = make_writer();

    Pipe *p = pipe_new(&r, 1, writers, PARALLEL);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_sent));
    TEST_ASSERT_EQUAL_INT(MESSAGE_COUNT, atomic_load(&g_total_received));
    TEST_ASSERT_EQUAL_INT(expected_checksum(MESSAGE_COUNT),
                          atomic_load(&g_checksum_received));
    TEST_ASSERT_EQUAL_INT(0, atomic_load(&g_destroyed));

    pipe_destroy(p);
}

// ---------- Failure-path test: writer fails on every record. ----------

static int failing_write(void *ctx, void **data) {
    (void)ctx;
    (void)data;
    return PIPE_ERR;   // pipe must call destroy_item on the in-flight item
}

// On writer error: cancellation propagates, in-flight items are freed via
// destroy_item, and pipe_run returns 0 (thread creation/join did not fail).
void test_pipe_run_writer_error_destroys_inflight(void) {
    Reader r = make_reader();
    Writer w = { failing_write, test_close_noop, test_destroy_int, NULL };

    Pipe *p = pipe_new(&r, 1, &w, 1);
    TEST_ASSERT_NOT_NULL(p);

    TEST_ASSERT_EQUAL_INT(0, pipe_run(p));

    // At least the first item that hit failing_write must have been destroyed.
    // Plus possibly any in-flight items the reader had queued before noticing
    // the cancellation. Either way, no leaks: every produced item is either
    // received by the writer (which here doesn't free anything before failing)
    // or destroyed via destroy_item.
    TEST_ASSERT_TRUE(atomic_load(&g_destroyed) >= 1);

    pipe_destroy(p);
}

// Run the tests.
int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_pipe_new_null_on_zero_readers);
    RUN_TEST(test_pipe_new_null_on_zero_writers);
    RUN_TEST(test_pipe_new_null_on_missing_reader_destroy);
    RUN_TEST(test_pipe_new_null_on_missing_writer_destroy);
    RUN_TEST(test_pipe_new_not_null);
    RUN_TEST(test_pipe_run_single);
    RUN_TEST(test_pipe_run_multi_paired);
    RUN_TEST(test_pipe_run_fan_in);
    RUN_TEST(test_pipe_run_fan_out);
    RUN_TEST(test_pipe_run_writer_error_destroys_inflight);

    return UNITY_END();
}