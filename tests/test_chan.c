#include <stdlib.h>
#include <pthread.h>

#include "unity.h"
#include "c_pipe/chan.h"

// Run before each test.
void setUp(void) {}

// After each.
void tearDown(void) {}

// Utils for test.

#define CHECKSUM 45
#define PARALLEL 5
#define DEFAULT_CHAN_SIZE_SINGLE 10
#define DEFAULT_CHAN_SIZE_MULTI 5
#define DEFAULT_MESSAGE_COUNT 10

typedef struct {
    Channel* chan;
    int sent, received;
    // Summ all messages to validate content.
    int checksum;
    pthread_mutex_t mu;
} arg_test;

arg_test *arg_test_new(Channel *chan) {
    // Init all with 0.
    arg_test *a = calloc(1, sizeof(arg_test));
    a->chan = chan;
    pthread_mutex_init(&a->mu, NULL);

    return a;
}

void arg_test_destroy(arg_test *a) {
    channel_destroy(a->chan);
    pthread_mutex_destroy(&a->mu);

    free(a);
}

// producer sends messages to the chan, for tests.
// To run it in parallel, it should accept void,
// to satisfy pthread_create "interface"
static void *producer(void *arg) {
    // cast type
    arg_test *a = arg;

    for (int i = 0; i < DEFAULT_MESSAGE_COUNT; i++) {
        // Allocate mem for test val.
        int *val = malloc(sizeof(int));
        // set "message"
        *val = i;
        // Send message
        if (channel_send(a->chan, val) != 0) {
            // If failed to send, free memory.
            free(val);

            return NULL;
        }
        // Increase counter.
        pthread_mutex_lock(&a->mu);
        a->sent++;
        pthread_mutex_unlock(&a->mu);
    }

    return NULL;
}

// consumer receives messages from the chan, for tests.
// To run it in parallel, it should accept void,
// to satisfy pthread_create "interface"
static void *consumer(void *arg) {
    // cast type
    arg_test *a = arg;
    // Prepare var for receiving results.
    void *result = NULL;

    while (channel_receive(a->chan, &result) == 0) {
        // Increase counters.
        pthread_mutex_lock(&a->mu);
        a->checksum += *(int *)result;
        a->received++;
        pthread_mutex_unlock(&a->mu);

        free(result);
    }

    return NULL;
}

// Tests.

void test_channel_new_not_null(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_SINGLE);
    TEST_ASSERT_NOT_NULL(chan);
    channel_destroy(chan);
}

void test_channel_new_null(void) {
    Channel *chan = channel_new(0);
    TEST_ASSERT_NULL(chan);
}

// Run single thread.
void test_channel_single_thread(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_SINGLE);
    TEST_ASSERT_NOT_NULL(chan);

    arg_test *a = arg_test_new(chan);

    producer(a);
    channel_close(a->chan);
    consumer(a);

    TEST_ASSERT_EQUAL_INT(a->sent, a->received);
    TEST_ASSERT_EQUAL_INT(a->checksum, CHECKSUM);

    arg_test_destroy(a);
}

// Run multithread pipe.
void test_channel_multi_thread(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_MULTI);
    TEST_ASSERT_NOT_NULL(chan);

    pthread_t prod_threads[PARALLEL];
    pthread_t cons_threads[PARALLEL];

    arg_test *a = arg_test_new(chan);

    // Start consumers.
    for (int i = 0; i < PARALLEL; i++) {
        pthread_create(&cons_threads[i], NULL, consumer, a);
    }

    // Start producers.
    for (int i = 0; i < PARALLEL; i++) {
        pthread_create(&prod_threads[i], NULL, producer, a);
    }

    // Wait for producers.
    for (int i = 0; i < PARALLEL; i++) {
        pthread_join(prod_threads[i], NULL);
    }

    // Close the chan.
    channel_close(a->chan);

    // Wait for consumers.
    for (int i = 0; i < PARALLEL; i++) {
        pthread_join(cons_threads[i], NULL);
    }

    TEST_ASSERT_EQUAL_INT(a->sent, a->received);
    TEST_ASSERT_EQUAL_INT(CHECKSUM * PARALLEL, a->checksum);

    arg_test_destroy(a);
}

// Sending to closed channel.
void test_channel_send_to_closed(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_SINGLE);
    TEST_ASSERT_NOT_NULL(chan);

    channel_close(chan);

    int *val = malloc(sizeof(int));
    *val = 1;

    int result = channel_send(chan, val);
    TEST_ASSERT_EQUAL_INT(-1, result);

    free(val);
    channel_destroy(chan);
}

// Receive from closed chan.
void test_channel_receive_from_closed(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_SINGLE);
    TEST_ASSERT_NOT_NULL(chan);

    channel_close(chan);

    void *result = NULL;
    int code = channel_receive(chan, &result);
    TEST_ASSERT_EQUAL_INT(-1, code);
    TEST_ASSERT_NULL(result);

    channel_destroy(chan);
}

// Read rest messages from closed chan.
void test_channel_drains_after_close(void) {
    Channel *chan = channel_new(DEFAULT_CHAN_SIZE_SINGLE);
    TEST_ASSERT_NOT_NULL(chan);

    arg_test *a = arg_test_new(chan);

    producer(a);
    channel_close(a->chan);
    consumer(a);

    TEST_ASSERT_EQUAL_INT(DEFAULT_MESSAGE_COUNT, a->sent);
    TEST_ASSERT_EQUAL_INT(DEFAULT_MESSAGE_COUNT, a->received);
    TEST_ASSERT_EQUAL_INT(CHECKSUM, a->checksum);

    arg_test_destroy(a);
}

// Run the tests.
int main(void) {
    UNITY_BEGIN();

    RUN_TEST(test_channel_new_not_null);
    RUN_TEST(test_channel_new_null);
    RUN_TEST(test_channel_single_thread);
    RUN_TEST(test_channel_multi_thread);
    RUN_TEST(test_channel_send_to_closed);
    RUN_TEST(test_channel_receive_from_closed);
    RUN_TEST(test_channel_drains_after_close);

    return UNITY_END();
}