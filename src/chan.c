#include <stdbool.h>
#include <stddef.h>
#include <pthread.h>
#include <stdlib.h>

#include "c_pipe/chan.h"

/**
 * @brief Thread-safe bounded channel (queue) backed by a ring buffer.
 *
 * Provides blocking send/receive semantics similar to Go channels.
 * Supports graceful shutdown via @ref channel_close.
 */
struct Channel {
    // Ring buffer.
    void **buf;

    // Buffers meta: size - capacity of buffer, count - how many entities already in use,
    // head - index for reading, tail - index for writing.
    size_t size;   /**< @brief Total capacity of the ring buffer. */
    size_t count;  /**< @brief Number of items currently in the buffer. */
    size_t head;   /**< @brief Index of the next read position (dequeue side). */
    size_t tail;   /**< @brief Index of the next write position (enqueue side). */

    // To check if channel is closed - will be signal to stop reading.
    bool is_closed; /**< @brief When true, no new items may be sent; receivers drain remaining items. */

    // Mutex to protect the buffer.
    pthread_mutex_t mu; /**< @brief Mutex protecting all fields of this struct. */

    // Conditions for reader and writer.
    pthread_cond_t not_empty; /**< @brief Signalled when the buffer transitions from empty to non-empty. */
    pthread_cond_t not_full;  /**< @brief Signalled when the buffer transitions from full to non-full. */
};


/**
 * @brief Allocates and initialises a new channel.
 *
 * @param[in] size  Capacity of the internal ring buffer. Must be > 0.
 * @return          Pointer to the newly created @ref Channel,
 *                  or @c NULL on invalid @p size or allocation/init failure.
 *
 * @note The caller is responsible for destroying the channel via @ref channel_destroy.
 */
Channel *channel_new(const size_t size) {
    // Validation.
    if (size == 0 || size > SIZE_MAX / sizeof(void *)) return NULL;

    // Create new chan with zero vals.
    Channel *chan = calloc(1, sizeof(Channel));
    if (chan == NULL) return NULL;

    // Init buffer.
    chan->buf = (void **) malloc(sizeof(void *) * size);
    if (chan->buf == NULL) goto cleanup_chan;

    // Set size.
    chan->size = size;

    // Init sync primitives.
    int ec = pthread_mutex_init(&chan->mu, NULL);
    if (ec != 0) goto cleanup_buf;

    ec = pthread_cond_init(&chan->not_empty, NULL);
    if (ec != 0) goto cleanup_mutex;

    ec = pthread_cond_init(&chan->not_full, NULL);
    if (ec != 0) goto cleanup_cond_empty;

    return chan;

// Cleanups depending on where we exit.
cleanup_cond_empty:
    pthread_cond_destroy(&chan->not_empty);
cleanup_mutex:
    pthread_mutex_destroy(&chan->mu);
cleanup_buf:
    free(chan->buf);
cleanup_chan:
    free(chan);
    return NULL;
}

/**
 * @brief Destroys a channel and releases all associated resources.
 *
 * Destroys synchronisation primitives and frees the ring buffer and
 * the channel struct itself. The channel must not be used after this call.
 *
 * @param[in] chan  Channel to destroy. Must not be @c NULL.
 *
 * @warning Calling this function while other threads are blocked inside
 *          @ref channel_send or @ref channel_receive is undefined behaviour.
 *          Close the channel first and let all threads exit.
 */
void channel_destroy(Channel *chan) {
    if (chan == NULL) return;

    pthread_mutex_destroy(&chan->mu);
    pthread_cond_destroy(&chan->not_empty);
    pthread_cond_destroy(&chan->not_full);

    free(chan->buf);
    free(chan);
}

/**
 * @brief Closes the channel, signalling all blocked senders and receivers.
 *
 * After this call:
 * - @ref channel_send returns @c -1 immediately.
 * - @ref channel_receive drains any remaining buffered items, then returns @c -1.
 *
 * @param[in] chan  Channel to close. Must not be @c NULL.
 */
void channel_close(Channel *chan) {
    // Lock mutex.
    pthread_mutex_lock(&chan->mu);

    chan->is_closed = true;

    // Tell all sleeping cond that they can exit.
    pthread_cond_broadcast(&chan->not_empty);
    pthread_cond_broadcast(&chan->not_full);

    // Unlock mutex.
    pthread_mutex_unlock(&chan->mu);
}

/**
 * @brief Sends an item into the channel, blocking if the buffer is full.
 *
 * Blocks the calling thread until either:
 * - space becomes available and the item is enqueued, or
 * - the channel is closed.
 *
 * @param[in] chan  Target channel. Must not be @c NULL.
 * @param[in] data  Pointer to the item to enqueue. Ownership semantics
 *                  are defined by the caller.
 * @return  @c 0  on success.
 * @return  @c -1 if the channel is closed.
 */
int channel_send(Channel *chan, void *data) {
    // Lock mutex.
    pthread_mutex_lock(&chan->mu);

    // Wait while buffer is full and channel is open.
    while (chan->count == chan->size && !chan->is_closed) {
        pthread_cond_wait(&chan->not_full, &chan->mu);
    }

    // Check if we can send anything.
    if (chan->is_closed) {
        pthread_mutex_unlock(&chan->mu);
        return -1;
    }

    // Add message to buffer.
    chan->buf[chan->tail] = data;
    chan->tail = (chan->tail + 1) % chan->size;
    chan->count++;

    // Signal that we put smth.
    pthread_cond_signal(&chan->not_empty);

    // Unlock mutex.
    pthread_mutex_unlock(&chan->mu);

    // Ok.
    return 0;
}

/**
 * @brief Receives an item from the channel, blocking if the buffer is empty.
 *
 * Blocks the calling thread until either:
 * - an item is available and written to @p data, or
 * - the channel is closed **and** the buffer is drained.
 *
 * @param[in]  chan  Source channel. Must not be @c NULL.
 * @param[out] data  Address where the dequeued pointer will be stored.
 *                   Must not be @c NULL.
 * @return  @c 0  on success; @p *data is valid.
 * @return  @c -1 if the channel is closed and no items remain;
 *          @p *data is left unmodified.
 */
int channel_receive(Channel *chan, void **data) {
    // Lock mutex.
    pthread_mutex_lock(&chan->mu);

    // Wait for messages.
    while (chan->count == 0 && !chan->is_closed) {
        pthread_cond_wait(&chan->not_empty, &chan->mu);
    }

    // Check if we can read anything.
    if (chan->count == 0) {
        pthread_mutex_unlock(&chan->mu);
        return -1;
    }

    // Read data from head.
    *data = chan->buf[chan->head];
    chan->head = (chan->head + 1) % chan->size;
    chan->count--;

    // Signal that we read smth.
    pthread_cond_signal(&chan->not_full);

    // Unlock mutex.
    pthread_mutex_unlock(&chan->mu);

    return 0;
}