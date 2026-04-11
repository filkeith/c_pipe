#ifndef C_PIPE_CHAN_H
#define C_PIPE_CHAN_H

#include <stddef.h>

/**
 * @file chan.h
 * @brief Thread-safe bounded channel backed by a ring buffer.
 *
 * Provides blocking send/receive semantics similar to Go channels.
 * All operations are safe to call from multiple threads concurrently.
 *
 * Typical usage:
 * @code
 *   Channel *ch = channel_new(64);
 *
 *   // producer thread
 *   channel_send(ch, my_data);
 *
 *   // consumer thread
 *   void *data = NULL;
 *   channel_receive(ch, &data);
 *
 *   channel_close(ch);
 *   channel_destroy(ch);
 * @endcode
 */

/**
 * @brief Opaque channel handle.
 *
 * Internal layout is private to @c chan.c. Always allocate via @ref channel_new
 * and free via @ref channel_destroy.
 */
typedef struct Channel Channel;

/**
 * @brief Allocates and initialises a new channel.
 *
 * @param[in] size  Capacity of the internal ring buffer. Must be > 0.
 * @return          Pointer to the new @ref Channel, or @c NULL on invalid
 *                  @p size or allocation failure.
 *
 * @note The caller is responsible for destroying the channel via @ref channel_destroy.
 */
Channel *channel_new(size_t size);

/**
 * @brief Closes the channel, unblocking all waiting senders and receivers.
 *
 * After this call:
 * - @ref channel_send returns @c -1 immediately.
 * - @ref channel_receive drains any remaining buffered items, then returns @c -1.
 *
 * @param[in] chan  Channel to close. Must not be @c NULL.
 */
void channel_close(Channel *chan);

/**
 * @brief Destroys the channel and releases all associated resources.
 *
 * @param[in] chan  Channel to destroy. Must not be @c NULL.
 *
 * @warning Must only be called after all threads have stopped using the channel.
 *          Close the channel first and join all threads before destroying.
 */
void channel_destroy(Channel *chan);

/**
 * @brief Sends an item into the channel, blocking if the buffer is full.
 *
 * Blocks until either space becomes available or the channel is closed.
 *
 * @param[in] chan  Target channel. Must not be @c NULL.
 * @param[in] data  Pointer to enqueue. Ownership semantics are defined by the caller.
 * @return  @c  0  on success.
 * @return  @c -1  if the channel is closed.
 */
int channel_send(Channel *chan, void *data);

/**
 * @brief Receives an item from the channel, blocking if the buffer is empty.
 *
 * Blocks until either an item is available or the channel is closed and drained.
 *
 * @param[in]  chan  Source channel. Must not be @c NULL.
 * @param[out] data  Set to the dequeued pointer on success. Must not be @c NULL.
 * @return  @c  0  on success; @p *data is valid.
 * @return  @c -1  if the channel is closed and no items remain; @p *data is unchanged.
 */
int channel_receive(Channel *chan, void **data);

#endif /* C_PIPE_CHAN_H */