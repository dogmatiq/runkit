// Package messagequeue delivers the commands and deadlines in the message
// queues to their handlers.
//
// Each message is in one queue. A delivery is the job of getting one message to
// its handler, and it can take more than one attempt. Each attempt is one
// database transaction. In it, the [Consumer] claims the message by locking the
// message's row, then passes the message to its [Handler]. If the handler
// succeeds, the message is removed from its queue, and the delivery is
// complete. If the handler returns [ErrFailed], the consumer postpones the
// delivery, which means that it delays the next attempt.
//
// A delivery is deemed "contended" if another transaction holds the lock on
// the message's row, or if the handler needs a row and another transaction
// holds that row's lock. The consumer doesn't wait for the message's lock, and
// the handler returns [ErrBusy] instead of waiting for a lock. In both cases,
// the consumer attempts the delivery again after a delay.
//
// If the handler returns any other error, the transaction is rolled back, and
// the message is left for another attempt.
package messagequeue
