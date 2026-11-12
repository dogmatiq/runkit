// Package messagequeue delivers the commands and deadlines in the message
// queues to their handlers.
//
// Each message is in one queue. A delivery is the job of getting one message to
// its handler, and it can take more than one attempt. In each attempt, the
// [Consumer] begins a database transaction, claims the message by locking the
// message's row, then passes the message to its [Sink], which delivers it to
// one of the application's message handlers. If the sink returns nil, the
// consumer removes the message from its queue and commits the transaction, and
// the delivery is complete.
//
// Otherwise, the consumer rolls back the transaction, which discards
// everything that the sink wrote. If the sink returned [ErrFailed], the
// consumer then postpones the delivery, which means that it delays the next
// attempt.
//
// A delivery is deemed "contended" if another transaction holds the lock on
// the message's row, or if the sink returns [ErrBusy] because it needs a lock
// while something else holds it. The consumer doesn't wait for the message's
// lock. In both cases, the consumer attempts the delivery again after a delay.
//
// If the sink returns any other error, the message is left for another
// attempt.
package messagequeue
