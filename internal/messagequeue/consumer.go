package messagequeue

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Consumer delivers the messages in each queue while its node owns the queue's
// workload.
//
// The queue's ID is the workload's key, as described by
// [cluster.FindWorkloadOwner].
type Consumer struct {
	// DB is the database that stores the message queue.
	DB *sql.DB

	// Node is this engine's node in the cluster.
	Node *cluster.Node

	// Queues is the catalog of the queues that have messages.
	Queues *Catalog

	// Sinks maps the ID of each message type to the sink for messages of that
	// type.
	//
	// The consumer ignores queued messages of any other type.
	Sinks uuidcollection.Map[Sink]

	// Concurrency is the maximum number of messages that the consumer attempts
	// at once.
	//
	// It must be positive.
	Concurrency int

	// ContentionDelay is how long the consumer waits before it attempts a
	// contended delivery again.
	//
	// A delivery is deemed "contended" if another transaction holds the lock
	// on the message's row, or if the message's sink returns [ErrBusy]. It
	// must be positive.
	ContentionDelay time.Duration

	// EngineFailurePolicy is how the engine responds to failures of the consumer itself, such as
	// failures to find the next messages.
	EngineFailurePolicy failure.Policy

	// DeliveryFailurePolicy is how the engine responds to failures to deliver a message.
	DeliveryFailurePolicy failure.Policy

	// EngineLogger is the target for the engine's records about consuming the
	// queues and delivering their messages.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's records from the sinks'
	// handlers.
	AppLogger *slog.Logger

	// messageAdded is notified when a message is added to a queue that the
	// consumer consumes, including by being moved there from another queue.
	messageAdded cosyne.Signal

	// deliveryAttempted is the channel on which each [deliveryAttempt] is sent
	// after it has been made.
	//
	// Its capacity is large enough to hold every attempt that can be in
	// progress at once, so sending never blocks.
	deliveryAttempted chan deliveryAttempt

	// attemptsInProgress holds the ID of the message for each attempt in
	// progress.
	attemptsInProgress uuidcollection.Set

	// contendedMessages maps the ID of each contended message to the time at
	// which the consumer may attempt it again.
	//
	// A message is deemed "contended" if another transaction holds its lock,
	// so the consumer can't claim it for an attempt.
	contendedMessages uuidcollection.Map[time.Time]

	// backoffUntil is the end of the consumer's backoff.
	//
	// The backoff begins when an attempt ends with an error, such as when the
	// database is unavailable. It doesn't begin when another transaction holds
	// the message's lock. The consumer doesn't start any attempts until the
	// backoff ends.
	backoffUntil time.Time

	// detachQueueSignal holds a function for each queue being consumed.
	//
	// Calling it detaches the queue's signal from messageAdded.
	detachQueueSignal uuidcollection.Map[func()]

	// messageTypeIDs is the set of IDs of the message types in Sinks.
	messageTypeIDs *uuidcollection.Set

	// failures keeps the streak of the consumer's own failures, such as losing
	// its database connection.
	//
	// The consumer records a failure in it if the consumer can't find the next
	// messages, or if an attempt ends with an error, unless another
	// transaction holds the message's lock. failures decides how long the
	// consumer backs off after each failure, and logs the failure.
	failures *failure.Tracker
}

// Run delivers messages until ctx is canceled.
//
// It doesn't return until every attempt in progress has been made. It must be
// called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	if c.Concurrency <= 0 {
		panic("concurrency must be positive")
	}

	if c.ContentionDelay <= 0 {
		panic("contention delay must be positive")
	}

	c.messageTypeIDs = uuidcollection.NewSet()
	for typeID := range c.Sinks.Keys() {
		c.messageTypeIDs.Add(typeID)
	}

	c.failures = &failure.Tracker{
		Description:  "consume queue",
		Policy:       c.EngineFailurePolicy,
		EngineLogger: c.EngineLogger,
	}
	defer c.failures.Complete()

	c.deliveryAttempted = make(chan deliveryAttempt, c.Concurrency)
	defer func() {
		for !c.attemptsInProgress.IsEmpty() {
			a := <-c.deliveryAttempted
			c.recordAttempt(ctx, a)
		}
	}()

	for {
		liveNodes, liveNodesChanged := c.Node.LiveNodes().Load()
		availableQueues, availableQueuesChanged := c.Queues.QueueIDs().Load()

		queues := uuidcollection.NewSet()
		for queueID := range availableQueues.All() {
			if c.Node.OwnsWorkload(queueID, liveNodes) {
				queues.Add(queueID)
			}
		}

		c.attachQueueSignals(ctx, queues)

		// Several attempts may have been made while the loop was busy.
		// Recording them all first means that one call to startAttempts can
		// start a new attempt in place of each of them.
		for len(c.deliveryAttempted) > 0 {
			a := <-c.deliveryAttempted
			c.recordAttempt(ctx, a)
		}

		nextAttemptDue, backlogIsEmpty := c.startAttempts(ctx, queues)

	wait:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-liveNodesChanged:
		case <-availableQueuesChanged:
		case <-c.messageAdded.Chan():
		case <-nextAttemptDue:
		case a := <-c.deliveryAttempted:
			c.recordAttempt(ctx, a)

			if a.Completed && backlogIsEmpty {
				goto wait
			}
		}
	}
}

// attachQueueSignals attaches the signal of each of the given queues to
// c.messageAdded, and detaches the signal of every other queue.
func (c *Consumer) attachQueueSignals(
	ctx context.Context,
	queues *uuidcollection.Set,
) {
	for queueID, detach := range c.detachQueueSignal.All() {
		if !queues.Has(queueID) {
			detach()
			c.detachQueueSignal.Delete(queueID)
		}
	}

	for queueID := range queues.All() {
		if !c.detachQueueSignal.Has(queueID) {
			signal := "messagequeue." + queueID.AsString()
			detach := c.Node.AttachSignal(ctx, signal, &c.messageAdded)
			c.detachQueueSignal.Set(queueID, detach)
		}
	}
}

// startAttempts starts a delivery attempt for each message whose next attempt
// is due, up to the consumer's concurrency limit.
//
// It attempts only the messages in the given queues. Each attempt runs in its
// own goroutine, and is sent to c.deliveryAttempted after it has been made. It
// doesn't wait for any of them.
//
// It returns a channel on which a value is sent when the consumer should call
// startAttempts again, such as when the next message becomes due. It returns
// nil if there's no such time.
//
// It also reports whether the backlog is known to be empty. The backlog is the
// set of messages in the given queues whose next attempt is due but that aren't
// being attempted because there is insufficient concurrency headroom, as
// limited by [Consumer.Concurrency].
//
// It might not start any attempts, such as when the consumer is backing off or
// at its concurrency limit, or when there are no messages that are due.
func (c *Consumer) startAttempts(
	ctx context.Context,
	queues *uuidcollection.Set,
) (nextAttemptDue <-chan time.Time, backlogIsEmpty bool) {
	// An attempt ended with an error, so the consumer doesn't start any
	// attempts until its backoff ends.
	if backoff := time.Until(c.backoffUntil); backoff > 0 {
		return time.After(backoff), false
	}

	headroom := c.Concurrency - c.attemptsInProgress.Len()

	// The consumer is at its concurrency limit, so it waits for an attempt to
	// be made.
	if headroom == 0 {
		return nil, false
	}

	// next is the earliest time at which there may be another message to
	// attempt, or zero if there's no such time.
	var next time.Time

	for messageID, retryAt := range c.contendedMessages.All() {
		if !time.Now().Before(retryAt) {
			c.contendedMessages.Delete(messageID)
		} else if next.IsZero() || retryAt.Before(next) {
			next = retryAt
		}
	}

	messages := c.findNextMessages(ctx, queues, headroom)

	// If the batch has fewer messages than the headroom, it contains every
	// message whose next attempt is due. Otherwise, other messages may be due.
	backlogIsEmpty = len(messages) < headroom

	for _, m := range messages {
		// The message isn't due yet, and the rest of the batch is due later
		// still.
		if !m.IsDue() {
			if next.IsZero() || m.NextAttemptAt.Before(next) {
				next = m.NextAttemptAt
			}
			backlogIsEmpty = true
			break
		}

		c.attemptsInProgress.Add(m.ID)

		a := deliveryAttempt{
			MessageID: m.ID,
		}

		go func() {
			a.Completed, a.Err = c.attemptDelivery(ctx, queues, a.MessageID)
			c.deliveryAttempted <- a
		}()
	}

	// There are no contended messages, and every message in the batch is being
	// attempted. Either the batch had as many messages as the headroom, or
	// there are no more messages.
	if next.IsZero() {
		return nil, backlogIsEmpty
	}

	delay := time.Until(next)
	return time.After(delay), backlogIsEmpty
}

// recordAttempt records that the given attempt has been made.
func (c *Consumer) recordAttempt(ctx context.Context, a deliveryAttempt) {
	c.attemptsInProgress.Delete(a.MessageID)

	switch {
	case ctx.Err() != nil:
		// The consumer is stopping, so an error isn't a failure of the attempt.
	case xsql.IsLockNotAvailableError(a.Err):
		// Another transaction holds the message's lock. Waiting for the
		// contention delay stops the consumer from finding it locked again
		// straight away, without the message counting toward the concurrency
		// limit.
		retryAt := time.Now().Add(c.ContentionDelay)
		c.contendedMessages.Set(a.MessageID, retryAt)
	case a.Err != nil:
		c.backoffUntil = c.failures.WorkFailed(ctx, a.Err)
	default:
		c.failures.WorkSucceeded(ctx)
	}
}

// deliveryAttempt is an attempt to deliver a message.
//
// MessageID is set when the attempt starts, and the other fields are set after
// it has been made.
type deliveryAttempt struct {
	// MessageID is the ID of the message to deliver.
	MessageID *uuidpb.UUID

	// Completed is true if the attempt completed the delivery.
	Completed bool

	// Err is the error that ended the attempt, or nil if there wasn't one.
	//
	// For example, the database may be unavailable, or another transaction may
	// hold the message's lock.
	Err error
}
