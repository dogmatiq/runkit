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
	"github.com/dogmatiq/runkit/internal/supervisor"
)

// Consumer delivers the messages in each queue that its node is responsible
// for.
type Consumer struct {
	// DB is the database that stores the message queue.
	DB *sql.DB

	// Node is this engine's node in the cluster.
	Node *cluster.Node

	// Queues is the catalog of the queues that have messages.
	Queues *Catalog

	// MessageTypeIDs is the set of IDs of the message types that the consumer
	// delivers.
	MessageTypeIDs *uuidcollection.Set

	// Attempt delivers the message with the given ID. It's called in its own
	// goroutine once the message is due, and the message isn't offered again
	// until the call returns.
	Attempt func(ctx context.Context, messageID *uuidpb.UUID)

	// EngineLogger is the target for log messages produced by the consumer.
	EngineLogger *slog.Logger

	// messageAdded is notified when a message is added to a queue that the
	// consumer consumes, including by being moved there from another queue.
	messageAdded cosyne.Signal

	// attemptEnded receives the ID of the message of each attempt that ends.
	attemptEnded chan *uuidpb.UUID

	// active is the set of IDs of the messages being attempted.
	active uuidcollection.Set

	// detachQueueSignal holds a function for each queue being consumed.
	// Calling it detaches the queue's signal from messageAdded.
	detachQueueSignal uuidcollection.Map[func()]

	// work tracks each streak of failed discoveries.
	work *supervisor.Work
}

// Run delivers messages until ctx is canceled.
//
// It returns once every attempt has ended. It must be called at most once.
func (c *Consumer) Run(ctx context.Context) error {
	c.work = &supervisor.Work{
		Description:  "consume queue",
		EngineLogger: c.EngineLogger,
	}
	defer c.work.Complete()

	c.attemptEnded = make(chan *uuidpb.UUID)
	defer c.awaitAttempts()

	for {
		queues, queuesChanged := c.Queues.QueueIDs().Load()
		liveNodes, liveNodesChanged := c.Node.LiveNodes().Load()

		responsible := c.reconcile(ctx, queues, liveNodes)
		nextMessageDue := c.dispatchDue(ctx, responsible)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-queuesChanged:
		case <-liveNodesChanged:
		case <-c.messageAdded.Chan():
		case <-nextMessageDue:
		case messageID := <-c.attemptEnded:
			// Discovery leaves out a message while it's being attempted. Once
			// the attempt ends, the consumer discovers again, as the message
			// may still be due, or have been postponed, which sends no signal.
			c.endAttempt(messageID)
		}
	}
}

// reconcile attaches the signal of each of the given queues that this node is
// responsible for, given the live nodes, to c.messageAdded, and detaches the
// signal of every other queue.
//
// It returns the IDs of the queues that this node is responsible for.
func (c *Consumer) reconcile(
	ctx context.Context,
	queues *uuidcollection.Set,
	liveNodes *uuidcollection.Set,
) *uuidcollection.Set {
	responsible := uuidcollection.NewSet()

	for queueID := range queues.All() {
		if c.Node.IsResponsibleFor(queueID, liveNodes) {
			responsible.Add(queueID)
		}
	}

	for queueID, detach := range c.detachQueueSignal.All() {
		if !responsible.Has(queueID) {
			detach()
			c.detachQueueSignal.Delete(queueID)
		}
	}

	for queueID := range responsible.All() {
		if !c.detachQueueSignal.Has(queueID) {
			detach := c.Node.AttachSignal(ctx, queueSignal(queueID), &c.messageAdded)
			c.detachQueueSignal.Set(queueID, detach)
		}
	}

	return responsible
}

// dispatchDue starts an attempt for each due message in the given queues.
//
// It returns a channel that fires when the next message becomes due, or nil if
// there are no more messages.
func (c *Consumer) dispatchDue(
	ctx context.Context,
	queues *uuidcollection.Set,
) (nextMessageDue <-chan time.Time) {
	for {
		next, ok := c.discoverNext(ctx, queues)

		// There are no more messages, so the consumer waits for a signal.
		if !ok {
			return nil
		}

		// The message isn't due yet, so the consumer waits until it is.
		if !next.IsDue() {
			delay := time.Until(next.NextAttemptAt)
			return time.After(delay)
		}

		c.startAttempt(ctx, next.MessageID)
	}
}

// discoverNext discovers the next message in the given queues, leaving out the
// messages being attempted. It returns false if there are no such messages.
//
// If discovery fails, it's retried after a delay, until it succeeds or ctx is
// canceled. It returns false if ctx is canceled.
func (c *Consumer) discoverNext(
	ctx context.Context,
	queues *uuidcollection.Set,
) (candidate, bool) {
	for {
		next, ok, err := discover(
			ctx,
			c.DB,
			queues,
			c.MessageTypeIDs,
			&c.active,
		)

		// The consumer is stopping, so the error isn't a failure of discovery.
		if ctx.Err() != nil {
			return candidate{}, false
		}

		// Discovery succeeded, which ends any streak of failures.
		if err == nil {
			c.work.Succeeded(ctx)
			return next, ok
		}

		delay := c.work.Failed(ctx, err)

		select {
		case <-ctx.Done():
			return candidate{}, false
		case <-time.After(delay):
		}
	}
}

// startAttempt starts an attempt to deliver the message with the given ID.
//
// When the attempt ends, it sends the message's ID to c.attemptEnded.
func (c *Consumer) startAttempt(ctx context.Context, messageID *uuidpb.UUID) {
	c.active.Add(messageID)

	go func() {
		c.Attempt(ctx, messageID)
		c.attemptEnded <- messageID
	}()
}

// endAttempt records that the attempt to deliver the message with the given ID
// has ended.
func (c *Consumer) endAttempt(messageID *uuidpb.UUID) {
	c.active.Delete(messageID)
}

// awaitAttempts waits until every attempt in progress has ended.
func (c *Consumer) awaitAttempts() {
	for !c.active.IsEmpty() {
		messageID := <-c.attemptEnded
		c.endAttempt(messageID)
	}
}

// queueSignal returns the cluster-wide signal that is notified when a message
// is added to the queue with the given ID, including by being moved there from
// another queue.
func queueSignal(queueID *uuidpb.UUID) string {
	return "messagequeue." + queueID.AsString()
}
