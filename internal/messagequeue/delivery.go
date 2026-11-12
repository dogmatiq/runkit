package messagequeue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// failureBackoffBase is the delay, before jitter, after a delivery's first
	// failure.
	//
	// It doubles with each failure after that, up to failureBackoffMax.
	failureBackoffBase = 100 * time.Millisecond

	// failureBackoffMax is the longest delay after a failure.
	failureBackoffMax = 5 * time.Minute
)

// attemptDelivery makes an attempt to deliver the message with the given ID.
//
// It doesn't deliver the message unless the message is in one of the given
// queues.
//
// If the attempt doesn't complete the delivery, it rolls back the attempt's
// transaction, which discards everything that the sink wrote. It then postpones
// the delivery if the attempt failed because of a problem with the message or
// its handler, or because the sink was busy.
//
// It returns true if the attempt completed the delivery, and the error that
// ended the attempt, if there was one. When another transaction holds the
// message's lock, [xsql.IsLockNotAvailableError] returns true for that error.
func (c *Consumer) attemptDelivery(
	ctx context.Context,
	queues *uuidcollection.Set,
	messageID *uuidpb.UUID,
) (bool, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	d, envelopeData, ok, err := c.claimMessage(ctx, tx, queues, messageID)
	if err != nil {
		return false, err
	}

	// The message was removed, postponed or moved to another queue since
	// [Consumer.findNextMessages] returned it.
	if !ok {
		return false, nil
	}

	// [Consumer.claimMessage] claims only messages of the types in c.Sinks, so
	// there's always a sink for the message's type.
	sink, _ := c.Sinks.Get(d.MessageTypeID)

	engineLogger := c.EngineLogger.With(
		slog.Group(
			"delivery",
			xslog.UUID("message_id", d.MessageID),
		),
		sink.LogAttr(),
	)

	// The application's records are held until the attempt completes the
	// delivery and its transaction commits, so an attempt that fails or rolls
	// back writes none.
	appLogger, flush := xslog.Buffered(
		c.AppLogger.With(sink.LogAttr()),
	)

	env := &envelopepb.Envelope{}
	err = env.UnmarshalBinary(envelopeData)
	if err == nil {
		err = env.Validate()
	}

	if err != nil {
		// The message can't be delivered without a valid envelope, so its
		// delivery is postponed as a failure.
		engineLogger.DebugContext(
			ctx,
			"message envelope is invalid",
			xslog.Error(err),
		)

		err = ErrFailed
	} else {
		handledMessage := xslog.Envelope("handled_message", env)
		engineLogger = engineLogger.With(handledMessage)

		err = c.deliverMessage(
			ctx,
			tx,
			d,
			env,
			sink,
			engineLogger,
			appLogger.With(handledMessage),
		)
	}

	if err != nil {
		// tx holds the message's lock, which the postponement must acquire, so
		// it's rolled back first.
		if err := tx.Rollback(); err != nil {
			return false, fmt.Errorf("unable to roll back transaction: %w", err)
		}

		switch {
		case errors.Is(err, ErrFailed):
			return false, c.postponeAfterFailure(ctx, d, engineLogger)
		case errors.Is(err, ErrBusy):
			return false, c.postponeAfterContention(ctx, d, engineLogger)
		default:
			return false, err
		}
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("unable to commit transaction: %w", err)
	}

	engineLogger.DebugContext(ctx, "message delivery completed")
	flush(ctx)

	return true, nil
}

// claimMessage locks the message with the given ID within tx.
//
// It claims the message only if it's due, the consumer has a sink for its
// type, and it's in one of the given queues. It returns the message's delivery
// and the binary representation of its envelope, or false if there's no such
// message.
//
// It doesn't wait if another transaction holds the message's lock. Instead, it
// returns an error for which [xsql.IsLockNotAvailableError] returns true.
func (c *Consumer) claimMessage(
	ctx context.Context,
	tx *sql.Tx,
	queues *uuidcollection.Set,
	messageID *uuidpb.UUID,
) (Delivery, []byte, bool, error) {
	// The time compared with next_attempt_at comes from Go's clock, not the
	// database's, because [dogma.ProcessDeadlineScope.Now] reads Go's clock
	// too. A deadline is claimed only after Go's clock reaches its scheduled
	// time, so Now never returns an earlier time, whatever the offset between
	// the two clocks.
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			message_id,
			message_type_id,
			queue_id,
			envelope,
			failures,
			failing_since
		FROM messagequeue.messages
		WHERE message_id = $1
			AND queue_id = ANY($2::uuid[])
			AND message_type_id = ANY($3::uuid[])
			AND next_attempt_at <= $4
		FOR UPDATE NOWAIT`,
		xsql.UUID(messageID),
		xsql.UUIDSeq(queues.All()),
		xsql.UUIDSeq(c.messageTypeIDs.All()),
		time.Now(),
	)

	d := Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
		QueueID:       &uuidpb.UUID{},
	}

	var (
		envelopeData []byte
		failingSince sql.NullTime
	)

	if err := row.Scan(
		xsql.UUID(d.MessageID),
		xsql.UUID(d.MessageTypeID),
		xsql.UUID(d.QueueID),
		&envelopeData,
		&d.Failures,
		&failingSince,
	); err != nil {
		// There's no such message.
		if errors.Is(err, sql.ErrNoRows) {
			return Delivery{}, nil, false, nil
		}

		return Delivery{}, nil, false, fmt.Errorf("unable to claim message: %w", err)
	}

	d.FailingSince = failingSince.Time

	return d, envelopeData, true, nil
}

// deliverMessage passes the message in the given envelope to the given sink
// within tx.
//
// It removes the message from its queue after the sink returns nil. Otherwise,
// it returns the sink's error.
//
// engineLogger is the target for the engine's records about the delivery, both
// the consumer's and the sink's. appLogger is passed to the sink.
func (c *Consumer) deliverMessage(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	env *envelopepb.Envelope,
	sink Sink,
	engineLogger *slog.Logger,
	appLogger *slog.Logger,
) error {
	engineLogger.DebugContext(ctx, "message delivery attempt started")

	if err := sink.Deliver(ctx, tx, d, env, engineLogger, appLogger); err != nil {
		return err
	}

	// The sink may already have removed the message within tx, such as when
	// the handler ends the process instance while handling a deadline, which
	// removes the instance's deadlines.
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM messagequeue.messages
		WHERE message_id = $1`,
		xsql.UUID(d.MessageID),
	); err != nil {
		return fmt.Errorf("unable to remove message: %w", err)
	}

	return nil
}

// postponeAfterFailure postpones the given delivery after a failed attempt,
// and increments its failure count.
//
// It's called after the attempt's transaction has been rolled back. It writes
// nothing if another transaction holds the message's lock, the message has
// been removed, or the message's failure count has changed since the attempt
// claimed it.
func (c *Consumer) postponeAfterFailure(
	ctx context.Context,
	d Delivery,
	engineLogger *slog.Logger,
) error {
	// The delay reaches its maximum long before the shift would overflow.
	doublings := min(d.Failures, 32)
	backoff := min(failureBackoffBase<<doublings, failureBackoffMax)
	delay := supervisor.Jitter(backoff)

	// The statement doesn't wait for the message's lock, and it compares only
	// the failure count to tell whether the message has changed. Together,
	// these have two rare consequences. In both, the message stays in its
	// queue and is attempted again:
	//
	//   - A transaction that isn't a delivery attempt can hold the lock, such
	//     as one that ends a process instance and deletes its deadlines. If
	//     that transaction leaves the message in its queue, this failure isn't
	//     counted, and the next attempt isn't delayed by a failure backoff.
	//
	//   - While ownership of a queue's workload moves between nodes, both
	//     nodes can attempt the message. If the other node postponed it after
	//     contention, which doesn't change its failure count, this statement
	//     replaces that postponement.
	err := xsql.ExecOne(
		ctx,
		c.DB,
		`UPDATE messagequeue.messages SET
			failures = failures + 1,
			failing_since = COALESCE(failing_since, clock_timestamp()),
			next_attempt_at = clock_timestamp() + $3
		WHERE message_id = (
			SELECT message_id
			FROM messagequeue.messages
			WHERE message_id = $1
				AND failures = $2
			FOR UPDATE SKIP LOCKED
		)`,
		xsql.UUID(d.MessageID),
		d.Failures,
		delay,
	)

	// Another transaction holds the message's lock, or the message has been
	// removed or its failure count has changed since the attempt claimed it.
	if errors.Is(err, sql.ErrNoRows) {
		engineLogger.DebugContext(
			ctx,
			"message delivery not postponed because the message is locked or has changed",
			slog.String("reason", "failure"),
		)
		return nil
	}

	if err != nil {
		return fmt.Errorf("unable to postpone message: %w", err)
	}

	engineLogger.DebugContext(
		ctx,
		"message delivery postponed",
		slog.Duration("delay", delay),
		slog.String("reason", "failure"),
		slog.Uint64("failures", d.Failures+1),
	)

	return nil
}

// postponeAfterContention postpones the given delivery after its sink
// returned [ErrBusy], without changing its failure count.
//
// It's called after the attempt's transaction has been rolled back. It writes
// nothing in the same cases as [Consumer.postponeAfterFailure].
func (c *Consumer) postponeAfterContention(
	ctx context.Context,
	d Delivery,
	engineLogger *slog.Logger,
) error {
	// As in [Consumer.postponeAfterFailure], a transaction that isn't a
	// delivery attempt can hold the message's lock. If that transaction leaves
	// the message in its queue, the message can be attempted again before the
	// contention delay has passed.
	err := xsql.ExecOne(
		ctx,
		c.DB,
		`UPDATE messagequeue.messages SET
			next_attempt_at = clock_timestamp() + $3
		WHERE message_id = (
			SELECT message_id
			FROM messagequeue.messages
			WHERE message_id = $1
				AND failures = $2
			FOR UPDATE SKIP LOCKED
		)`,
		xsql.UUID(d.MessageID),
		d.Failures,
		c.ContentionDelay,
	)

	// Another transaction holds the message's lock, or the message has been
	// removed or its failure count has changed since the attempt claimed it.
	if errors.Is(err, sql.ErrNoRows) {
		engineLogger.DebugContext(
			ctx,
			"message delivery not postponed because the message is locked or has changed",
			slog.String("reason", "contention"),
		)
		return nil
	}

	if err != nil {
		return fmt.Errorf("unable to postpone message: %w", err)
	}

	engineLogger.DebugContext(
		ctx,
		"message delivery postponed",
		slog.Duration("delay", c.ContentionDelay),
		slog.String("reason", "contention"),
	)

	return nil
}
