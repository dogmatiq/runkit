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

// attemptDelivery makes an attempt to deliver the message with the given ID,
// within one transaction.
//
// It doesn't deliver the message unless the message is in one of the given
// queues. It returns true if it delivered the message, and the error that ended
// the attempt, if there was one. When another transaction holds the message's
// lock, [xsql.IsLockNotAvailableError] returns true for that error.
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

	logger := c.EngineLogger.With(
		xslog.UUID("message_id", d.MessageID),
		slog.Uint64("failures", d.Failures),
	)

	delivered, err := c.deliverMessage(ctx, tx, d, envelopeData, logger)
	if err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("unable to commit transaction: %w", err)
	}

	if delivered {
		logger.DebugContext(ctx, "message delivered")
	}

	return delivered, nil
}

// claimMessage locks the message with the given ID within tx.
//
// It claims the message only if it's due, the consumer has a handler for its
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

// deliverMessage passes the message to its handler within tx.
//
// It removes the message from its queue after the handler succeeds, and
// postpones the delivery instead if the envelope is invalid, or the handler
// returns [ErrFailed] or [ErrBusy]. It returns true if it removed the message.
// envelopeData is the binary representation of the message's envelope.
func (c *Consumer) deliverMessage(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	envelopeData []byte,
	logger *slog.Logger,
) (bool, error) {
	env := &envelopepb.Envelope{}
	err := env.UnmarshalBinary(envelopeData)
	if err == nil {
		err = env.Validate()
	}

	// The message can't be delivered without a valid envelope, so its delivery
	// is postponed as a failure.
	if err != nil {
		logger.DebugContext(
			ctx,
			"message envelope is invalid",
			xslog.Error(err),
		)

		return false, c.postponeAfterFailure(ctx, tx, d, logger)
	}

	logger = logger.With(
		xslog.Envelope("handled_message", env),
	)

	logger.DebugContext(ctx, "message delivery attempt started")

	// [Consumer.claimMessage] claims only the messages whose type has a handler, so
	// there's always a handler.
	h, _ := c.Handlers.Get(d.MessageTypeID)
	err = h.Deliver(ctx, tx, d, env, logger)

	switch {
	case err == nil:
		if _, err := tx.ExecContext(
			ctx,
			`SELECT messagequeue.remove($1)`,
			xsql.UUID(d.MessageID),
		); err != nil {
			return false, fmt.Errorf("unable to remove message: %w", err)
		}
		return true, nil
	case errors.Is(err, ErrFailed):
		return false, c.postponeAfterFailure(ctx, tx, d, logger)
	case errors.Is(err, ErrBusy):
		return false, c.postponeAfterContention(ctx, tx, d, logger)
	default:
		return false, err
	}
}

// postponeAfterFailure postpones the given delivery after a failed attempt,
// and increments its failure count.
func (c *Consumer) postponeAfterFailure(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	logger *slog.Logger,
) error {
	// The delay reaches its maximum long before the shift would overflow.
	doublings := min(d.Failures, 32)
	backoff := min(failureBackoffBase<<doublings, failureBackoffMax)
	delay := supervisor.Jitter(backoff)

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE messagequeue.messages SET
			failures = failures + 1,
			failing_since = COALESCE(failing_since, clock_timestamp()),
			next_attempt_at = clock_timestamp() + $2
		WHERE message_id = $1`,
		xsql.UUID(d.MessageID),
		delay,
	); err != nil {
		return fmt.Errorf("unable to postpone message: %w", err)
	}

	logger.DebugContext(
		ctx,
		"message delivery postponed",
		slog.Duration("delay", delay),
		slog.String("reason", "failure"),
	)

	return nil
}

// postponeAfterContention postpones the given delivery after its handler
// returned [ErrBusy], without changing its failure count.
func (c *Consumer) postponeAfterContention(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	logger *slog.Logger,
) error {
	// The contention is on a row that the handler needs. The transaction
	// already holds the lock on the message's row, so this update doesn't wait
	// for another transaction.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE messagequeue.messages SET
			next_attempt_at = clock_timestamp() + $2
		WHERE message_id = $1`,
		xsql.UUID(d.MessageID),
		c.ContentionDelay,
	); err != nil {
		return fmt.Errorf("unable to postpone message: %w", err)
	}

	logger.DebugContext(
		ctx,
		"message delivery postponed",
		slog.Duration("delay", c.ContentionDelay),
		slog.String("reason", "contention"),
	)

	return nil
}
