package commandqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

const (
	// baseRetryInterval is the base interval of the exponential
	// backoff applied to a failed command before the next retry.
	baseRetryInterval = 5 * time.Second

	// maximumRetryInterval is the maximum interval between retries.
	maximumRetryInterval = 5 * time.Minute
)

// Enqueue adds a command to the queue.
func Enqueue(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
	targetHandlerKey *uuidpb.UUID,
	targetAggregateInstanceID *string,
) error {
	if idempotencyKey := commandEnvelope.GetBody().GetIdempotencyKey(); idempotencyKey != "" {
		res, err := tx.ExecContext(
			ctx,
			`INSERT INTO command_idempotency_keys (
				idempotency_key
			) VALUES ($1)
			ON CONFLICT (idempotency_key)
			DO NOTHING`,
			idempotencyKey,
		)
		if err != nil {
			return fmt.Errorf(
				"unable to insert idempotency key %q: %w",
				idempotencyKey,
				err,
			)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"unable to determine if idempotency key %q was inserted: %w",
				idempotencyKey,
				err,
			)
		}

		if rowsAffected == 0 {
			return nil
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO pending_commands (
			message_id,
			correlation_id,
			handler_key,
			aggregate_instance_id,
			envelope
		) VALUES ($1, $2, $3, $4, $5)`,
		database.MarshalUUID(commandEnvelope.GetBody().GetMessageId()),
		database.MarshalUUID(commandEnvelope.GetHeader().GetCorrelationId()),
		database.MarshalUUID(targetHandlerKey),
		targetAggregateInstanceID,
		database.MarshalEnvelope(commandEnvelope),
	); err != nil {
		return fmt.Errorf("unable to insert pending command: %w", err)
	}

	return nil
}

// Dequeue removes the command with the given message ID from the queue.
func Dequeue(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM pending_commands
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to delete pending command: %w", err)
	}

	return nil
}

// Backoff records that the command with the given message ID failed, scheduling
// the command for retry with exponential backoff.
func Backoff(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE pending_commands SET
			attempt_count = attempt_count + 1,
			next_attempt_at = clock_timestamp() + LEAST(
				pow(2, attempt_count) * $2,
				$3
			) * interval '1 second'
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
		baseRetryInterval.Seconds(),
		maximumRetryInterval.Seconds(),
	); err != nil {
		return fmt.Errorf("unable to mark command %s as failed: %w", messageID, err)
	}

	return nil
}
