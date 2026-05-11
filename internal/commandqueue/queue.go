package commandqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

const (
	// backoffBase is the base interval of the exponential backoff applied
	// to a Nack'd command before the next retry.
	backoffBase = 5 * time.Second

	// backoffCap is the maximum interval between retries.
	backoffCap = 5 * time.Minute
)

// Enqueue enqueues a command for handling, optionally guarded by an
// idempotency key.
func Enqueue(
	ctx context.Context,
	tx *sql.Tx,
	env *envelopepb.Envelope,
) error {
	if ik := env.GetBody().GetIdempotencyKey(); ik != "" {
		res, err := tx.ExecContext(
			ctx,
			`INSERT INTO commandqueue.idempotency_keys (
				idempotency_key
			) VALUES ($1)
			ON CONFLICT (idempotency_key) DO NOTHING`,
			ik,
		)
		if err != nil {
			return fmt.Errorf(
				"unable to claim idempotency key %q: %w",
				ik,
				err,
			)
		}

		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf(
				"unable to determine if idempotency key %q was claimed: %w",
				ik,
				err,
			)
		}

		if rowsAffected == 0 {
			return nil
		}
	}

	envData, err := env.MarshalBinary()
	if err != nil {
		return fmt.Errorf("unable to marshal command envelope: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO commandqueue.commands (
			message_id,
			correlation_id,
			message_type_id,
			envelope
		) VALUES ($1, $2, $3, $4)`,
		env.GetBody().GetMessageId().AsString(),
		env.GetHeader().GetCorrelationId().AsString(),
		env.GetBody().GetMessage().GetTypeId().AsString(),
		envData,
	); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	return nil
}

// Ack confirms successful handling of the command with the given message ID.
func Ack(ctx context.Context, tx *sql.Tx, messageID string) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM commandqueue.commands
		WHERE message_id = $1`,
		messageID,
	); err != nil {
		return fmt.Errorf("unable to ack command %s: %w", messageID, err)
	}

	return nil
}

// Nack records that the command with the given message ID failed, scheduling
// the command for retry with exponential backoff.
func Nack(ctx context.Context, tx *sql.Tx, messageID string) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE commandqueue.commands SET
			attempt_count = attempt_count + 1,
			next_attempt_at = now() + LEAST(
				pow(2, attempt_count) * $2,
				$3
			) * interval '1 second'
		WHERE message_id = $1`,
		messageID,
		backoffBase.Seconds(),
		backoffCap.Seconds(),
	); err != nil {
		return fmt.Errorf("unable to nack command %s: %w", messageID, err)
	}

	return nil
}

// Reset clears all transient state for the command, resetting it to the state
// it was when first enqueued.
func Reset(ctx context.Context, tx *sql.Tx, messageID string) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE commandqueue.commands SET
			attempt_count = 0,
			next_attempt_at = clock_timestamp()
		WHERE message_id = $1`,
		messageID,
	); err != nil {
		return fmt.Errorf("unable to reset command %s: %w", messageID, err)
	}

	return nil
}
