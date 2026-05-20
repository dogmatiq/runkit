package commandqueue

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

const (
	// backoffBase is the base interval of the exponential backoff applied
	// to a Nack'd command before the next retry.
	backoffBase = 5 * time.Second

	// backoffCap is the maximum interval between retries.
	backoffCap = 5 * time.Minute
)

// Enqueue enqueues a command for handling.
func Enqueue(
	ctx context.Context,
	tx *sql.Tx,
	envelope *envelopepb.Envelope,
) error {
	if idempotencyKey := envelope.GetBody().GetIdempotencyKey(); idempotencyKey != "" {
		if claimed, err := claimIdempotencyKey(ctx, tx, idempotencyKey); !claimed {
			return err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO command_queue (
			message_id,
			correlation_id,
			message_type_id,
			envelope
		) VALUES ($1, $2, $3, $4)`,
		database.MarshalUUID(envelope.GetBody().GetMessageId()),
		database.MarshalUUID(envelope.GetHeader().GetCorrelationId()),
		database.MarshalUUID(envelope.GetBody().GetMessage().GetTypeId()),
		database.MarshalEnvelope(envelope),
	); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	return nil
}

// claimIdempotencyKey attempts to claim the given idempotency key.
//
// It returns true if the key was successfully claimed by this call, or false if
// the key had already been claimed. claimed is always false if err is not nil.
func claimIdempotencyKey(
	ctx context.Context,
	tx *sql.Tx,
	idempotencyKey string,
) (claimed bool, err error) {
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO command_idempotency_keys (
			idempotency_key
		) VALUES ($1)
		ON CONFLICT (idempotency_key) DO NOTHING`,
		idempotencyKey,
	)
	if err != nil {
		return false, fmt.Errorf(
			"unable to claim idempotency key %q: %w",
			idempotencyKey,
			err,
		)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf(
			"unable to determine if idempotency key %q was claimed: %w",
			idempotencyKey,
			err,
		)
	}

	return rowsAffected == 1, nil
}

// Ack confirms successful handling of the command with the given message ID.
//
// The caller must already hold a FOR UPDATE lock on the command's row in tx.
func Ack(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM command_queue
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to ack command %s: %w", messageID, err)
	}

	return nil
}

// Nack records that the command with the given message ID failed, scheduling
// the command for retry with exponential backoff.
//
// The caller must already hold a FOR UPDATE lock on the command's row in tx.
func Nack(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE command_queue SET
			attempt_count = attempt_count + 1,
			next_attempt_at = now() + LEAST(
				pow(2, attempt_count) * $2,
				$3
			) * interval '1 second'
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
		backoffBase.Seconds(),
		backoffCap.Seconds(),
	); err != nil {
		return fmt.Errorf("unable to nack command %s: %w", messageID, err)
	}

	return nil
}

// NextAttemptByCorrelationID returns the earliest time that any command with
// the given correlation ID is scheduled for its next attempt.
func NextAttemptByCorrelationID(
	ctx context.Context,
	q database.Querier,
	correlationID *uuidpb.UUID,
) (time.Time, bool, error) {
	row := q.QueryRowContext(
		ctx,
		`SELECT next_attempt_at
		FROM command_queue
		WHERE correlation_id = $1
		ORDER BY next_attempt_at
		LIMIT 1`,
		database.MarshalUUID(correlationID),
	)

	var nextAttempt time.Time
	if err := row.Scan(&nextAttempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}

		return time.Time{}, false, fmt.Errorf(
			"unable to get next attempt for correlation ID %s: %w",
			correlationID,
			err,
		)
	}

	return nextAttempt, true, nil
}
