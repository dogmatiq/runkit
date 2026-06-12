package commandqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

const (
	// deferBase is the initial delay applied the first time a command is
	// deferred. Each subsequent deferral doubles the delay up to deferCap.
	deferBase = 10 * time.Millisecond

	// deferCap is the maximum delay that can be applied when deferring a
	// command.
	deferCap = 300 * time.Second
)

// Add adds a command to the queue.
//
// If the envelope has an idempotency key that has already been used, the
// command is silently discarded, messageID is the ID of the message that
// originally claimed the idempotency key, and ok is false.
//
// Otherwise, the command is added to the queue, messageID is the ID of the
// newly added command, and ok is true.
func Add(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) (messageID *uuidpb.UUID, ok bool, err error) {
	idempotencyKey := commandEnvelope.GetBody().GetIdempotencyKey()
	messageID = commandEnvelope.GetBody().GetMessageId()
	messageTypeID := commandEnvelope.GetBody().GetMessage().GetTypeId()

	if idempotencyKey == "" {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO commandqueue.commands (
				message_id,
				correlation_id,
				message_type_id,
				envelope
			) VALUES ($1, $2, $3, $4)`,
			xsql.UUID(messageID),
			xsql.UUID(commandEnvelope.GetHeader().GetCorrelationId()),
			xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(commandEnvelope),
		); err != nil {
			return nil, false, fmt.Errorf("unable to add command to queue: %w", err)
		}

		return messageID, true, nil
	}

	row := tx.QueryRowContext(
		ctx,
		`WITH idempotency_key AS (
			INSERT INTO commandqueue.idempotency_keys (
				idempotency_key,
				message_id
			)
			VALUES ($1, $2)
			ON CONFLICT (idempotency_key)
			DO UPDATE SET
				idempotency_key = EXCLUDED.idempotency_key
			RETURNING message_id
		), pending_command AS (
			INSERT INTO commandqueue.commands (
				message_id,
				correlation_id,
				message_type_id,
				envelope
			)
			SELECT $2, $3, $4, $5
			FROM idempotency_key
			WHERE idempotency_key.message_id = $2
		)
		SELECT
			message_id,
			message_id = $2 AS enqueued
		FROM idempotency_key`,
		idempotencyKey,
		xsql.UUID(messageID),
		xsql.UUID(commandEnvelope.GetHeader().GetCorrelationId()),
		xsql.UUID(messageTypeID),
		xsql.Envelope(commandEnvelope),
	)

	// Ensure we're not scanning on top of the message ID in the envelope.
	messageID = &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(messageID),
		&ok,
	); err != nil {
		return nil, false, fmt.Errorf("unable to add command to queue: %w", err)
	}

	return messageID, ok, nil
}

// Remove removes a command from the queue.
func Remove(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`SELECT commandqueue.remove($1)`,
		xsql.UUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to remove command from queue: %w", err)
	}

	return nil
}

// DeferDueToContention defers a command by a fixed delay without incrementing
// the failure count. It is used when a command cannot be processed due to
// transient contention rather than an error.
func DeferDueToContention(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE commandqueue.commands SET
			execute_at = clock_timestamp() + $2 * interval '1 millisecond'
		WHERE message_id = $1`,
		xsql.UUID(messageID),
		deferBase.Milliseconds(),
	); err != nil {
		return fmt.Errorf("unable to defer queued command due to contention: %w", err)
	}

	return nil
}

// DeferDueToFailure defers execution of a command by an exponentially
// increasing amount of time based on the number of failures.
func DeferDueToFailure(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE commandqueue.commands SET
			failures = failures + 1,
			execute_at = clock_timestamp() + LEAST(
				pow(2, failures) * $2,
				$3
			) * interval '1 millisecond'
		WHERE message_id = $1`,
		xsql.UUID(messageID),
		deferBase.Milliseconds(),
		deferCap.Milliseconds(),
	); err != nil {
		return fmt.Errorf("unable to defer queued command due to failure: %w", err)
	}

	return nil
}
