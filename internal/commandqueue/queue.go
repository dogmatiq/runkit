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
func Add(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO dogma.pending_commands (
			message_id,
			message_type_id,
			envelope
		) VALUES ($1, $2, $3)`,
		xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
		xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
		xsql.Envelope(commandEnvelope),
	); err != nil {
		return fmt.Errorf("unable to add command to queue: %w", err)
	}

	return nil
}

// Remove removes a command from the queue.
func Remove(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`DELETE FROM dogma.pending_commands
		WHERE message_id = $1`,
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
		`UPDATE dogma.pending_commands SET
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
		`UPDATE dogma.pending_commands SET
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
