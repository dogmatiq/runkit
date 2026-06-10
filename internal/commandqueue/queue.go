package commandqueue

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
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

// Defer defers execution of a command by an exponentially increasing amount of
// time based on the number of times it has been deferred.
func Defer(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE dogma.pending_commands SET
			attempt_count = attempt_count + 1,
			attempt_at = clock_timestamp() + LEAST(
				pow(2, attempt_count) * 0.5,
				300
			) * interval '1 second'
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to defer queued command: %w", err)
	}

	return nil
}
