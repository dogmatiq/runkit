package commandqueue

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// Add adds a command to the queue.
func Add(
	ctx context.Context,
	x xsql.Executor,
	commandEnvelope *envelopepb.Envelope,
) error {
	if _, err := x.ExecContext(
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
	x xsql.Executor,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		x,
		`DELETE FROM dogma.pending_commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to remove command from queue: %w", err)
	}

	return nil
}
