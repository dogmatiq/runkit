package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// addCommandsToQueue adds any commands scheduled by the handler to the queue
// with the given ID.
func addCommandsToQueue(
	ctx context.Context,
	tx *sql.Tx,
	queueID *uuidpb.UUID,
	packer *envelope.EffectPacker,
) error {
	commandEnvelopes, ok := packer.Seal()
	if !ok {
		return nil
	}

	for commandEnvelope := range commandEnvelopes.All() {
		if _, err := tx.ExecContext(
			ctx,
			`SELECT messagequeue.add_command($1, $2, $3, $4, $5)`,
			xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
			xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.UUID(queueID),
			xsql.Envelope(commandEnvelope),
			commandEnvelope.GetBody().GetIdempotencyKey(),
		); err != nil {
			return fmt.Errorf("unable to add command to queue: %w", err)
		}
	}

	return nil
}
