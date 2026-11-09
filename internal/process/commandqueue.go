package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// addCommandsToQueue adds any commands executed by the handler to the queues
// that the given router chooses.
func addCommandsToQueue(
	ctx context.Context,
	tx *sql.Tx,
	router *messagequeue.Router,
	packer *envelope.EffectPacker,
) error {
	commandEnvelopes, ok := packer.Seal()
	if !ok {
		return nil
	}

	for commandEnvelope := range commandEnvelopes.All() {
		queueID, err := router.RouteCommandToQueue(ctx, tx, commandEnvelope)
		if err != nil {
			return err
		}

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
