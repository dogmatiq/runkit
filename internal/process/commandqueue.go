package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// addCommandsToQueue inserts any commands scheduled by the handler into the
// command queue.
func addCommandsToQueue(
	ctx context.Context,
	tx *sql.Tx,
	envelopes []preparedEnvelope,
) error {
	for _, prepared := range envelopes {
		commandEnvelope := prepared.envelope
		if _, err := tx.ExecContext(
			ctx,
			`SELECT commandqueue.add($1, $2, $3, $4)`,
			xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
			xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
			prepared.data,
			commandEnvelope.GetBody().GetIdempotencyKey(),
		); err != nil {
			return fmt.Errorf("unable to add command to queue: %w", err)
		}
	}

	return nil
}
