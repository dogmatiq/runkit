package process

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// addCommandsToQueue inserts any commands scheduled by the handler into the
// command queue.
func addCommandsToQueue(
	ctx context.Context,
	tx *sql.Tx,
	packer *envelopepb.EffectPacker,
) error {
	commandEnvelopes, ok := packer.Seal()
	if !ok {
		return nil
	}

	for commandEnvelope := range commandEnvelopes.All() {
		if _, err := tx.ExecContext(
			ctx,
			`SELECT commandqueue.add($1, $2, $3, $4)`,
			xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
			xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(commandEnvelope),
			commandEnvelope.GetBody().GetIdempotencyKey(),
		); err != nil {
			return fmt.Errorf("unable to add command to queue: %w", err)
		}
	}

	return nil
}
