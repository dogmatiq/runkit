package eventstream

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// Append adds events to the end of the event stream with the given ID.
func Append(
	ctx context.Context,
	tx *sql.Tx,
	eventStreamID *uuidpb.UUID,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	source := eventEnvelopes.GetHeader().GetSource()

	var (
		aggregateHandlerKey *uuidpb.UUID
		aggregateInstanceID any
	)
	if source.GetInstanceId() != "" {
		aggregateHandlerKey = source.GetHandler().GetKey()
		aggregateInstanceID = source.GetInstanceId()
	}

	for eventEnvelope := range eventEnvelopes.All() {
		if _, err := tx.ExecContext(
			ctx,
			`WITH x AS (
				UPDATE dogma.event_streams SET
					next_offset = next_offset + 1
				WHERE event_stream_id = $1
				RETURNING
					event_stream_id,
					OLD.next_offset
			)
			INSERT INTO dogma.events (
				event_stream_id,
				event_offset,
				envelope,
				aggregate_handler_key,
				aggregate_instance_id
			)
			SELECT
				event_stream_id,
				next_offset,
				$2,
				$3,
				$4
			FROM x`,
			xsql.UUID(eventStreamID),
			xsql.Envelope(eventEnvelope),
			xsql.UUID(aggregateHandlerKey),
			aggregateInstanceID,
		); err != nil {
			return fmt.Errorf("unable to append event to stream: %w", err)
		}
	}

	return nil
}
