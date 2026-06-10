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
//
// It returns the next offset in the stream after the appended events.
func Append(
	ctx context.Context,
	tx *sql.Tx,
	eventStreamID *uuidpb.UUID,
	eventEnvelopes *envelopepb.MultiEnvelope,
) (Offset, error) {
	var (
		aggregateHandlerKey *uuidpb.UUID
		aggregateInstanceID any
	)
	if source := eventEnvelopes.GetHeader().GetSource(); source.GetInstanceId() != "" {
		aggregateHandlerKey = source.GetHandler().GetKey()
		aggregateInstanceID = source.GetInstanceId()
	}

	var offset Offset

	for eventEnvelope := range eventEnvelopes.All() {
		row := tx.QueryRowContext(
			ctx,
			`WITH streams AS (
				UPDATE dogma.event_streams SET
					next_offset = next_offset + 1
				WHERE id = $1
				RETURNING
					id,
					OLD.next_offset
			)
			INSERT INTO dogma.events (
				event_stream_id,
				event_stream_offset,
				message_id,
				envelope,
				aggregate_handler_key,
				aggregate_instance_id
			)
			SELECT
				s.id,
				s.next_offset,
				$2,
				$3,
				$4,
				$5
			FROM streams AS s
			RETURNING event_stream_offset`,
			xsql.UUID(eventStreamID),
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.Envelope(eventEnvelope),
			xsql.UUID(aggregateHandlerKey),
			aggregateInstanceID,
		)

		if err := row.Scan(&offset); err != nil {
			return 0, fmt.Errorf("unable to append event to stream: %w", err)
		}
	}

	return offset + 1, nil
}
