package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
	streamID *uuidpb.UUID,
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
				UPDATE eventstream.streams SET
					next_offset = next_offset + 1
				WHERE id = $1
				RETURNING
					id,
					OLD.next_offset
			)
			INSERT INTO eventstream.events (
				stream_id,
				stream_offset,
				message_id,
				correlation_id,
				message_type_id,
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
				$5,
				$6,
				$7
			FROM streams AS s
			RETURNING stream_offset`,
			xsql.UUID(streamID),
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.UUID(eventEnvelope.GetHeader().GetCorrelationId()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId()),
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

// AppendAny acquires an event stream and appends events to it.
//
// It returns the ID of the acquired stream and the next offset in the stream
// after the appended events.
func AppendAny(
	ctx context.Context,
	tx *sql.Tx,
	eventEnvelopes *envelopepb.MultiEnvelope,
) (*uuidpb.UUID, Offset, error) {
	var (
		aggregateHandlerKey *uuidpb.UUID
		aggregateInstanceID any
	)
	if source := eventEnvelopes.GetHeader().GetSource(); source.GetInstanceId() != "" {
		aggregateHandlerKey = source.GetHandler().GetKey()
		aggregateInstanceID = source.GetInstanceId()
	}

	var (
		query strings.Builder
		args  []any
	)

	// $1 = correlation_id, $2 = aggregate_handler_key, $3 = aggregate_instance_id
	args = append(
		args,
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
		xsql.UUID(aggregateHandlerKey),
		aggregateInstanceID,
	)

	query.WriteString(`SELECT * FROM eventstream.append_any($1, $2, $3, ARRAY[`)

	first := true
	for eventEnvelope := range eventEnvelopes.All() {
		if first {
			first = false
		} else {
			query.WriteString(", ")
		}

		n := len(args)
		fmt.Fprintf(
			&query,
			"ROW($%d, $%d, $%d)::eventstream.event",
			n+1, n+2, n+3,
		)

		args = append(
			args,
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(eventEnvelope),
		)
	}

	query.WriteString(`])`)

	streamID := &uuidpb.UUID{}
	var nextOffset Offset

	row := tx.QueryRowContext(ctx, query.String(), args...)
	if err := row.Scan(xsql.UUID(streamID), &nextOffset); err != nil {
		return nil, 0, fmt.Errorf("unable to append events to stream: %w", err)
	}

	return streamID, nextOffset, nil
}
