package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"iter"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Append adds the events in the given envelopes to the end of the event stream.
//
// It returns the next unused offset of the stream after the new events have
// been appended.
func Append(
	ctx context.Context,
	tx *sql.Tx,
	envelopes *envelopepb.MultiEnvelope,
) (next Offset, err error) {
	numEvents := len(envelopes.GetBodies())

	// Advance the next offset of the stream to accommodate the new events.
	//
	// The conflict target (TRUE) refers to idx_event_stream_offset_singleton,
	// which enforces the "exactly one row" invariant on the event_stream_offset
	// table.
	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO event_stream_offset (
			next_offset
		) VALUES ($1)
		ON CONFLICT ((TRUE))
		DO UPDATE
			SET next_offset = event_stream_offset.next_offset + EXCLUDED.next_offset
		RETURNING COALESCE(OLD.next_offset, 0)`,
		numEvents,
	)

	if err := row.Scan(&next); err != nil {
		return 0, fmt.Errorf("unable to advance stream offset: %w", err)
	}

	stmt, err := tx.PrepareContext(
		ctx,
		`INSERT INTO event_stream (
			event_offset,
			correlation_id,
			message_type_id,
			envelope,
			aggregate_handler_key,
			aggregate_instance_id
		) VALUES ($1, $2, $3, $4, $5, $6)`,
	)
	if err != nil {
		return 0, fmt.Errorf("unable to prepare event insert statement: %w", err)
	}
	defer stmt.Close()

	// Insert each event into the events table with the claimed offsets.
	for envelope := range envelopes.All() {
		// Populate both aggregate_handler_key and aggregate_instance_id only if
		// the source instance ID is non-empty.
		//
		// Aggregates are the only handler type with per-instance semantics that
		// produce events, so this check is adequate to detect that the source
		// handler is an aggregate.
		var aggregateHandlerKey, aggregateInstanceID any
		if id := envelope.GetHeader().GetSource().GetInstanceId(); id != "" {
			aggregateHandlerKey = database.MarshalUUID(envelope.GetHeader().GetSource().GetHandler().GetKey())
			aggregateInstanceID = id
		}

		if _, err := stmt.ExecContext(
			ctx,
			next,
			database.MarshalUUID(envelope.GetHeader().GetCorrelationId()),
			database.MarshalUUID(envelope.GetBody().GetMessage().GetTypeId()),
			database.MarshalEnvelope(envelope),
			aggregateHandlerKey,
			aggregateInstanceID,
		); err != nil {
			return 0, fmt.Errorf("unable to insert event at offset %d: %w", next, err)
		}

		next++
	}

	return next, nil
}

// Read returns a sequence of events in the stream, starting at the given
// offset.
func Read(
	ctx context.Context,
	q database.Querier,
	offset Offset,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(
		ctx,
		q,
		`SELECT
			event_offset,
			envelope
		FROM event_stream
		WHERE event_offset >= $1
		ORDER BY event_offset
		LIMIT $2`,
		offset,
		eventsPerPage,
	)
}

// ReadByAggregateInstance returns a sequence of events recorded by a specific
// aggregate instance, starting at or after the given offset.
func ReadByAggregateInstance(
	ctx context.Context,
	q database.Querier,
	offset Offset,
	aggregateHandlerKey *uuidpb.UUID,
	aggregateInstanceID string,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(
		ctx,
		q,
		`SELECT
			event_offset,
			envelope
		FROM event_stream
		WHERE event_offset >= $1
			AND aggregate_handler_key = $2
			AND aggregate_instance_id = $3
		ORDER BY event_offset
		LIMIT $4`,
		offset,
		database.MarshalUUID(aggregateHandlerKey),
		aggregateInstanceID,
		eventsPerPage,
	)
}

// ReadByCorrelationID returns a sequence of events that share the given
// correlation ID, starting at or after the given offset.
func ReadByCorrelationID(
	ctx context.Context,
	q database.Querier,
	offset Offset,
	correlationID *uuidpb.UUID,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(
		ctx,
		q,
		`SELECT
			event_offset,
			envelope
		FROM event_stream
		WHERE event_offset >= $1
			AND correlation_id = $2
		ORDER BY event_offset
		LIMIT $3`,
		offset,
		database.MarshalUUID(correlationID),
		eventsPerPage,
	)
}

// eventsPerPage is the number of events read from the database at a time.
const eventsPerPage = 100

// read returns a sequence of events read from the database using the given
// query and arguments, starting at the given offset. The query must accept the
// offset as $1.
func read(
	ctx context.Context,
	q database.Querier,
	query string,
	offset Offset,
	args ...any,
) iter.Seq2[*envelopepb.Envelope, error] {
	return func(yield func(*envelopepb.Envelope, error) bool) {
		stmt, err := q.PrepareContext(ctx, query)
		if err != nil {
			yield(nil, fmt.Errorf("unable to prepare event query statement: %w", err))
			return
		}
		defer stmt.Close()

		envelopes := make([]*envelopepb.Envelope, eventsPerPage)

		for {
			// Collect the events into the slice first so that the transaction
			// can be used during the call to yield().
			numEnvelopes, err := readPage(
				ctx,
				stmt,
				envelopes,
				offset,
				args...,
			)
			if err != nil {
				yield(nil, fmt.Errorf("unable to read page of events: %w", err))
				return
			}

			for _, envelope := range envelopes[:numEnvelopes] {
				if !yield(envelope, nil) {
					return
				}
			}

			if numEnvelopes < eventsPerPage {
				return
			}

			last := envelopes[numEnvelopes-1]
			offset = OffsetOf(last) + 1
		}
	}
}

// readPage reads a page of events from the database, starting at the given
// offset.
//
// It populates envelopes with the events read, attaching each event's offset
// via [setOffset], and returns the number of events.
func readPage(
	ctx context.Context,
	stmt *sql.Stmt,
	envelopes []*envelopepb.Envelope,
	offset Offset,
	args ...any,
) (numEvents int, err error) {
	rows, err := stmt.QueryContext(
		ctx,
		append(
			[]any{offset},
			args...,
		)...,
	)
	if err != nil {
		return 0, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			offset   Offset
			envelope = &envelopepb.Envelope{}
		)
		if err := rows.Scan(
			&offset,
			database.UnmarshalEnvelope(envelope),
		); err != nil {
			return 0, fmt.Errorf("unable to scan event: %w", err)
		}

		setOffset(envelope, offset)

		envelopes[numEvents] = envelope
		numEvents++
	}

	return numEvents, rows.Err()
}
