package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"strings"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// eventsPerSecondThreshold is the lifetime average event rate, in events per
// second, above which a stream is considered at capacity. When all streams are
// at or above this threshold, a new stream is created.
const eventsPerSecondThreshold = 1000

// Acquire returns the ID of the stream to write the next events to.
//
// It acquires the stream with the lowest lifetime average event rate. If all
// streams are at or above [eventsPerSecondThreshold], or if no streams exist,
// a new stream is created and returned.
func Acquire(ctx context.Context, tx *sql.Tx) (*uuidpb.UUID, error) {
	eventStreamID := &uuidpb.UUID{}

	row := tx.QueryRowContext(
		ctx,
		`WITH s AS (
			SELECT
				event_stream_id,
				CASE
					WHEN next_offset = 0 THEN 0
					ELSE next_offset / EXTRACT(EPOCH FROM (now() - created_at))
				END AS lifetime_events_per_second
			FROM event_streams
		)
		SELECT event_stream_id
		FROM s
		WHERE lifetime_events_per_second < $1
		ORDER BY lifetime_events_per_second
		LIMIT 1`,
		eventsPerSecondThreshold,
	)

	if err := row.Scan(database.UnmarshalUUID(eventStreamID)); err == nil {
		return eventStreamID, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("unable to query event streams: %w", err)
	}

	eventStreamID = uuidpb.Generate()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO event_streams (
			event_stream_id
		) VALUES ($1)`,
		database.MarshalUUID(eventStreamID),
	); err != nil {
		return nil, fmt.Errorf("unable to create event stream: %w", err)
	}

	return eventStreamID, nil
}

// Append adds the events in the given envelopes to the end of the given event
// stream.
//
// It returns the next unused offset of the stream after the new events have
// been appended.
func Append(
	ctx context.Context,
	tx *sql.Tx,
	eventStreamID *uuidpb.UUID,
	envelopes *envelopepb.MultiEnvelope,
) (next Offset, err error) {
	numEvents := len(envelopes.GetBodies())

	// Advance the next offset of the stream to accommodate the new events.
	row := tx.QueryRowContext(
		ctx,
		`UPDATE event_streams SET
			next_offset = next_offset + $1
		WHERE event_stream_id = $2
		RETURNING OLD.next_offset`,
		numEvents,
		database.MarshalUUID(eventStreamID),
	)

	if err := row.Scan(&next); err != nil {
		return 0, fmt.Errorf("unable to advance stream offset: %w", err)
	}

	stmt, err := tx.PrepareContext(
		ctx,
		`INSERT INTO events (
			event_stream_id,
			event_offset,
			correlation_id,
			message_type_id,
			envelope,
			aggregate_handler_key,
			aggregate_instance_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
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
			database.MarshalUUID(eventStreamID),
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

// Offsets returns a map of stream ID to the next unused offset for every
// stream.
func Offsets(ctx context.Context, q database.Querier) (*uuidpb.Map[Offset], error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT
			event_stream_id,
			next_offset
		FROM event_streams`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query stream offsets: %w", err)
	}
	defer rows.Close()

	offsets := &uuidpb.Map[Offset]{}
	for rows.Next() {
		var (
			eventStreamID = &uuidpb.UUID{}
			offset        Offset
		)
		if err := rows.Scan(
			database.UnmarshalUUID(eventStreamID),
			&offset,
		); err != nil {
			return nil, fmt.Errorf("unable to scan stream offset: %w", err)
		}

		offsets.Set(eventStreamID, offset)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate stream offsets: %w", err)
	}

	return offsets, nil
}

// Read returns a sequence of events in the given stream, starting at the given
// offset.
func Read(
	ctx context.Context,
	q database.Querier,
	eventStreamID *uuidpb.UUID,
	offset Offset,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(ctx, q, eventStreamID, offset, nil)
}

// ReadByAggregateInstance returns a sequence of events recorded by a specific
// aggregate instance, starting at or after the given offset.
func ReadByAggregateInstance(
	ctx context.Context,
	q database.Querier,
	eventStreamID *uuidpb.UUID,
	offset Offset,
	aggregateHandlerKey *uuidpb.UUID,
	aggregateInstanceID string,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(ctx, q, eventStreamID, offset, map[string]any{
		"aggregate_handler_key": database.MarshalUUID(aggregateHandlerKey),
		"aggregate_instance_id": aggregateInstanceID,
	})
}

// ReadByCorrelationID returns a sequence of events that share the given
// correlation ID within a specific stream, starting at or after the given
// offset.
func ReadByCorrelationID(
	ctx context.Context,
	q database.Querier,
	eventStreamID *uuidpb.UUID,
	offset Offset,
	correlationID *uuidpb.UUID,
) iter.Seq2[*envelopepb.Envelope, error] {
	return read(ctx, q, eventStreamID, offset, map[string]any{
		"correlation_id": database.MarshalUUID(correlationID),
	})
}

// eventsPerPage is the number of events read from the database at a time.
const eventsPerPage = 100

// read returns a sequence of events from the given stream. filters is a map
// of column name to required value.
func read(
	ctx context.Context,
	q database.Querier,
	eventStreamID *uuidpb.UUID,
	offset Offset,
	filters map[string]any,
) iter.Seq2[*envelopepb.Envelope, error] {
	var query strings.Builder
	query.WriteString(
		`SELECT
			event_offset,
			envelope
		FROM events
		WHERE event_stream_id = $1
			AND event_offset >= $2`,
	)

	args := make([]any, 0, len(filters))
	placeholder := 3
	for k, v := range filters {
		query.WriteString(" AND ")
		query.WriteString(k)
		query.WriteString(" = $")
		query.WriteString(strconv.Itoa(placeholder))
		args = append(args, v)
		placeholder++
	}

	query.WriteString(" ORDER BY event_offset LIMIT ")
	query.WriteString(strconv.Itoa(eventsPerPage))

	return func(yield func(*envelopepb.Envelope, error) bool) {
		stmt, err := q.PrepareContext(ctx, query.String())
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
				eventStreamID,
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
// via [envelopepb.SetExtension], and returns the number of events.
func readPage(
	ctx context.Context,
	stmt *sql.Stmt,
	envelopes []*envelopepb.Envelope,
	eventStreamID *uuidpb.UUID,
	offset Offset,
	args ...any,
) (numEvents int, err error) {
	queryArgs := make([]any, 0, 2+len(args))
	queryArgs = append(queryArgs, database.MarshalUUID(eventStreamID))
	queryArgs = append(queryArgs, offset)
	queryArgs = append(queryArgs, args...)

	rows, err := stmt.QueryContext(ctx, queryArgs...)
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

		envelopepb.SetExtension(
			envelope.GetBody(),
			envelopepb.
				NewEventStreamPositionBuilder().
				WithStreamId(eventStreamID).
				WithOffset(uint64(offset)).
				Build(),
		)

		envelopes[numEvents] = envelope
		numEvents++
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("unable to iterate events: %w", err)
	}

	return numEvents, nil
}
