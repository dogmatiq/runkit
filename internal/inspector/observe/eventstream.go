package observe

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// EventStream is one of the engine's event streams.
type EventStream struct {
	// ID is the stream's ID.
	ID *uuidpb.UUID

	// Length is the number of events on the stream, which is also the offset
	// of the next event to be appended.
	Length uint64
}

// Event is an event on an event stream.
type Event struct {
	// StreamID is the ID of the event's stream.
	StreamID *uuidpb.UUID

	// Offset is the event's offset on its stream.
	Offset uint64

	// Envelope is the event's envelope.
	Envelope *envelopepb.Envelope

	// AggregateHandlerKey is the key of the aggregate handler that recorded
	// the event. It is nil if an integration handler recorded the event.
	AggregateHandlerKey *uuidpb.UUID

	// AggregateInstanceID is the ID of the aggregate instance that recorded
	// the event, if an aggregate handler recorded it.
	AggregateInstanceID string

	// RecordedAt is the time that the event was appended to its stream.
	RecordedAt time.Time
}

// eventColumns is the columns that [scanEvent] reads, in order.
const eventColumns = `
	e.stream_id,
	e.stream_offset,
	e.envelope,
	e.aggregate_handler_key,
	e.aggregate_instance_id,
	e.recorded_at`

// scanEvent reads one row of [eventColumns].
func scanEvent(rows interface{ Scan(...any) error }) (Event, error) {
	var (
		e          Event
		envelope   []byte
		handlerKey sql.Null[string]
		instanceID sql.Null[string]
	)

	e.StreamID = &uuidpb.UUID{}

	if err := rows.Scan(
		xsql.UUID(e.StreamID),
		&e.Offset,
		&envelope,
		&handlerKey,
		&instanceID,
		&e.RecordedAt,
	); err != nil {
		return Event{}, err
	}

	if handlerKey.Valid {
		key, err := uuidpb.Parse(handlerKey.V)
		if err != nil {
			return Event{}, err
		}
		e.AggregateHandlerKey = key
		e.AggregateInstanceID = instanceID.V
	}

	e.Envelope = &envelopepb.Envelope{}
	if err := e.Envelope.UnmarshalBinary(envelope); err != nil {
		return Event{}, fmt.Errorf("unable to unmarshal envelope: %w", err)
	}

	return e, nil
}

// EventStreamEvents returns the events on the stream with the given ID with
// offsets lower than before, newest first, up to the given limit.
func (d *Database) EventStreamEvents(
	ctx context.Context,
	streamID *uuidpb.UUID,
	before uint64,
	limit int,
) ([]Event, error) {
	return d.events(
		ctx,
		`SELECT `+eventColumns+`
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
			AND e.stream_offset < $2
		ORDER BY e.stream_offset DESC
		LIMIT $3`,
		xsql.UUID(streamID),
		before,
		limit,
	)
}

// AggregateInstanceEvents returns the events that the given aggregate instance
// recorded, newest first, up to the given limit.
func (d *Database) AggregateInstanceEvents(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	instanceID string,
	limit int,
) ([]Event, error) {
	return d.events(
		ctx,
		`SELECT `+eventColumns+`
		FROM eventstream.events AS e
		WHERE e.aggregate_handler_key = $1
			AND e.aggregate_instance_id = $2
		ORDER BY e.stream_offset DESC
		LIMIT $3`,
		xsql.UUID(handlerKey),
		instanceID,
		limit,
	)
}

// Event returns the event with the given message ID. It returns false if there
// is no such event.
func (d *Database) Event(ctx context.Context, id *uuidpb.UUID) (Event, bool, error) {
	events, err := d.events(
		ctx,
		`SELECT `+eventColumns+`
		FROM eventstream.events AS e
		WHERE e.message_id = $1`,
		xsql.UUID(id),
	)
	if err != nil || len(events) == 0 {
		return Event{}, false, err
	}

	return events[0], true, nil
}

func (d *Database) events(ctx context.Context, query string, args ...any) ([]Event, error) {
	var events []Event

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				e, err := scanEvent(rows)
				if err != nil {
					return err
				}
				events = append(events, e)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read events: %w", err)
	}

	return events, nil
}

// eventStreams returns the event streams, ordered by ID, up to the given
// limit.
func (d *Database) eventStreams(ctx context.Context, limit int) ([]EventStream, error) {
	var streams []EventStream

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT s.id, s.next_offset
				FROM eventstream.streams AS s
				ORDER BY s.id
				LIMIT $1`,
				limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				s := EventStream{
					ID: &uuidpb.UUID{},
				}

				if err := rows.Scan(
					xsql.UUID(s.ID),
					&s.Length,
				); err != nil {
					return err
				}

				streams = append(streams, s)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read event streams: %w", err)
	}

	return streams, nil
}
