package eventstream

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
)

// Offset is a zero-based position of an event in the event stream.
type Offset uint64

// Event is an event that has been appended to the event stream.
type Event struct {
	// Offset is the event's offset within the stream.
	Offset Offset

	// Envelope is the message envelope containing the event.
	Envelope *envelopepb.Envelope
}

// Append adds the events in the given envelopes to the end of the event stream.
//
// It returns the offset _after_ the last event appended; that is, the next
// "unused" offset on the stream.
func Append(
	ctx context.Context,
	tx *sql.Tx,
	envelopes *envelopepb.MultiEnvelope,
) (offsetAfterLastEvent Offset, err error) {
	numEvents := len(envelopes.GetBodies())

	// Claim offsets for the new events. This is done before inserting any
	// events to ensure that the offsets are contiguous. The ((TRUE)) conflict
	// target matches the singleton unique index that enforces the table's
	// "exactly one row" invariant.
	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO eventstream.next_offset (next_offset) VALUES ($1)
		ON CONFLICT ((TRUE)) DO UPDATE
			SET next_offset = next_offset.next_offset + $1
		RETURNING next_offset`,
		numEvents,
	)

	if err := row.Scan(&offsetAfterLastEvent); err != nil {
		return 0, fmt.Errorf("unable to claim offsets for %d events: %w", numEvents, err)
	}

	offsetOfNextEvent := offsetAfterLastEvent - Offset(numEvents)

	// Insert each event into the events table with the claimed offsets.
	for envelope := range envelopes.All() {
		envData, err := envelope.MarshalBinary()
		if err != nil {
			return 0, fmt.Errorf("unable to marshal event envelope: %w", err)
		}

		// aggregate_handler_key and aggregate_instance_id are populated
		// together for events recorded by an aggregate handler, and left
		// NULL together for events recorded by an integration handler. The
		// source instance ID being empty is the discriminator.
		var aggregateHandlerKey, aggregateInstanceID any
		if id := envelope.GetHeader().GetSource().GetInstanceId(); id != "" {
			aggregateHandlerKey = envelope.GetHeader().GetSource().GetHandler().GetKey().AsString()
			aggregateInstanceID = id
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO eventstream.events (
				"offset",
				message_type_id,
				correlation_id,
				envelope,
				aggregate_handler_key,
				aggregate_instance_id
			) VALUES ($1, $2, $3, $4, $5, $6)`,
			offsetOfNextEvent,
			envelope.GetBody().GetMessage().GetTypeId().AsString(),
			envelope.GetHeader().GetCorrelationId().AsString(),
			envData,
			aggregateHandlerKey,
			aggregateInstanceID,
		); err != nil {
			return 0, fmt.Errorf("unable to insert event at offset %d: %w", offsetOfNextEvent, err)
		}

		offsetOfNextEvent++
	}

	return offsetAfterLastEvent, nil
}

// Read calls fn for each event in the stream, starting with the event at the
// given offset.
//
// handlerKey and instanceID are optional constraints that limit the stream to
// events for a specific handler and/or instance.
func Read(
	ctx context.Context,
	tx *sql.Tx,
	offset Offset,
	handlerKey, instanceID string,
	fn func(Event) error,
) error {
	events := make([]Event, eventsPerPage)

	for {
		numEvents, err := readPage(
			ctx,
			tx,
			offset,
			handlerKey,
			instanceID,
			events,
		)
		if err != nil {
			return err
		}

		for _, event := range events[:numEvents] {
			if err := fn(event); err != nil {
				return err
			}
		}

		if numEvents < eventsPerPage {
			return nil
		}

		offset = events[numEvents-1].Offset + 1
	}
}

// eventsPerPage is the number of events read from the database at a time.
const eventsPerPage = 100

// readPage reads a page of events from the database, starting at the given
// offset.
//
// It populates the given offsets and envelopes slices with the events read,
// and returns the number of events.
func readPage(
	ctx context.Context,
	tx *sql.Tx,
	offset Offset,
	handlerKey, instanceID string,
	events []Event,
) (numEvents int, err error) {
	var rows *sql.Rows

	if handlerKey == "" {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT "offset", envelope
			FROM eventstream.events
			WHERE "offset" >= $1
			ORDER BY "offset"
			LIMIT $2`,
			offset,
			eventsPerPage,
		)
	} else {
		rows, err = tx.QueryContext(
			ctx,
			`SELECT "offset", envelope
			FROM eventstream.events
			WHERE aggregate_handler_key = $1
				AND aggregate_instance_id = $2
				AND "offset" >= $3
			ORDER BY "offset"
			LIMIT $4`,
			handlerKey,
			instanceID,
			offset,
			eventsPerPage,
		)
	}

	if err != nil {
		return 0, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			eventOffset  Offset
			envelopeData []byte
		)
		if err := rows.Scan(&eventOffset, &envelopeData); err != nil {
			return 0, fmt.Errorf("unable to scan event: %w", err)
		}

		var envelope envelopepb.Envelope
		if err := envelope.UnmarshalBinary(envelopeData); err != nil {
			return 0, fmt.Errorf("unable to unmarshal envelope: %w", err)
		}

		events[numEvents] = Event{
			Offset:   eventOffset,
			Envelope: &envelope,
		}
		numEvents++
	}

	return numEvents, rows.Err()
}
