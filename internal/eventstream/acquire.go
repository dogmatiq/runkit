package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

const (
	// Capacity is the target maximum sustained throughput, in events per
	// second, for each event stream. If a stream exceeds this throughput it is
	// considered to be at capacity and is ignored by [Acquire].
	Capacity = 1000
)

// Acquire returns the ID of the stream with the most available capacity. If all
// existing streams are at capacity according to [Capacity], a new stream is
// created and its ID returned.
func Acquire(ctx context.Context, tx *sql.Tx) (*uuidpb.UUID, error) {
	row := tx.QueryRowContext(
		ctx,
		`WITH streams AS (
			SELECT
				s.id,
				COALESCE(
					(s.next_offset - e.event_stream_offset) / EXTRACT(EPOCH FROM clock_timestamp() - e.recorded_at),
					0
				) AS events_per_second
			FROM dogma.event_streams AS s
			LEFT JOIN dogma.events AS e
				ON e.event_stream_id = s.id
				AND e.event_stream_offset = GREATEST(0, s.next_offset - $1)
		)
		SELECT id
		FROM streams
		WHERE events_per_second < $1
		ORDER BY events_per_second
		LIMIT 1`,
		Capacity,
	)

	eventStreamID := &uuidpb.UUID{}

	if err := row.Scan(
		xsql.UUID(eventStreamID),
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("unable to query event streams: %w", err)
		}

		return Create(ctx, tx)
	}

	return eventStreamID, nil
}

// Create forces creation of a new event stream.
func Create(ctx context.Context, tx *sql.Tx) (*uuidpb.UUID, error) {
	eventStreamID := uuidpb.Generate()

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO dogma.event_streams (
			id
		) VALUES ($1)`,
		xsql.UUID(eventStreamID),
	); err != nil {
		return nil, fmt.Errorf("unable to create event stream: %w", err)
	}

	return eventStreamID, nil
}
