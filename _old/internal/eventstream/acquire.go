package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
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
		`WITH stream_rates AS (
			SELECT
				s.event_stream_id,
				COALESCE(
					(s.next_offset - e.event_offset) / EXTRACT(EPOCH FROM clock_timestamp() - e.recorded_at),
					0
				) AS events_per_second
			FROM event_streams AS s
			LEFT JOIN events AS e
				ON e.event_stream_id = s.event_stream_id
				AND e.event_offset = GREATEST(0, s.next_offset - $1)
		)
		SELECT event_stream_id
		FROM stream_rates
		WHERE events_per_second < $1
		ORDER BY events_per_second
		LIMIT 1`,
		Capacity,
	)

	eventStreamID := &uuidpb.UUID{}

	if err := row.Scan(
		database.UnmarshalUUID(eventStreamID),
	); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
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
	}

	return eventStreamID, nil
}
