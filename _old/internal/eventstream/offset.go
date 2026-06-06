package eventstream

import (
	"context"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Offset is a zero-based position of an event within a specific event stream.
type Offset uint64

// OffsetOf returns the event stream offset stored on env's body extensions.
//
// It panics if the envelope was not obtained from the event stream.
func OffsetOf(env *envelopepb.Envelope) Offset {
	pos, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](env.GetBody())
	if err != nil {
		panic("unexpected error reading event stream position: " + err.Error())
	}
	if !ok {
		panic("envelope does not have an event stream position")
	}

	return Offset(pos.GetOffset())
}

// Offsets returns a map of stream ID to the next unused offset for every
// stream.
func Offsets(ctx context.Context, q database.Executor) (*uuidpb.Map[Offset], error) {
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
