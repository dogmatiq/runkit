package metrics

import (
	"context"
	"fmt"
	"uuid"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// EventStreamLengths is a [Group] that holds only the
// [metricsgrpc.EventStreamLength] metric. It reads the offset of each event
// stream's next event from the table of event streams, and the subject of each
// sample is the stream's ID.
var EventStreamLengths = NewGroup(
	"event stream lengths",
	measureEventStreamLengths,
	WithMetric[metricsgrpc.EventStreamLength](),
)

// measureEventStreamLengths returns one sample for each row of the table of
// event streams: the stream's ID, and the offset at which its next event will
// be appended.
func measureEventStreamLengths(
	ctx context.Context,
	q xsql.Querier,
	m Manifest,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT
			id,
			next_offset
		FROM runkit.event_streams`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	var samples []*metricsgrpc.WatchMetricsResponse_Sample

	for rows.Next() {
		var (
			id     uuid.UUID
			length uint64
		)

		if err := rows.Scan(xsql.UUID(&id), &length); err != nil {
			return nil, fmt.Errorf("unable to scan event stream: %w", err)
		}

		message := metricsgrpc.NewEventStreamLengthBuilder().
			WithEventStreamId(uuidpb.FromStd(id)).
			WithValue(length).
			Build()

		samples = append(samples, m.NewSample(id[:], message))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return samples, nil
}
