package metrics

import (
	"context"
	"fmt"
	"slices"
	"uuid"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// AggregateInstanceEventStreamBindings is a [Group] that holds only the
// [metricsgrpc.AggregateInstanceEventStreamBindings] metric. The database
// stores a count for each pair of handler and stream, and the group reports
// those counts. The subject of each sample is the handler's key followed by the
// stream's ID.
var AggregateInstanceEventStreamBindings = NewGroup(
	"aggregate instance event stream bindings",
	measureAggregateInstanceEventStreamBindings,
	WithMetric[metricsgrpc.AggregateInstanceEventStreamBindings](),
)

// measureAggregateInstanceEventStreamBindings returns one sample for each row
// of the table of those counts: the handler's key, the stream's ID, and the
// number of the handler's instances that are bound to the stream.
func measureAggregateInstanceEventStreamBindings(
	ctx context.Context,
	q xsql.Querier,
	m Manifest,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT
			handler_key,
			stream_id,
			instances
		FROM runkit.aggregate_stream_binding_counts`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query aggregate instance bindings: %w", err)
	}
	defer rows.Close()

	var samples []*metricsgrpc.WatchMetricsResponse_Sample

	for rows.Next() {
		var (
			handlerKey uuid.UUID
			streamID   uuid.UUID
			instances  uint64
		)

		if err := rows.Scan(
			xsql.UUID(&handlerKey),
			xsql.UUID(&streamID),
			&instances,
		); err != nil {
			return nil, fmt.Errorf("unable to scan aggregate instance bindings: %w", err)
		}

		message := metricsgrpc.NewAggregateInstanceEventStreamBindingsBuilder().
			WithHandlerKey(uuidpb.FromStd(handlerKey)).
			WithEventStreamId(uuidpb.FromStd(streamID)).
			WithValue(instances).
			Build()

		subject := slices.Concat(handlerKey[:], streamID[:])

		samples = append(samples, m.NewSample(subject, message))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate aggregate instance bindings: %w", err)
	}

	return samples, nil
}
