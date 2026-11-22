package grpcserver

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"
	"uuid"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/protobuf/configpb"
	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// supportedMetrics lists the metrics that [MetricsService] supports, in the
// order that the preamble reports them. A metric's index in this list is the
// index that a response on the wire identifies it by.
var supportedMetrics = []*metricsgrpc.SupportedMetric{
	metricsgrpc.NewSupportedMetricBuilder().
		WithMessageName(string(proto.MessageName(new(metricsgrpc.EventStreamLength)))).
		WithReach(metricsgrpc.Reach_REACH_CLUSTER).
		Build(),
	metricsgrpc.NewSupportedMetricBuilder().
		WithMessageName(string(proto.MessageName(new(metricsgrpc.AggregateInstanceEventStreamBindings)))).
		WithReach(metricsgrpc.Reach_REACH_CLUSTER).
		Build(),
}

// metricIndexes maps the full name of each supported metric's message to the
// metric's index in [supportedMetrics].
var metricIndexes = map[string]int{}

func init() {
	for i, m := range supportedMetrics {
		metricIndexes[m.GetMessageName()] = i
	}
}

// MetricsService provides the [metricsgrpc.MetricsAPI] service over the
// measurements that this node takes of the engine, so that clients can watch
// them.
//
// While at least one call to WatchMetrics is in progress, the service measures
// the engine every [MetricsService.Interval] and keeps the latest measurements,
// which all of the calls send. It stops measuring when the last call ends.
//
// It supports these metrics:
//
//   - EventStreamLength
//   - AggregateInstanceEventStreamBindings
type MetricsService struct {
	metricsgrpc.UnimplementedMetricsAPIServer

	// NodeID is the ID of the engine's node.
	NodeID uuid.UUID

	// ClusterID is the ID of the engine's cluster.
	ClusterID uuid.UUID

	// Application describes the application that the engine runs.
	Application *configpb.Application

	// DB is the database that the service measures.
	DB *sql.DB

	// Interval is the time between measurements. It must be positive.
	Interval time.Duration

	// EngineLogger is the target for log messages produced by the service.
	EngineLogger *slog.Logger

	// m guards the watches, stop and done fields.
	m sync.Mutex

	// watches is the number of calls to WatchMetrics that are in progress.
	watches int

	// stop stops the goroutine that measures the engine, which runs while
	// watches is above zero.
	stop context.CancelFunc

	// done is closed when the last goroutine that measured the engine has
	// returned. It is nil if none has run.
	done <-chan struct{}

	// latest is the result of the measuring so far. Only the goroutine that
	// measures the engine replaces it, and measuring begins by emptying it.
	latest cosyne.Observable[result]
}

// result is what measuring the engine has produced so far: the snapshot of the
// latest samples, or the error that ended the measuring, after which there are
// no more samples.
type result struct {
	Snapshot snapshot
	Err      error
}

// WatchMetrics implements the WatchMetrics method of
// [metricsgrpc.MetricsAPIServer].
func (s *MetricsService) WatchMetrics(
	_ *metricsgrpc.WatchMetricsRequest,
	res grpc.ServerStreamingServer[metricsgrpc.WatchMetricsResponse],
) error {
	if s.Interval <= 0 {
		panic("metrics interval must be positive")
	}

	observable, release := s.acquire()
	defer release()

	if err := res.Send(s.preamble()); err != nil {
		return fmt.Errorf("unable to send preamble: %w", err)
	}

	// sent is the snapshot that this call last sent samples from.
	var sent snapshot

	for {
		latest, changed := observable.Load()

		if latest.Err != nil {
			// The error is in the engine's log. It doesn't go to the client,
			// which can't act on it.
			return status.Error(
				codes.Unavailable,
				"unable to measure the engine's metrics",
			)
		}

		for _, sample := range latest.Snapshot.ChangesSince(sent) {
			if err := res.Send(
				metricsgrpc.NewWatchMetricsResponseBuilder().
					WithSample(sample).
					Build(),
			); err != nil {
				return fmt.Errorf("unable to send sample: %w", err)
			}
		}

		sent = latest.Snapshot

		select {
		case <-res.Context().Done():
			return res.Context().Err()
		case <-changed:
		}
	}
}

// preamble returns the first response of every call.
func (s *MetricsService) preamble() *metricsgrpc.WatchMetricsResponse {
	return metricsgrpc.NewWatchMetricsResponseBuilder().
		WithPreamble(
			metricsgrpc.NewWatchMetricsResponse_PreambleBuilder().
				WithId(uuidpb.FromStd(s.NodeID)).
				WithClusterId(uuidpb.FromStd(s.ClusterID)).
				WithApplication(s.Application).
				WithSupportedMetrics(supportedMetrics).
				Build(),
		).
		Build()
}

// acquire returns the latest measurements, and a function that releases them.
// It starts measuring the engine if no call has acquired them. Measuring stops
// when every call that acquired the measurements has released them.
func (s *MetricsService) acquire() (cosyne.ObservableR[result], func()) {
	s.m.Lock()
	defer s.m.Unlock()

	if s.watches == 0 {
		// The goroutine from the last time may still be finishing a
		// measurement. It must not store it in the snapshot that this empties.
		if s.done != nil {
			<-s.done
		}

		ctx, stop := context.WithCancel(context.Background())
		done := make(chan struct{})

		s.stop = stop
		s.done = done
		s.latest.Store(result{})

		go func() {
			defer close(done)
			s.measure(ctx)
		}()
	}

	s.watches++

	return &s.latest, func() {
		s.m.Lock()
		defer s.m.Unlock()

		s.watches--

		if s.watches == 0 {
			s.stop()
		}
	}
}

// measure measures the engine once at first and then once each interval, until
// ctx is canceled or a measurement fails. Each time that a measurement changes
// a sample, it stores a new snapshot in s.latest. If a measurement fails, it
// logs the error, stores it there, and returns.
//
// All of the changes of one measurement have the same revision, which is the
// time of the measurement as a fixed-width, big-endian integer. The revision is
// greater than that of every earlier measurement, even if the clock goes
// backward.
func (s *MetricsService) measure(ctx context.Context) {
	ticker := time.NewTicker(s.Interval)
	defer ticker.Stop()

	var revision uint64

	for {
		samples, err := s.samples(ctx)
		now := time.Now()

		if err != nil {
			// A canceled context means that the last call ended, not that the
			// measurement failed.
			if ctx.Err() == nil {
				s.EngineLogger.ErrorContext(
					ctx,
					"unable to measure the engine's metrics",
					xslog.Error(err),
				)

				s.latest.Store(result{Err: err})
			}

			return
		}

		latest, _ := s.latest.Load()
		next := latest.Snapshot.With(samples)

		if changes := next.ChangesSince(latest.Snapshot); len(changes) > 0 {
			measuredAt := uint64(now.UnixNano())
			revision = max(revision+1, measuredAt)
			encoded := binary.BigEndian.AppendUint64(nil, revision)

			// The new snapshot isn't shared until it is stored, so the changes
			// can still be given their revision.
			for _, c := range changes {
				c.SetRevision(encoded)
			}

			s.latest.Store(result{Snapshot: next})
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// samples returns a measurement of every metric.
func (s *MetricsService) samples(
	ctx context.Context,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	lengths, err := s.eventStreamLengths(ctx)
	if err != nil {
		return nil, err
	}

	bindings, err := s.aggregateInstanceEventStreamBindings(ctx)
	if err != nil {
		return nil, err
	}

	return slices.Concat(lengths, bindings), nil
}

// eventStreamLengths returns a measurement of the EventStreamLength metric.
func (s *MetricsService) eventStreamLengths(
	ctx context.Context,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	rows, err := s.DB.QueryContext(
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

		samples = append(samples, newSample(id[:], message))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return samples, nil
}

// aggregateInstanceEventStreamBindings returns a measurement of the
// AggregateInstanceEventStreamBindings metric.
func (s *MetricsService) aggregateInstanceEventStreamBindings(
	ctx context.Context,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	rows, err := s.DB.QueryContext(
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

		samples = append(samples, newSample(subject, message))
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate aggregate instance bindings: %w", err)
	}

	return samples, nil
}
