package grpcserver

import (
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"uuid"

	"github.com/dogmatiq/protobuf/configpb"
	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// MetricsService serves the [metricsgrpc.MetricsAPIServer] service from this
// node. It implements WatchClusterMetrics, and leaves WatchNodeMetrics
// unimplemented, so that gRPC answers it with an UNIMPLEMENTED error.
//
// While at least one call to WatchClusterMetrics is in progress, the service
// measures the engine and sends the changes to every call, which share the
// measurements. [MetricsService.MinInterval] and [MetricsService.MaxDutyCycle]
// say how often it measures each group of metrics. It stops measuring when the
// last call ends.
//
// WatchClusterMetrics panics unless MinInterval is positive and MaxDutyCycle is
// above 0 and at most 1.
//
// WatchClusterMetrics reports these metrics:
//
//   - EventStreamLength
//   - AggregateInstanceEventStreamBindings
type MetricsService struct {
	metricsgrpc.UnimplementedMetricsAPIServer

	// NodeID is the ID of the engine's node.
	NodeID uuid.UUID

	// ClusterID is the ID of the engine's cluster.
	ClusterID uuid.UUID

	// Application describes the application that runs on the engine.
	Application *configpb.Application

	// DB is the database to measure.
	DB *sql.DB

	// MinInterval is the shortest time between the starts of two measurements
	// of the same group of metrics. It must be positive.
	MinInterval time.Duration

	// MaxDutyCycle is the largest fraction of the time during which the service
	// may be measuring a group of metrics, counted from the start of one
	// measurement to the start of the next. It must be above 0 and at most 1.
	//
	// For example, if it is 0.1, the next measurement starts at least 20
	// seconds after the start of a measurement that took 2 seconds.
	MaxDutyCycle float64

	// EngineLogger is the target for log messages produced by the service.
	EngineLogger *slog.Logger

	// clusterOnce makes the cluster field the first time that a call needs it.
	clusterOnce sync.Once

	// cluster measures the metrics that WatchClusterMetrics reports.
	cluster *metrics.Reporter
}

// WatchClusterMetrics implements the WatchClusterMetrics method of
// [metricsgrpc.MetricsAPIServer].
func (s *MetricsService) WatchClusterMetrics(
	_ *metricsgrpc.WatchClusterMetricsRequest,
	res grpc.ServerStreamingServer[metricsgrpc.WatchMetricsResponse],
) error {
	reporter := s.clusterReporter()

	observable, release := reporter.Acquire()
	defer release()

	if err := res.Send(s.preamble(reporter)); err != nil {
		return fmt.Errorf("unable to send preamble: %w", err)
	}

	// sent is the snapshot that this call last sent samples from.
	var sent metrics.Snapshot

	for {
		latest, changed := observable.Load()

		if latest.Err != nil {
			// The error is in the engine's log. The client receives a fixed
			// message instead.
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

// clusterReporter returns the reporter of the metrics that WatchClusterMetrics
// reports.
func (s *MetricsService) clusterReporter() *metrics.Reporter {
	s.clusterOnce.Do(func() {
		s.cluster = &metrics.Reporter{
			DB:           s.DB,
			EngineLogger: s.EngineLogger,
			MinInterval:  s.MinInterval,
			MaxDutyCycle: s.MaxDutyCycle,
			Groups: []metrics.Group{
				metrics.EventStreamLengths,
				metrics.AggregateInstanceEventStreamBindings,
			},
		}
	})

	return s.cluster
}

// preamble returns the first response of every call. It lists the metrics that
// the given reporter measures.
func (s *MetricsService) preamble(
	reporter *metrics.Reporter,
) *metricsgrpc.WatchMetricsResponse {
	return metricsgrpc.NewWatchMetricsResponseBuilder().
		WithPreamble(
			metricsgrpc.NewWatchMetricsResponse_PreambleBuilder().
				WithId(uuidpb.FromStd(s.NodeID)).
				WithClusterId(uuidpb.FromStd(s.ClusterID)).
				WithApplication(s.Application).
				WithSupportedMetrics(reporter.Names()).
				Build(),
		).
		Build()
}
