package metrics_test

import (
	"context"
	"testing"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/runkit/internal/x/xsql"

	. "github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics"
)

func TestNewGroup(t *testing.T) {
	measure := func(
		context.Context,
		xsql.Querier,
		Manifest,
	) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
		return nil, nil
	}

	metric := WithMetric[metricsgrpc.EventStreamLength]()

	t.Run("it panics if the name is empty", func(t *testing.T) {
		mustPanic(t, func() {
			NewGroup("", measure, metric)
		})
	})

	t.Run("it panics if measure is nil", func(t *testing.T) {
		mustPanic(t, func() {
			NewGroup("name", nil, metric)
		})
	})

	t.Run("it panics if there are no metrics", func(t *testing.T) {
		mustPanic(t, func() {
			NewGroup("name", measure)
		})
	})
}
