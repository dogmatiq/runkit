package metrics

import (
	"context"
	"slices"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Group is a set of metrics that are measured together, and are repeatedly
// measured on one timer. Use [NewGroup] to make one.
//
// A group is fixed once it is made, so it can be shared.
type Group struct {
	name    string
	metrics []ManifestOption
	measure func(
		ctx context.Context,
		q xsql.Querier,
		m Manifest,
	) ([]*metricsgrpc.WatchMetricsResponse_Sample, error)
}

// NewGroup returns a group of metrics. Each given option adds one metric to the
// group, as it would to a [Manifest].
//
// The name describes the group in log records.
//
// The group measures its metrics by calling fn. fn is given a querier for the
// database, and a manifest that has the group's metrics, which fn uses to build
// its samples.
//
// It panics if the name is empty, if fn is nil, or if the options are empty.
func NewGroup(
	name string,
	fn func(
		ctx context.Context,
		q xsql.Querier,
		m Manifest,
	) ([]*metricsgrpc.WatchMetricsResponse_Sample, error),
	metrics ...ManifestOption,
) Group {
	if name == "" {
		panic("group name must not be empty")
	}

	if fn == nil {
		panic("group must have a measure function")
	}

	if len(metrics) == 0 {
		panic("group must have at least one metric")
	}

	return Group{
		name:    name,
		metrics: slices.Clone(metrics),
		measure: fn,
	}
}
