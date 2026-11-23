package metrics_test

import (
	"context"
	"reflect"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"

	. "github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics"
)

func TestReporter(t *testing.T) {
	t.Run("func Acquire()", func(t *testing.T) {
		t.Run("it sends a zero for each subject that a measurement drops", func(t *testing.T) {
			t.Parallel()

			var (
				streamA = uuid.New()
				streamB = uuid.New()
				binding = uuid.New()
			)

			// The first measurement has two streams and one binding. Every
			// later measurement has only the first stream.
			calls := 0

			group := NewGroup(
				"scripted",
				func(
					_ context.Context,
					_ xsql.Querier,
					m Manifest,
				) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
					calls++

					length := func(id uuid.UUID) *metricsgrpc.WatchMetricsResponse_Sample {
						return m.NewSample(
							id[:],
							metricsgrpc.NewEventStreamLengthBuilder().
								WithEventStreamId(uuidpb.FromStd(id)).
								WithValue(7).
								Build(),
						)
					}

					samples := []*metricsgrpc.WatchMetricsResponse_Sample{
						length(streamA),
					}

					if calls == 1 {
						samples = append(
							samples,
							length(streamB),
							m.NewSample(
								binding[:],
								metricsgrpc.NewAggregateInstanceEventStreamBindingsBuilder().
									WithEventStreamId(uuidpb.FromStd(binding)).
									WithValue(3).
									Build(),
							),
						)
					}

					return samples, nil
				},
				WithMetric[metricsgrpc.EventStreamLength](),
				WithMetric[metricsgrpc.AggregateInstanceEventStreamBindings](),
			)

			reporter := &Reporter{
				DB:           xtesting.NewDatabase(t),
				EngineLogger: spruce.NewTestLogger(t),
				MinInterval:  time.Millisecond,
				MaxDutyCycle: 1,
				Groups:       []Group{group},
			}

			observable, release := reporter.Acquire()
			defer release()

			expectSamples(
				t,
				reporter,
				observable,
				map[string]map[string]uint64{
					nameOf[metricsgrpc.EventStreamLength](): {
						streamA.String(): 7,
						streamB.String(): 0,
					},
					nameOf[metricsgrpc.AggregateInstanceEventStreamBindings](): {
						binding.String(): 0,
					},
				},
			)
		})
	})
}

// expectSamples fails the test unless the latest value of every subject of
// every metric that the reporter has measured becomes the given one. The keys
// of want are the metric's full name, and then the subject as a UUID.
func expectSamples(
	t *testing.T,
	reporter *Reporter,
	observable cosyne.ObservableR[Result],
	want map[string]map[string]uint64,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
	defer cancel()

	names := reporter.Names()

	for {
		latest, changed := observable.Load()
		if latest.Err != nil {
			t.Fatal(latest.Err)
		}

		got := map[string]map[string]uint64{}

		for _, sample := range latest.Snapshot.ChangesSince(Snapshot{}) {
			name := names[sample.GetMetricIndex()]

			if got[name] == nil {
				got[name] = map[string]uint64{}
			}

			subject := uuid.UUID(sample.GetSubject()).String()
			got[name][subject] = valueOf(t, name, sample.GetValue())
		}

		if reflect.DeepEqual(want, got) {
			return
		}

		select {
		case <-changed:
		case <-ctx.Done():
			t.Fatalf("unexpected samples:\n got %v\nwant %v", got, want)
		}
	}
}
