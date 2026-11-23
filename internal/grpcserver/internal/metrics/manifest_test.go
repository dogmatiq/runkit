package metrics_test

import (
	"slices"
	"testing"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"google.golang.org/protobuf/proto"

	. "github.com/dogmatiq/runkit/internal/grpcserver/internal/metrics"
)

func TestManifest(t *testing.T) {
	var (
		lengthName   = string(proto.MessageName(new(metricsgrpc.EventStreamLength)))
		bindingsName = string(proto.MessageName(new(metricsgrpc.AggregateInstanceEventStreamBindings)))
	)

	manifest := NewManifest(
		WithMetric[metricsgrpc.EventStreamLength](),
		WithMetric[metricsgrpc.AggregateInstanceEventStreamBindings](),
	)

	t.Run("func NewManifest()", func(t *testing.T) {
		t.Run("it panics if two options add the same metric", func(t *testing.T) {
			mustPanic(t, func() {
				NewManifest(
					WithMetric[metricsgrpc.EventStreamLength](),
					WithMetric[metricsgrpc.EventStreamLength](),
				)
			})
		})

		t.Run("it panics if an option is nil", func(t *testing.T) {
			mustPanic(t, func() {
				NewManifest(nil)
			})
		})
	})

	t.Run("func Names()", func(t *testing.T) {
		t.Run("it returns the full names of the metrics in order", func(t *testing.T) {
			want := []string{lengthName, bindingsName}

			if got := manifest.Names(); !slices.Equal(got, want) {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	})

	t.Run("func NewSample()", func(t *testing.T) {
		t.Run("it identifies the metric by its index in the list", func(t *testing.T) {
			value := metricsgrpc.NewAggregateInstanceEventStreamBindingsBuilder().
				WithValue(3).
				Build()

			sample := manifest.NewSample([]byte("subject"), value)

			if got, want := sample.GetMetricIndex(), uint32(1); got != want {
				t.Fatalf("got index %d, want %d", got, want)
			}

			if got, want := string(sample.GetSubject()), "subject"; got != want {
				t.Fatalf("got subject %q, want %q", got, want)
			}

			var got metricsgrpc.AggregateInstanceEventStreamBindings
			if err := proto.Unmarshal(sample.GetValue(), &got); err != nil {
				t.Fatal(err)
			}

			if !proto.Equal(&got, value) {
				t.Fatalf("got value %v, want %v", &got, value)
			}
		})

		t.Run("it returns a sample with no revision", func(t *testing.T) {
			sample := manifest.NewSample(nil, new(metricsgrpc.EventStreamLength))

			if len(sample.GetRevision()) != 0 {
				t.Fatalf("got revision %x, want none", sample.GetRevision())
			}
		})

		t.Run("it panics if the metric isn't in the list", func(t *testing.T) {
			mustPanic(t, func() {
				manifest.NewSample(nil, new(metricsgrpc.QueueCommandsPending))
			})
		})
	})
}

// mustPanic fails the test if fn doesn't panic.
func mustPanic(t testing.TB, fn func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatal("expected a panic")
		}
	}()

	fn()
}
