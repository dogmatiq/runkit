package projection_test

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestConcurrency_handlerIsInvokedConcurrentlyWithMaximizeConcurrencyPreference
// verifies that a handler with the MaximizeConcurrency preference handles
// events concurrently.
func TestConcurrency_handlerIsInvokedConcurrentlyWithMaximizeConcurrencyPreference(t *testing.T) {
	// barrier is used to prove concurrency: one invocation sends, the other
	// receives. If the handler is not invoked concurrently, the send blocks
	// forever and the test times out.
	barrier := make(chan struct{})

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				1, 1, // 2 streams, 1 event each
			)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "a1b2c3d4-5e6f-4a7b-8c9d-0e1f2a3b4c5d")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
					c.ConcurrencyPreference(dogma.MaximizeConcurrency)
				},
				HandleEventFunc: func(
					ctx context.Context,
					s dogma.ProjectionEventScope,
					_ dogma.Event,
				) (uint64, error) {
					select {
					case <-ctx.Done():
						return 0, ctx.Err()
					case barrier <- struct{}{}:
						return s.Offset() + 1, nil
					case <-barrier:
						return s.Offset() + 1, nil
					}
				},
			},
		),
	)
}

// TestConcurrency_handlerIsNotInvokedConcurrentlyWithMinimizeConcurrencyPreference
// verifies that a handler with the MinimizeConcurrency preference handles
// events serially.
func TestConcurrency_handlerIsNotInvokedConcurrentlyWithMinimizeConcurrencyPreference(t *testing.T) {
	const handlerKey = "ca363143-d35b-4e71-81f1-2ecd3458ae59"

	const eventCount = 20

	var (
		concurrent  atomic.Int32
		checkpoints checkpointStore
		handled     atomic.Int32
		done        cosyne.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, cluster *xtesting.Cluster, db *sql.DB) {
			xtesting.PopulateEventStreams(
				t,
				db,
				func(*uuidpb.UUID, uint64) dogma.Event {
					return stubs.EventA1
				},
				eventCount/2, eventCount/2, // 2 streams
			)

			xtesting.WaitForLatches(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", handlerKey)
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
					c.ConcurrencyPreference(dogma.MinimizeConcurrency)
				},
				HandleEventFunc: func(
					_ context.Context,
					s dogma.ProjectionEventScope,
					_ dogma.Event,
				) (uint64, error) {
					n := concurrent.Add(1)
					defer concurrent.Add(-1)

					if n > 1 {
						t.Errorf("handler invoked concurrently: %d simultaneous calls", n)
					}

					// Hold the handler open long enough for a concurrent
					// dispatch to be observable.
					time.Sleep(5 * time.Millisecond)

					return checkpoints.HandleEvent(
						s,
						func() error {
							if handled.Add(1) == eventCount {
								done.Set()
							}
							return nil
						},
					)
				},
				CheckpointOffsetFunc: checkpoints.CheckpointOffset,
			},
		),
	)
}
