package projection_test

import (
	"context"
	"database/sql"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

func TestHandlersAreInvokedConcurrentlyWhenConcurrencyPreferenceIsMaximize(t *testing.T) {
	// barrier is used to prove concurrency: one handler sends, the other
	// receives. If both handlers are not running concurrently, the send blocks
	// forever and the test times out.
	barrier := make(chan struct{})

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.Transact(t, engine.DB, func(tx *sql.Tx) {
				xtesting.PopulateEventStreams(
					t,
					tx,
					func(*uuidpb.UUID, uint64) dogma.Event {
						return &stubs.EventStub[stubs.TypeA]{}
					},
					1, 1, // 2 streams, 1 event each
				)
			})
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

func TestHandlersAreNotInvokedConcurrentlyWhenConcurrencyPreferenceIsMinimize(t *testing.T) {
	const handlerKey = "b2c3d4e5-6f7a-4b8c-9d0e-1f2a3b4c5d6e"

	var concurrent atomic.Int32

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.Transact(t, engine.DB, func(tx *sql.Tx) {
				xtesting.PopulateEventStreams(
					t,
					tx,
					func(*uuidpb.UUID, uint64) dogma.Event {
						return &stubs.EventStub[stubs.TypeA]{}
					},
					10, 10, // 2 streams, 10 events each
				)
			})

			xtesting.ExpectNoUnconsumedEventsEventually(
				t,
				engine.DB,
				handlerKey,
			)
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

					return s.Offset() + 1, nil
				},
			},
		),
	)
}
