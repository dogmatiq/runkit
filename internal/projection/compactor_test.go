package projection_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	dogmaengine "github.com/dogmatiq/reference-engine"
	"github.com/dogmatiq/reference-engine/internal/x/xtesting"
)

// TestCompaction_handlerIsCalledPeriodically verifies that the engine
// periodically invokes Compact() on a projection handler.
func TestCompaction_handlerIsCalledPeriodically(t *testing.T) {
	var (
		calls  atomic.Int32
		called xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExpectLatchesSetEventually(t, &called)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "c1a2b3c4-d5e6-4f7a-8b9c-0d1e2f3a4b5c")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				CompactFunc: func(context.Context, dogma.ProjectionCompactScope) error {
					if calls.Add(1) >= 2 {
						called.Set()
					}
					return nil
				},
			},
		),
	)
}

// TestCompaction_handlerContinuesAfterCompactionFailure verifies that event
// handling continues even when Compact() returns an error or panics.
func TestCompaction_handlerContinuesAfterCompactionFailure(t *testing.T) {
	const handlerKey = "d2b3c4d5-e6f7-4a8b-9c0d-1e2f3a4b5c6d"

	cases := []struct {
		Name        string
		CompactFunc func(context.Context, dogma.ProjectionCompactScope) error
	}{
		{
			"returns error",
			func(context.Context, dogma.ProjectionCompactScope) error {
				return errors.New("<compact error>")
			},
		},
		{
			"panics",
			func(context.Context, dogma.ProjectionCompactScope) error {
				panic("<compact panic>")
			},
		},
	}

	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			var compacted xsync.Latch

			xtesting.RunEngines(
				t,
				func(t testing.TB, engine *dogmaengine.Engine) {
					xtesting.ExpectLatchesSetEventually(t, &compacted)

					xtesting.PopulateEventStreams(
						t,
						engine.DB,
						func(*uuidpb.UUID, uint64) dogma.Event {
							return stubs.EventA1
						},
						10,
					)

					xtesting.WaitForHandlerToConsumeAllEvents(t, engine.DB, handlerKey)
				},
				dogma.ViaProjection(
					&stubs.ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<handler>", handlerKey)
							c.Routes(
								dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							)
						},
						CompactFunc: func(ctx context.Context, s dogma.ProjectionCompactScope) error {
							defer compacted.Set()
							return c.CompactFunc(ctx, s)
						},
					},
				),
			)
		})
	}
}

// TestCompaction_handlerIsNotInvokedConcurrently verifies that Compact() is
// never invoked concurrently, even across multiple engine instances.
func TestCompaction_handlerIsNotInvokedConcurrently(t *testing.T) {
	var (
		concurrent atomic.Int32
		calls      atomic.Int32
		called     xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			xtesting.ExpectLatchesSetEventually(t, &called)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "e3c4d5e6-f7a8-4b9c-0d1e-2f3a4b5c6d7e")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				CompactFunc: func(context.Context, dogma.ProjectionCompactScope) error {
					n := concurrent.Add(1)
					defer concurrent.Add(-1)

					if n > 1 {
						t.Errorf("compact invoked concurrently: %d simultaneous calls", n)
					}

					// Hold the handler open long enough for a concurrent
					// dispatch to be observable.
					time.Sleep(5 * time.Millisecond)

					// Wait for enough serial calls to be confident that all
					// engine instances had a chance to overlap.
					if calls.Add(1) >= 10 {
						called.Set()
					}

					return nil
				},
			},
		),
	)
}

// TestCompaction_intervalIsRespectedAcrossEngineInstances verifies that
// multiple engine instances collectively respect the compaction interval,
// rather than each instance compacting independently at full rate.
func TestCompaction_intervalIsRespectedAcrossEngineInstances(t *testing.T) {
	var (
		m               sync.Mutex
		compactInterval time.Duration
		lastCompactedAt time.Time
		calls           int
		done            xsync.Latch
	)

	xtesting.RunEngines(
		t,
		func(t testing.TB, engine *dogmaengine.Engine) {
			m.Lock()
			compactInterval = engine.ProjectionCompactInterval
			m.Unlock()

			if compactInterval <= 0 {
				panic("engine did not set a projection compact interval")
			}

			xtesting.ExpectLatchesSetEventually(t, &done)
		},
		dogma.ViaProjection(
			&stubs.ProjectionMessageHandlerStub{
				ConfigureFunc: func(c dogma.ProjectionConfigurer) {
					c.Identity("<handler>", "f4d5e6f7-a8b9-4c0d-1e2f-3a4b5c6d7e8f")
					c.Routes(
						dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
					)
				},
				CompactFunc: func(context.Context, dogma.ProjectionCompactScope) error {
					m.Lock()
					defer m.Unlock()

					if compactInterval == 0 {
						// Compact happened to run before we captured the
						// interval from the engine, ignore this call entirely.
						return nil
					}

					if time.Since(lastCompactedAt) < compactInterval {
						t.Errorf("compaction called too soon: gap %v, want >= %v", time.Since(lastCompactedAt), compactInterval)
					}

					calls++
					if calls == 5 {
						done.Set()
					}

					lastCompactedAt = time.Now()

					return nil
				},
			},
		),
	)
}
