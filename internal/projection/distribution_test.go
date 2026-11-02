package projection_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestDistribution_eachEventIsHandledOnceAfterTheClusterConverges verifies
// that, once the nodes in a cluster have converged, each event is passed to the
// handler exactly once, even though the streams are consumed by different
// nodes.
func TestDistribution_eachEventIsHandledOnceAfterTheClusterConverges(t *testing.T) {
	t.Parallel()

	const (
		streamCount = 10
		eventCount  = 10
	)

	db := xtesting.NewDatabase(t)
	handler := &countingProjection{
		Want: streamCount * eventCount,
	}

	xtesting.StartCluster(t, db, handler.App(), 3)

	counts := make([]uint64, streamCount)
	for i := range counts {
		counts[i] = eventCount
	}

	xtesting.PopulateEventStreams(
		t,
		db,
		func(*uuidpb.UUID, uint64) dogma.Event {
			return stubs.EventA1
		},
		counts...,
	)

	xtesting.ExpectLatchesSetEventually(t, &handler.Done)

	// Allow time for any duplicate delivery to occur.
	time.Sleep(200 * time.Millisecond)

	handler.ExpectEachEventHandledOnce(t)
}

// countingProjection is a projection that records how many times each event is
// passed to it.
type countingProjection struct {
	// Want is the number of distinct events after which Done is set.
	Want int

	// Done is set once Want distinct events have been handled.
	Done xsync.Latch

	checkpoints checkpointStore

	m     sync.Mutex
	calls map[string]int
}

// App returns an application that consists only of the projection, which
// prefers to maximize concurrency so that its streams are distributed across
// the nodes in the cluster.
func (p *countingProjection) App() dogma.Application {
	return &stubs.ApplicationStub{
		ConfigureFunc: func(c dogma.ApplicationConfigurer) {
			c.Identity("<app>", "4f0b9a1e-6c2d-4b8a-9e3f-7d5c1a2b3e4f")
			c.Routes(
				dogma.ViaProjection(
					&stubs.ProjectionMessageHandlerStub{
						ConfigureFunc: func(c dogma.ProjectionConfigurer) {
							c.Identity("<projection>", "a7c3e5f1-9b2d-4e6a-8c0f-2d4b6a8e0c1f")
							c.Routes(
								dogma.HandlesEvent[*stubs.EventStub[stubs.TypeA]](),
							)
							c.ConcurrencyPreference(dogma.MaximizeConcurrency)
						},
						HandleEventFunc:      p.handleEvent,
						CheckpointOffsetFunc: p.checkpoints.CheckpointOffset,
					},
				),
			)
		},
	}
}

func (p *countingProjection) handleEvent(
	_ context.Context,
	s dogma.ProjectionEventScope,
	_ dogma.Event,
) (uint64, error) {
	p.m.Lock()
	defer p.m.Unlock()

	if p.calls == nil {
		p.calls = map[string]int{}
	}

	key := fmt.Sprintf("%s@%d", s.StreamID(), s.Offset())
	p.calls[key]++

	if len(p.calls) == p.Want {
		p.Done.Set()
	}

	return p.checkpoints.HandleEvent(s, nil)
}

// ExpectEachEventHandledOnce asserts that no event was passed to the
// projection more than once.
func (p *countingProjection) ExpectEachEventHandledOnce(t testing.TB) {
	t.Helper()

	p.m.Lock()
	defer p.m.Unlock()

	for key, n := range p.calls {
		if n != 1 {
			t.Errorf("event %s was passed to the handler %d times", key, n)
		}
	}
}
