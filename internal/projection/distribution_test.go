package projection_test

import (
	"context"
	"database/sql"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
)

// TestDistribution_eachEventIsHandledOnceAfterTheClusterConverges verifies
// that, once the nodes in a cluster have converged, each event is handled
// exactly once, even though the streams are consumed by different nodes.
func TestDistribution_eachEventIsHandledOnceAfterTheClusterConverges(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	handler := &countingProjection{}

	xtesting.StartCluster(t, db, handler.App(), 3)

	populateStreams(t, db, 10, 10)
	handler.ExpectHandledEventually(t, 100, xtesting.WaitTimeout)

	handler.ExpectEachEventHandledOnce(t)
}

// TestDistribution_eachEventIsHandledOnceWhenANodeJoins verifies that when a
// node joins the cluster, the streams that it becomes responsible for are
// handed over to it without dropping or duplicating any events.
func TestDistribution_eachEventIsHandledOnceWhenANodeJoins(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	handler := &countingProjection{}

	cluster := xtesting.StartCluster(t, db, handler.App(), 2)

	// With 30 streams, the new node is all but certain to become responsible
	// for at least one of them.
	streamIDs := populateStreams(t, db, 30, 3)
	handler.ExpectHandledEventually(t, 90, xtesting.WaitTimeout)

	cluster.AddNode(t)

	appendToEachStream(t, db, streamIDs, 3)
	handler.ExpectHandledEventually(t, 180, xtesting.WaitTimeout)

	handler.ExpectEachEventHandledOnce(t)
}

// TestDistribution_streamsOfAStoppedNodeAreConsumedByTheOthers verifies that
// when a node stops, the streams that it was responsible for are consumed by
// the remaining nodes once they see that it has left the cluster.
func TestDistribution_streamsOfAStoppedNodeAreConsumedByTheOthers(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	handler := &countingProjection{}

	cluster := xtesting.StartCluster(t, db, handler.App(), 3)

	// With 30 streams, the stopped node is all but certain to have been
	// responsible for at least one of them.
	streamIDs := populateStreams(t, db, 30, 3)
	handler.ExpectHandledEventually(t, 90, xtesting.WaitTimeout)

	cluster.StopNode(cluster.Nodes()[1])

	appendToEachStream(t, db, streamIDs, 3)

	// A stopped node does not announce that it is leaving, so the others only
	// see that it has left once it misses its heartbeats, which takes up to
	// the heartbeat timeout plus one heartbeat interval.
	handler.ExpectHandledEventually(t, 180, 6*time.Second+xtesting.WaitTimeout)

	handler.ExpectEachEventHandledOnce(t)
}

// populateStreams creates streamCount event streams, each containing
// eventCount events, and returns their IDs.
func populateStreams(
	t testing.TB,
	db *sql.DB,
	streamCount int,
	eventCount uint64,
) []*uuidpb.UUID {
	t.Helper()

	counts := make([]uint64, streamCount)
	for i := range counts {
		counts[i] = eventCount
	}

	return xtesting.PopulateEventStreams(
		t,
		db,
		func(*uuidpb.UUID, uint64) dogma.Event {
			return stubs.EventA1
		},
		counts...,
	)
}

// appendToEachStream appends eventCount events to each of the given streams.
func appendToEachStream(
	t testing.TB,
	db *sql.DB,
	streamIDs []*uuidpb.UUID,
	eventCount int,
) {
	t.Helper()

	for range eventCount {
		for _, streamID := range streamIDs {
			xtesting.AppendToEventStream(t, db, streamID, stubs.EventA1)
		}
	}
}

// countingProjection is a projection that records how many times it handles
// each event. A duplicate delivery that its optimistic concurrency control
// rejects is not handled, so it is not counted.
type countingProjection struct {
	checkpoints checkpointStore

	m       sync.Mutex
	handled map[string]int
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
	return p.checkpoints.HandleEvent(
		s,
		func() error {
			p.m.Lock()
			defer p.m.Unlock()

			if p.handled == nil {
				p.handled = map[string]int{}
			}

			key := fmt.Sprintf("%s@%d", s.StreamID(), s.Offset())
			p.handled[key]++

			return nil
		},
	)
}

// ExpectHandledEventually waits for the projection to have handled n distinct
// events, and fails the test if that takes longer than timeout.
func (p *countingProjection) ExpectHandledEventually(
	t testing.TB,
	n int,
	timeout time.Duration,
) {
	t.Helper()

	deadline := time.After(timeout)

	for {
		p.m.Lock()
		handled := len(p.handled)
		p.m.Unlock()

		if handled >= n {
			return
		}

		select {
		case <-deadline:
			t.Fatalf("timeout waiting for events to be handled: %d of %d", handled, n)
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// ExpectEachEventHandledOnce asserts that the projection handled no event more
// than once, after allowing a short time for any duplicate handling to occur.
func (p *countingProjection) ExpectEachEventHandledOnce(t testing.TB) {
	t.Helper()

	time.Sleep(200 * time.Millisecond)

	p.m.Lock()
	defer p.m.Unlock()

	for key, n := range p.handled {
		if n != 1 {
			t.Errorf("event %s was handled %d times", key, n)
		}
	}
}
