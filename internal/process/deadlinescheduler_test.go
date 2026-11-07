package process_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
)

// TestDeadlineScheduler_ownershipAndFailover verifies rendezvous assignment per
// instance and delivery by another node after the assigned node stops.
func TestDeadlineScheduler_ownershipAndFailover(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, _ := newSinkWithDeadlines(t, db)
	const count = 30

	for i := range count {
		queueDeadline(t, db, sink, fmt.Sprint(i), time.Now())
	}

	// Hold the due work back until both membership views have converged.
	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE process.deadlines SET
			deliver_at = $1`,
		time.Now().Add(time.Hour),
	); err != nil {
		t.Fatal(err)
	}

	var mu sync.Mutex
	handled := map[string]int{}
	owner := map[string]int{}
	var nodes []*cluster.Node
	var stops []func()

	for index := range 2 {
		copy := *sink
		copy.Handler = &stubs.ProcessMessageHandlerStub[dogma.ProcessRoot]{
			NewFunc: func() dogma.ProcessRoot {
				return &stubs.ProcessRootStub{}
			},
			HandleDeadlineFunc: func(
				_ context.Context,
				_ dogma.ProcessRoot,
				s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
				_ dogma.Deadline,
			) error {
				mu.Lock()
				handled[s.InstanceID()]++
				owner[s.InstanceID()] = index
				mu.Unlock()
				s.ExecuteCommand(stubs.CommandX1)

				return nil
			},
		}
		node, stop := startDeadlineScheduler(t, &copy)
		nodes = append(nodes, node)
		stops = append(stops, stop)
	}

	xtesting.WaitFor(t, "deadline nodes converge", func() bool {
		views := make([]*cluster.Membership, len(nodes))

		for i, node := range nodes {
			views[i], _ = node.Membership().Load()

			if views[i] == nil || views[i].Peers().Len() != 1 {
				return false
			}
		}

		owns := make([]bool, len(nodes))

		for i := range count {
			workload := uuidpb.Derive(sink.Identity.GetKey(), fmt.Sprint(i))
			owners := 0

			for j, view := range views {
				if view.LocalNodeIsResponsibleFor(workload) {
					owns[j] = true
					owners++
				}
			}

			if owners != 1 {
				return false
			}
		}

		return owns[0] && owns[1]
	})

	if _, err := db.ExecContext(
		t.Context(),
		`UPDATE process.deadlines SET
			deliver_at = $1`,
		time.Now(),
	); err != nil {
		t.Fatal(err)
	}

	xtesting.WaitForQueryResult(
		t,
		"all deadlines committed",
		count,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
	mu.Lock()

	for i := range count {
		instanceID := fmt.Sprint(i)
		workload := uuidpb.Derive(sink.Identity.GetKey(), instanceID)

		if handled[instanceID] != 1 {
			t.Errorf("unexpected delivery count for %s: %d", instanceID, handled[instanceID])
		}

		m, _ := nodes[owner[instanceID]].Membership().Load()

		if !m.LocalNodeIsResponsibleFor(workload) {
			t.Errorf("deadline for %s was handled by the wrong node", instanceID)
		}
	}

	mu.Unlock()

	// Find work assigned to node zero, stop it, then enqueue that work. The
	// remaining node must wake on membership changes as well as notifications.
	m, _ := nodes[0].Membership().Load()
	var instanceID string

	for i := count; ; i++ {
		instanceID = fmt.Sprint(i)

		if m.LocalNodeIsResponsibleFor(uuidpb.Derive(sink.Identity.GetKey(), instanceID)) {
			break
		}
	}

	stops[0]()
	queueDeadline(t, db, sink, instanceID, time.Now())
	xtesting.WaitForQueryResult(
		t,
		"deadline delivered after owner departure",
		count+1,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
	mu.Lock()
	defer mu.Unlock()

	if handled[instanceID] != 1 || owner[instanceID] != 1 {
		t.Fatalf("unexpected failover delivery: calls=%d, owner=%d", handled[instanceID], owner[instanceID])
	}
}

// TestDeadlineScheduler_earlierDeadlineWakesDiscovery verifies that newly
// scheduled work need not wait for an existing far-future deadline's timer.
func TestDeadlineScheduler_earlierDeadlineWakesDiscovery(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	handler.HandleDeadlineFunc = func(
		_ context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		s.ExecuteCommand(stubs.CommandX1)

		return nil
	}
	queueDeadline(t, db, sink, "<later>", time.Now().Add(time.Hour))
	node, _ := startDeadlineScheduler(t, sink)
	xtesting.WaitFor(t, "deadline node is live", func() bool {
		m, _ := node.Membership().Load()

		return m.LocalNodeIsResponsibleFor(node.ID)
	})
	queueDeadline(t, db, sink, "<earlier>", time.Now())
	xtesting.WaitForQueryResult(
		t,
		"earlier deadline committed",
		1,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
	xtesting.ExpectQueryResult(
		t,
		"future deadline remains pending",
		1,
		db,
		`SELECT COUNT(*)
		FROM process.deadlines
		WHERE instance_id = '<later>'`,
	)
}

// TestDeadlineScheduler_membershipChangeDoesNotCancelDelivery verifies that a
// delivery can commit after the local node loses ownership of its workload.
func TestDeadlineScheduler_membershipChangeDoesNotCancelDelivery(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	queueDeadline(t, db, sink, "<instance>", time.Now())

	started := make(chan struct{})
	release := make(chan struct{})
	handler.HandleDeadlineFunc = func(
		ctx context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		close(started)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-release:
			s.ExecuteCommand(stubs.CommandX1)

			return nil
		}
	}

	ctx, cancel := context.WithCancel(t.Context())
	nodeCtx, stopNode := context.WithCancel(ctx)
	logger := spruce.NewTestLogger(t)
	bus := &signal.Bus{DB: db, Logger: logger}
	node := &cluster.Node{
		ID:       uuidpb.Generate(),
		Signals:  bus,
		Interval: 25 * time.Millisecond,
		Timeout:  time.Second,
		Logger:   logger,
	}
	scheduler := &process.DeadlineScheduler{
		Node:    node,
		Signals: bus,
		Sinks:   map[*process.Sink]int{sink: 2},
		Logger:  logger,
	}

	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return bus.Run(ctx)
	})
	group.Go(func() error {
		err := node.Run(nodeCtx)
		if errors.Is(err, context.Canceled) {
			return nil
		}

		return err
	})
	group.Go(func() error {
		return scheduler.Run(ctx)
	})
	t.Cleanup(func() {
		cancel()
		stopNode()

		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})

	xtesting.WaitFor(t, "deadline delivery starts", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})

	// Stop membership participation without stopping the scheduler. This
	// removes all local assignments while the handler is still running.
	stopNode()
	xtesting.WaitFor(t, "local node loses ownership", func() bool {
		m, _ := node.Membership().Load()

		return !m.LocalNodeIsResponsibleFor(uuidpb.Derive(sink.Identity.GetKey(), "<instance>"))
	})
	close(release)

	xtesting.WaitForQueryResult(
		t,
		"in-flight delivery commits after ownership is lost",
		1,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
}

// startDeadlineScheduler starts a cluster node and deadline scheduler.
//
// It returns a function that stops both, which is safe to call repeatedly.
func startDeadlineScheduler(
	t testing.TB,
	sink *process.Sink,
) (*cluster.Node, func()) {
	t.Helper()

	ctx, cancel := context.WithCancel(t.Context())
	logger := spruce.NewTestLogger(t)
	bus := &signal.Bus{DB: sink.DB, Logger: logger}
	node := &cluster.Node{
		ID:       uuidpb.Generate(),
		Signals:  bus,
		Interval: 25 * time.Millisecond,
		Timeout:  time.Second,
		Logger:   logger,
	}
	scheduler := &process.DeadlineScheduler{
		Node:    node,
		Signals: bus,
		Sinks:   map[*process.Sink]int{sink: 2},
		Logger:  logger,
	}
	group, ctx := errgroup.WithContext(ctx)
	group.Go(func() error {
		return bus.Run(ctx)
	})
	group.Go(func() error {
		return node.Run(ctx)
	})
	group.Go(func() error {
		return scheduler.Run(ctx)
	})
	stop := sync.OnceFunc(func() {
		cancel()

		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})
	t.Cleanup(stop)

	return node, stop
}
