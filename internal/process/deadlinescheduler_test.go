package process_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

// TestDeadlineScheduler_earlierDeadlineWakesDiscovery verifies that new work
// can progress while another instance is blocked and a future deadline is pending.
func TestDeadlineScheduler_earlierDeadlineWakesDiscovery(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	sink.HandleTimeout = 2 * xtesting.WaitTimeout

	started := make(chan struct{})
	release := make(chan struct{})
	handler.HandleDeadlineFunc = func(
		ctx context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		if s.InstanceID() == "<blocked>" {
			close(started)

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
				return nil
			}
		}

		s.ExecuteCommand(stubs.CommandX1)

		return nil
	}

	node, _ := startDeadlineScheduler(t, sink)
	xtesting.WaitFor(t, "deadline node is live", func() bool {
		m, _ := node.Membership().Load()

		return m.LocalNodeIsResponsibleFor(node.ID)
	})

	queueDeadline(t, db, sink, "<later>", time.Now().Add(time.Hour))
	queueDeadline(t, db, sink, "<blocked>", time.Now())
	xtesting.WaitFor(t, "barrier deadline starts", func() bool {
		select {
		case <-started:
			return true
		default:
			return false
		}
	})

	// The discovery query that found the blocked instance predates this
	// insertion. Its delivery cannot provide a completion wake-up while the
	// new deadline is waiting, and its handler timeout exceeds this wait.
	queueDeadline(t, db, sink, "<earlier>", time.Now())
	xtesting.WaitForQueryResult(
		t,
		"newly scheduled deadline committed",
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

	close(release)
}

// TestDeadlineScheduler_concurrencyLimit verifies that two instances can make
// progress independently while further deliveries wait for capacity.
func TestDeadlineScheduler_concurrencyLimit(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	sink.HandleTimeout = 2 * xtesting.WaitTimeout

	var active atomic.Int32
	started := make(chan string, 3)
	releases := map[string]chan struct{}{
		"<first>":  make(chan struct{}),
		"<second>": make(chan struct{}),
		"<third>":  make(chan struct{}),
	}
	handler.HandleDeadlineFunc = func(
		ctx context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		n := active.Add(1)
		defer active.Add(-1)

		if n > 2 {
			t.Errorf("concurrency limit exceeded: got %d active deliveries, want at most 2", n)
		}

		started <- s.InstanceID()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-releases[s.InstanceID()]:
			s.ExecuteCommand(stubs.CommandX1)

			return nil
		}
	}

	for id := range releases {
		queueDeadline(t, db, sink, id, time.Now())
	}

	// startDeadlineScheduler configures a concurrency limit of two.
	startDeadlineScheduler(t, sink)
	var ids []string
	xtesting.WaitFor(t, "two independent deliveries start", func() bool {
		select {
		case id := <-started:
			ids = append(ids, id)
		default:
		}

		return len(ids) == 2
	})

	// Observe the callback channel while both slots are occupied. This is a
	// bounded negative assertion, rather than a sleep followed by a snapshot
	// of an unsynchronized counter.
	timer := time.NewTimer(100 * time.Millisecond)
	defer timer.Stop()

	select {
	case id := <-started:
		t.Fatalf("delivery for %s started before capacity was released", id)
	case <-t.Context().Done():
		t.Fatal(t.Context().Err())
	case <-timer.C:
	}

	close(releases[ids[0]])
	var third string
	xtesting.WaitFor(t, "another delivery starts after capacity is released", func() bool {
		select {
		case third = <-started:
			return true
		default:
			return false
		}
	})

	if third == ids[0] || third == ids[1] {
		t.Fatalf("unexpected repeated delivery for %s", third)
	}

	if got := active.Load(); got != 2 {
		t.Errorf("unexpected active deliveries: got %d, want 2", got)
	}

	// The other original instance is still blocked when the third starts.
	// A completed instance therefore frees capacity independently of it.
	xtesting.ExpectQueryResult(
		t,
		"one delivery committed while two remain blocked",
		1,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)

	close(releases[ids[1]])
	close(releases[third])
	xtesting.WaitForQueryResult(
		t,
		"all three independent deliveries commit",
		3,
		db,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
}

// TestDeadlineScheduler_membershipChangeDoesNotCancelDelivery verifies that an
// in-flight delivery finishes after discovery observes its reassignment.
func TestDeadlineScheduler_membershipChangeDoesNotCancelDelivery(t *testing.T) {
	t.Parallel()

	db := xtesting.NewDatabase(t)
	sink, handler := newSinkWithDeadlines(t, db)
	sink.HandleTimeout = 2 * xtesting.WaitTimeout

	ctx, cancel := context.WithCancel(t.Context())
	logger := spruce.NewTestLogger(t)
	bus := &signal.Bus{DB: db, Logger: logger}
	node := &cluster.Node{
		ID:       uuidpb.Generate(),
		Signals:  bus,
		Interval: 25 * time.Millisecond,
		Timeout:  time.Second,
		Logger:   logger,
	}
	peerID := uuidpb.Generate()
	group, ctx := errgroup.WithContext(ctx)

	group.Go(func() error {
		return bus.Run(ctx)
	})
	group.Go(func() error {
		return node.Run(ctx)
	})

	// The peer participates in membership without delivering deadlines.
	// Restart it with the same ID so that assignment is the same after it rejoins.
	startPeer := func() context.CancelFunc {
		peerCtx, stop := context.WithCancel(ctx)
		peer := &cluster.Node{
			ID:       peerID,
			Signals:  bus,
			Interval: node.Interval,
			Timeout:  node.Timeout,
			Logger:   logger,
		}

		group.Go(func() error {
			err := peer.Run(peerCtx)
			if errors.Is(err, context.Canceled) {
				return nil
			}

			return err
		})

		return stop
	}
	stopPeer := startPeer()

	t.Cleanup(func() {
		cancel()

		if err := group.Wait(); err != nil && !errors.Is(err, context.Canceled) {
			t.Error(err)
		}
	})

	xtesting.WaitFor(t, "both nodes join", func() bool {
		m, _ := node.Membership().Load()

		return m != nil && m.Peers().Has(peerID)
	})

	// Choose one instance for each owner using the published membership,
	// then remove the peer so that this node initially owns both.
	m, _ := node.Membership().Load()
	var reassignedID, retainedID string

	for i := 0; reassignedID == "" || retainedID == ""; i++ {
		id := fmt.Sprint(i)
		workload := uuidpb.Derive(sink.Identity.GetKey(), id)

		if m.LocalNodeIsResponsibleFor(workload) {
			retainedID = id
		} else {
			reassignedID = id
		}
	}

	stopPeer()
	xtesting.WaitFor(t, "peer expires", func() bool {
		m, _ := node.Membership().Load()

		return m != nil && m.Peers().Len() == 0
	})

	started := make(chan context.Context, 1)
	release := make(chan struct{})
	retainedStarted := make(chan struct{})
	handler.HandleDeadlineFunc = func(
		ctx context.Context,
		_ dogma.ProcessRoot,
		s dogma.ProcessDeadlineScope[dogma.ProcessRoot],
		_ dogma.Deadline,
	) error {
		if s.InstanceID() == reassignedID {
			started <- ctx

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-release:
			}
		} else {
			close(retainedStarted)
		}

		s.ExecuteCommand(stubs.CommandX1)

		return nil
	}
	queueDeadline(t, db, sink, reassignedID, time.Now())

	scheduler := &process.DeadlineScheduler{
		Node:        node,
		Signals:     bus,
		Sinks:       []*process.Sink{sink},
		Concurrency: 2,
		Logger:      logger,
	}

	group.Go(func() error {
		return scheduler.Run(ctx)
	})

	var attemptCtx context.Context
	xtesting.WaitFor(t, "first delivery starts", func() bool {
		select {
		case attemptCtx = <-started:
			return true
		default:
			return false
		}
	})

	startPeer()
	xtesting.WaitFor(t, "instance is reassigned", func() bool {
		m, _ := node.Membership().Load()

		return m != nil && m.Peers().Has(peerID) &&
			!m.LocalNodeIsResponsibleFor(uuidpb.Derive(sink.Identity.GetKey(), reassignedID))
	})

	// This deadline is queued after the first discovery query and the membership
	// change. Handling it establishes that discovery has read the new view.
	queueDeadline(t, db, sink, retainedID, time.Now())
	xtesting.WaitFor(t, "retained instance starts", func() bool {
		select {
		case <-retainedStarted:
			return true
		default:
			return false
		}
	})

	if err := attemptCtx.Err(); err != nil {
		t.Fatalf("in-flight delivery was canceled after reassignment: %v", err)
	}

	close(release)
	xtesting.WaitForQueryResult(
		t,
		"both deliveries commit",
		2,
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
		Node:        node,
		Signals:     bus,
		Sinks:       []*process.Sink{sink},
		Concurrency: 2,
		Logger:      logger,
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
