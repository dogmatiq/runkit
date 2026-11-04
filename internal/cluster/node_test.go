package cluster_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestNode(t *testing.T) {
	const (
		interval = 20 * time.Millisecond
		timeout  = 5 * interval
	)

	// setup returns a signal bus that is listening for signals on a new
	// database.
	//
	// Some tests rely on a node's first heartbeat being received, and a signal
	// sent before the bus is listening is lost.
	setup := func(t *testing.T) *signal.Bus {
		signals := &signal.Bus{
			DB:     xtesting.NewDatabase(t),
			Logger: spruce.NewTestLogger(t),
		}

		// Once the bus is listening it signals every watcher, whatever its
		// topic. The first signal is delivered as soon as watching begins.
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		listening := signals.Watch(ctx, "test", 2)

		xtesting.GoE(t, signals.Run)
		xtesting.ExpectToReceiveN(t, "signal bus to start listening", listening, "", "")

		return signals
	}

	newNode := func(
		t *testing.T,
		signals *signal.Bus,
		interval, timeout time.Duration,
	) *Node {
		return &Node{
			ID:       uuidpb.Generate(),
			Signals:  signals,
			Interval: interval,
			Timeout:  timeout,
			Logger:   spruce.NewTestLogger(t),
		}
	}

	t.Run("it sends heartbeats on a topic named after the node", func(t *testing.T) {
		signals := setup(t)

		heartbeats := signals.Watch(t.Context(), "heartbeat", 10)

		n := newNode(t, signals, interval, timeout)
		xtesting.GoE(t, n.Run)

		// The first signals carry no node ID, as they are delivered when
		// watching begins and when the bus starts; after that, every signal
		// is a heartbeat from this node.
		want := n.ID.AsString()

		for i := 0; i < 3; {
			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for heartbeat")
			case got := <-heartbeats:
				if got == "" {
					continue
				}

				if got != want {
					t.Fatalf("unexpected heartbeat: got %q, want %q", got, want)
				}

				i++
			}
		}
	})

	t.Run("it keeps running when a heartbeat can not be sent", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		db.Close()

		signals := &signal.Bus{
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		n := newNode(t, signals, interval, timeout)

		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		// Run must not return until ctx is canceled, despite every attempt
		// to send a heartbeat failing.
		err := n.Run(ctx)

		if ctx.Err() == nil {
			t.Fatal("member stopped before context was canceled")
		}

		if !errors.Is(err, ctx.Err()) {
			t.Fatalf("unexpected error: got %v, want %v", err, ctx.Err())
		}
	})

	t.Run("it replies to a node that joins", func(t *testing.T) {
		signals := setup(t)

		heartbeats := signals.Watch(t.Context(), "heartbeat", 10)
		xtesting.ExpectToReceiveN(t, "initial signal", heartbeats, "")

		// The interval is long enough that any heartbeat after the first is
		// sent in reply to a joining node.
		n := newNode(t, signals, time.Hour, 2*time.Hour)
		xtesting.GoE(t, n.Run)

		want := n.ID.AsString()
		xtesting.ExpectToReceiveN(t, "first heartbeat", heartbeats, want)

		joining := uuidpb.Generate().AsString()
		if err := signals.Send(t.Context(), "heartbeat."+joining); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(t, "heartbeats", heartbeats, joining, want)
		xtesting.ExpectNoReceive(t, "reply to a known node", heartbeats)

		if err := signals.Send(t.Context(), "heartbeat."+joining); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(t, "heartbeats", heartbeats, joining)
		xtesting.ExpectNoReceive(t, "reply to a known node", heartbeats)
	})

	t.Run("it tracks nodes as they start and stop", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		xtesting.GoE(t, n.Run)

		expectPeers(t, n)

		node1 := newNode(t, signals, interval, timeout)
		stopNode1 := xtesting.GoE(t, node1.Run)

		expectPeers(t, n, node1.ID)

		node2 := newNode(t, signals, interval, timeout)
		stopNode2 := xtesting.GoE(t, node2.Run)

		expectPeers(t, n, node1.ID, node2.ID)

		stopNode1()

		expectPeers(t, n, node2.ID)

		stopNode2()

		expectPeers(t, n)
	})

	// start runs n and returns a function that stops it.
	start := func(t *testing.T, n *Node) func() {
		return xtesting.GoE(t, n.Run)
	}

	// rejoin starts a new node with the same ID as n, as though n had
	// restarted.
	rejoin := func(t *testing.T, signals *signal.Bus, n *Node) {
		r := newNode(t, signals, interval, timeout)
		r.ID = n.ID
		start(t, r)
	}

	t.Run("it reports whether this node is responsible for a workload", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		workload := workloadFor(t, other, n)

		responsible, unobserve := n.ObserveResponsibility(workload)
		defer unobserve()

		expectResponsible(t, responsible, false)

		stopOther()
		expectResponsible(t, responsible, true)

		rejoin(t, signals, other)
		expectResponsible(t, responsible, false)
	})

	t.Run("it stops reporting changes in responsibility once unobserved", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		workload := workloadFor(t, other, n)
		stopOther()

		responsible, unobserve := n.ObserveResponsibility(workload)
		expectResponsible(t, responsible, true)

		unobserve()

		rejoin(t, signals, other)
		expectPeers(t, n, other.ID)

		if got, _ := responsible(); !got {
			t.Fatal("responsibility changed after unobserving")
		}
	})

	t.Run("it keeps reporting changes to a workload that is still observed elsewhere", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		workload := workloadFor(t, other, n)
		stopOther()

		_, unobserveFirst := n.ObserveResponsibility(workload)
		second, unobserveSecond := n.ObserveResponsibility(workload)
		defer unobserveSecond()

		expectResponsible(t, second, true)

		unobserveFirst()

		rejoin(t, signals, other)
		expectResponsible(t, second, false)
	})

	t.Run("it makes exactly one node responsible for each workload", func(t *testing.T) {
		signals := setup(t)

		nodes := []*Node{
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
		}

		for _, n := range nodes {
			xtesting.GoE(t, n.Run)
		}

		for _, n := range nodes {
			var want []*uuidpb.UUID
			for _, peer := range nodes {
				if peer != n {
					want = append(want, peer.ID)
				}
			}

			expectPeers(t, n, want...)
		}

		for range 20 {
			workload := uuidpb.Generate()

			var observers []func() (bool, <-chan struct{})
			for _, n := range nodes {
				responsible, unobserve := n.ObserveResponsibility(workload)
				defer unobserve()

				observers = append(observers, responsible)
			}

			expectExactlyOneResponsible(t, workload, observers)
		}
	})

	t.Run("it learns about existing nodes without waiting for their next heartbeat", func(t *testing.T) {
		signals := setup(t)

		// The interval and timeout are long enough that no heartbeat is sent
		// during the test other than each node's first and its replies to a
		// joining node, and no node times out.
		existing := newNode(t, signals, time.Hour, 2*time.Hour)
		xtesting.GoE(t, existing.Run)

		// Wait for the existing node to hear its own heartbeat, at which point
		// it is responsible for any workload, being the only live node.
		responsible, unobserve := existing.ObserveResponsibility(uuidpb.Generate())
		defer unobserve()
		expectResponsible(t, responsible, true)

		joining := newNode(t, signals, time.Hour, 2*time.Hour)
		xtesting.GoE(t, joining.Run)

		expectPeers(t, joining, existing.ID)
		expectPeers(t, existing, joining.ID)
	})
}

func expectPeers(
	t testing.TB,
	n *Node,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := n.Peers()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected peers: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}

func expectResponsible(
	t testing.TB,
	responsible func() (bool, <-chan struct{}),
	want bool,
) {
	t.Helper()

	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := responsible()

		if got == want {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected responsibility: got %t, want %t", got, want)
		case <-changed:
		}
	}
}

func expectExactlyOneResponsible(
	t testing.TB,
	workload *uuidpb.UUID,
	observers []func() (bool, <-chan struct{}),
) {
	t.Helper()

	timeout := time.After(xtesting.WaitTimeout)

	for {
		count := 0
		for _, responsible := range observers {
			if ok, _ := responsible(); ok {
				count++
			}
		}

		if count == 1 {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("expected exactly one node to be responsible for workload %s, got %d", workload, count)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

// workloadFor returns a workload that the responsible node is responsible for
// while it and the other nodes are live. All of the nodes must be running, and
// no others.
func workloadFor(
	t testing.TB,
	responsible *Node,
	others ...*Node,
) *uuidpb.UUID {
	t.Helper()

	nodes := append([]*Node{responsible}, others...)

	for _, n := range nodes {
		var peers []*uuidpb.UUID
		for _, p := range nodes {
			if p != n {
				peers = append(peers, p.ID)
			}
		}

		expectPeers(t, n, peers...)
	}

	for {
		workload := uuidpb.Generate()

		var observers []func() (bool, <-chan struct{})
		var unobservers []func()

		for _, n := range nodes {
			o, unobserve := n.ObserveResponsibility(workload)
			observers = append(observers, o)
			unobservers = append(unobservers, unobserve)
		}

		expectExactlyOneResponsible(t, workload, observers)
		ok, _ := observers[0]()

		for _, unobserve := range unobservers {
			unobserve()
		}

		if ok {
			return workload
		}
	}
}
