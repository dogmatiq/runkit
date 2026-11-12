package cluster_test

import (
	"context"
	"errors"
	"fmt"
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
			DB:           xtesting.NewDatabase(t),
			EngineLogger: spruce.NewTestLogger(t),
		}

		// Once the bus is listening it signals every watcher, whatever its
		// topic. The first signal is delivered as soon as watching begins.
		listening, stop := signals.Watch(t.Context(), "test", 2)
		defer stop()

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
			ID:                uuidpb.Generate(),
			DB:                signals.DB,
			Signals:           signals,
			HeartbeatInterval: interval,
			HeartbeatTimeout:  timeout,
			EngineLogger:      spruce.NewTestLogger(t),
		}
	}

	t.Run("it sends heartbeats on a topic named after the node", func(t *testing.T) {
		signals := setup(t)

		heartbeats, stop := signals.Watch(t.Context(), "heartbeat", 10)
		defer stop()

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
			DB:           db,
			EngineLogger: spruce.NewTestLogger(t),
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

		heartbeats, stop := signals.Watch(t.Context(), "heartbeat", 10)
		defer stop()

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

	t.Run("it reports whether this node owns the workload with a key", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		key := keyFor(t, other, n)
		expectNodeOwnsWorkload(t, n, key, false)

		stopOther()
		expectNodeOwnsWorkload(t, n, key, true)

		rejoin(t, signals, other)
		expectNodeOwnsWorkload(t, n, key, false)
	})

	t.Run("it makes exactly one node the owner of the workload with each key", func(t *testing.T) {
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
			expectExactlyOneOwner(t, uuidpb.Generate(), nodes)
		}
	})

	t.Run("it doesn't own any workloads after it stops", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		stop := start(t, n)
		start(t, other)

		key := keyFor(t, n, other)
		expectNodeOwnsWorkload(t, n, key, true)

		stop()

		liveNodes, _ := n.LiveNodes().Load()

		if n.OwnsWorkload(key, liveNodes) {
			t.Fatal("stopped node owns a workload")
		}

		if !liveNodes.IsEmpty() {
			t.Fatalf("stopped node sees live nodes: %s", liveNodes)
		}
	})

	t.Run("it makes the node whose ID is the key the owner of the workload with that key", func(t *testing.T) {
		signals := setup(t)

		// If only scores counted, all four nodes would own the workloads whose
		// keys are their own IDs in only one run in 256.
		nodes := []*Node{
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		expectEachOther(t, nodes)

		for _, n := range nodes {
			if got := expectSameWorkloadOwner(t, n.ID, nodes); !got.Equal(n.ID) {
				t.Fatalf("unexpected owner of the workload with key %s: got %s", n.ID, got)
			}
		}
	})

	t.Run("it scores a key normally when no live node has that ID", func(t *testing.T) {
		signals := setup(t)

		nodes := []*Node{
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		stopped := newNode(t, signals, interval, timeout)
		stop := start(t, stopped)

		expectEachOther(t, append(nodes, stopped))
		stop()
		expectEachOther(t, nodes)

		if got := expectSameWorkloadOwner(t, stopped.ID, nodes); got.Equal(stopped.ID) {
			t.Fatal("stopped node owns the workload whose key is its own ID")
		}
	})

	t.Run("it returns the same workload owner on every node", func(t *testing.T) {
		signals := setup(t)

		nodes := []*Node{
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
			newNode(t, signals, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		expectEachOther(t, nodes)

		for range 20 {
			expectSameWorkloadOwner(t, uuidpb.Generate(), nodes)
		}
	})

	t.Run("it returns false when there are no live nodes", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		stop := start(t, n)

		key := uuidpb.Generate()
		expectNodeOwnsWorkload(t, n, key, true)

		stop()

		liveNodes, _ := n.LiveNodes().Load()

		if got, ok := FindWorkloadOwner(key, liveNodes); ok {
			t.Fatalf("unexpected workload owner: got %s", got)
		}

		if got, ok := FindWorkloadOwner(n.ID, liveNodes); ok {
			t.Fatalf("unexpected owner of the workload whose key is the stopped node's ID: got %s", got)
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
		// it owns every workload, being the only live node.
		expectNodeOwnsWorkload(t, existing, uuidpb.Generate(), true)

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

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the node to have peers %s", expected),
		n.LiveNodes(),
		func(liveNodes *uuidcollection.Set) bool {
			peers := uuidcollection.NewSet()

			for id := range liveNodes.All() {
				if !id.Equal(n.ID) {
					peers.Add(id)
				}
			}

			return peers.IsEqual(expected)
		},
	)
}

func expectNodeOwnsWorkload(
	t testing.TB,
	n *Node,
	key *uuidpb.UUID,
	want bool,
) {
	t.Helper()

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the node's ownership of the workload with key %s to be %t", key, want),
		n.LiveNodes(),
		func(liveNodes *uuidcollection.Set) bool {
			return n.OwnsWorkload(key, liveNodes) == want
		},
	)
}

func expectExactlyOneOwner(
	t testing.TB,
	key *uuidpb.UUID,
	nodes []*Node,
) {
	t.Helper()

	xtesting.WaitFor(
		t,
		fmt.Sprintf("exactly one node to own the workload with key %s", key),
		func() bool {
			return len(workloadOwners(key, nodes)) == 1
		},
	)
}

// workloadOwners returns the nodes that own the workload with the given key,
// according to their own memberships.
func workloadOwners(key *uuidpb.UUID, nodes []*Node) []*Node {
	var owners []*Node

	for _, n := range nodes {
		liveNodes, _ := n.LiveNodes().Load()

		if n.OwnsWorkload(key, liveNodes) {
			owners = append(owners, n)
		}
	}

	return owners
}

// keyFor returns the key of a workload that the given owner owns while it and
// the other nodes are live. All of the nodes must be running, and no others.
func keyFor(
	t testing.TB,
	owner *Node,
	others ...*Node,
) *uuidpb.UUID {
	t.Helper()

	nodes := append([]*Node{owner}, others...)

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
		key := uuidpb.Generate()

		if got := workloadOwners(key, nodes); len(got) == 1 && got[0] == owner {
			return key
		}
	}
}

// expectEachOther waits until each of the given nodes has the others, and no
// other nodes, as its peers.
func expectEachOther(t testing.TB, nodes []*Node) {
	t.Helper()

	for _, n := range nodes {
		var peers []*uuidpb.UUID
		for _, p := range nodes {
			if p != n {
				peers = append(peers, p.ID)
			}
		}

		expectPeers(t, n, peers...)
	}
}

// expectSameWorkloadOwner waits until every given node returns the same owner
// for the workload with the given key, and that node is the only one that
// reports that it owns the workload. It returns the owner's ID.
func expectSameWorkloadOwner(
	t testing.TB,
	key *uuidpb.UUID,
	nodes []*Node,
) *uuidpb.UUID {
	t.Helper()

	var owner *uuidpb.UUID

	xtesting.WaitFor(
		t,
		fmt.Sprintf("every node to agree on the owner of the workload with key %s", key),
		func() bool {
			owner = nil

			for _, n := range nodes {
				liveNodes, _ := n.LiveNodes().Load()

				got, ok := FindWorkloadOwner(key, liveNodes)
				if !ok {
					return false
				}

				if owner == nil {
					owner = got
				} else if !got.Equal(owner) {
					return false
				}

				if n.OwnsWorkload(key, liveNodes) != n.ID.Equal(got) {
					return false
				}
			}

			return true
		},
	)

	return owner
}
