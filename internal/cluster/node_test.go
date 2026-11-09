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

	t.Run("it reports whether this node is responsible for the work with a key", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		key := keyFor(t, other, n)
		expectNodeResponsibleFor(t, n, key, false)

		stopOther()
		expectNodeResponsibleFor(t, n, key, true)

		rejoin(t, signals, other)
		expectNodeResponsibleFor(t, n, key, false)
	})

	t.Run("it makes exactly one node responsible for the work with each key", func(t *testing.T) {
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
			expectExactlyOneResponsible(t, uuidpb.Generate(), nodes)
		}
	})

	t.Run("it is responsible for nothing once it stops", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		other := newNode(t, signals, interval, timeout)

		stop := start(t, n)
		start(t, other)

		key := keyFor(t, n, other)
		expectNodeResponsibleFor(t, n, key, true)

		stop()

		liveNodes, _ := n.LiveNodes().Load()

		if n.IsResponsibleFor(key, liveNodes) {
			t.Fatal("stopped node is responsible for work")
		}

		if !liveNodes.IsEmpty() {
			t.Fatalf("stopped node sees live nodes: %s", liveNodes)
		}
	})

	t.Run("it makes the node whose ID is the key responsible for the work with that key", func(t *testing.T) {
		signals := setup(t)

		// If only scores counted, all four nodes would be responsible for
		// their own IDs in one run in 256.
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

		for _, owner := range nodes {
			if got := expectSameResponsibleNode(t, owner.ID, nodes); !got.Equal(owner.ID) {
				t.Fatalf("unexpected responsible node for %s: got %s", owner.ID, got)
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

		if got := expectSameResponsibleNode(t, stopped.ID, nodes); got.Equal(stopped.ID) {
			t.Fatal("stopped node is responsible for the work with its own ID as the key")
		}
	})

	t.Run("it returns the same responsible node on every node", func(t *testing.T) {
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
			expectSameResponsibleNode(t, uuidpb.Generate(), nodes)
		}
	})

	t.Run("it returns false when there are no live nodes", func(t *testing.T) {
		signals := setup(t)

		n := newNode(t, signals, interval, timeout)
		stop := start(t, n)

		key := uuidpb.Generate()
		expectNodeResponsibleFor(t, n, key, true)

		stop()

		liveNodes, _ := n.LiveNodes().Load()

		if got, ok := FindResponsibleNode(key, liveNodes); ok {
			t.Fatalf("unexpected responsible node: got %s", got)
		}

		if got, ok := FindResponsibleNode(n.ID, liveNodes); ok {
			t.Fatalf("unexpected responsible node for the stopped node's ID: got %s", got)
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
		// it is responsible for all work, being the only live node.
		expectNodeResponsibleFor(t, existing, uuidpb.Generate(), true)

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

func expectNodeResponsibleFor(
	t testing.TB,
	n *Node,
	key *uuidpb.UUID,
	want bool,
) {
	t.Helper()

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the node's responsibility for the work with key %s to be %t", key, want),
		n.LiveNodes(),
		func(liveNodes *uuidcollection.Set) bool {
			return n.IsResponsibleFor(key, liveNodes) == want
		},
	)
}

func expectExactlyOneResponsible(
	t testing.TB,
	key *uuidpb.UUID,
	nodes []*Node,
) {
	t.Helper()

	xtesting.WaitFor(
		t,
		fmt.Sprintf("exactly one node to be responsible for the work with key %s", key),
		func() bool {
			return len(responsibleNodes(key, nodes)) == 1
		},
	)
}

// responsibleNodes returns the nodes that are responsible for the work with the
// given key, according to their own memberships.
func responsibleNodes(key *uuidpb.UUID, nodes []*Node) []*Node {
	var responsible []*Node

	for _, n := range nodes {
		liveNodes, _ := n.LiveNodes().Load()

		if n.IsResponsibleFor(key, liveNodes) {
			responsible = append(responsible, n)
		}
	}

	return responsible
}

// keyFor returns a key whose work the responsible node is responsible for
// while it and the other nodes are live. All of the nodes must be running, and
// no others.
func keyFor(
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
		key := uuidpb.Generate()

		if got := responsibleNodes(key, nodes); len(got) == 1 && got[0] == responsible {
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

// expectSameResponsibleNode waits until every given node returns the same
// responsible node for the work with the given key, and that node is the only
// one that reports itself responsible for it. It returns the responsible
// node's ID.
func expectSameResponsibleNode(
	t testing.TB,
	key *uuidpb.UUID,
	nodes []*Node,
) *uuidpb.UUID {
	t.Helper()

	var responsible *uuidpb.UUID

	xtesting.WaitFor(
		t,
		fmt.Sprintf("every node to agree on the node responsible for the work with key %s", key),
		func() bool {
			responsible = nil

			for _, n := range nodes {
				liveNodes, _ := n.LiveNodes().Load()

				got, ok := FindResponsibleNode(key, liveNodes)
				if !ok {
					return false
				}

				if responsible == nil {
					responsible = got
				} else if !got.Equal(responsible) {
					return false
				}

				if n.IsResponsibleFor(key, liveNodes) != n.ID.Equal(got) {
					return false
				}
			}

			return true
		},
	)

	return responsible
}
