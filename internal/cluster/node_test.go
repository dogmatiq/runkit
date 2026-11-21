package cluster_test

import (
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/cluster/internal/heartbeatpb"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNode(t *testing.T) {
	const (
		interval = 20 * time.Millisecond
		timeout  = 5 * interval
	)

	// setup returns a new database.
	setup := func(t *testing.T) *sql.DB {
		return xtesting.NewDatabase(t)
	}

	newNode := func(
		t *testing.T,
		db *sql.DB,
		interval, timeout time.Duration,
	) *Node {
		return &Node{
			DB:                  db,
			ID:                  uuid.New(),
			HeartbeatInterval:   interval,
			HeartbeatTimeout:    timeout,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        spruce.NewTestLogger(t),
		}
	}

	t.Run("it sends heartbeats on the heartbeat channel", func(t *testing.T) {
		db := setup(t)
		payloads := listenForHeartbeats(t, db)

		n := newNode(t, db, interval, timeout)
		xtesting.GoE(t, n.Run)

		for range 3 {
			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for heartbeat")

			case payload := <-payloads:
				h := decodePayload(t, payload)

				xtesting.Expect(t, "node ID", h.GetNodeId().AsStd(), n.ID)
				xtesting.Expect(t, "expiry", h.GetExpiresAfter().AsDuration(), timeout)
			}
		}
	})

	t.Run("it keeps running when a heartbeat can not be sent", func(t *testing.T) {
		db := xtesting.NewDatabase(t)
		db.Close()

		n := newNode(t, db, interval, timeout)

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

	t.Run("it doesn't send a heartbeat when its ID isn't valid", func(t *testing.T) {
		db := setup(t)
		payloads := listenForHeartbeats(t, db)

		n := newNode(t, db, interval, timeout)
		n.ID = uuid.Nil()
		xtesting.GoE(t, n.Run)

		xtesting.ExpectNoReceive(t, "heartbeat", payloads)
	})

	t.Run("it removes itself from its live nodes while a send takes longer than the heartbeat timeout", func(t *testing.T) {
		db := setup(t)

		// The node's listening connection is one of these. The test holds the
		// other, so each send has to wait for a connection until the node gives
		// up on it at the end of the timeout.
		db.SetMaxOpenConns(2)

		n := newNode(t, db, interval, timeout)
		xtesting.GoE(t, n.Run)

		key := uuid.New()
		expectNodeOwnsWorkload(t, n, key, true)

		conn, err := db.Conn(t.Context())
		if err != nil {
			t.Fatal(err)
		}

		// Without a deadline on each send, the node would wait on the first
		// one for as long as the test holds the connection, and so would never
		// expire its own heartbeat.
		expectNodeOwnsWorkload(t, n, key, false)

		conn.Close()
		expectNodeOwnsWorkload(t, n, key, true)
	})

	t.Run("it sends heartbeats when opening a connection takes longer than a heartbeat interval", func(t *testing.T) {
		// Each dial takes longer than an interval, so a send whose deadline is
		// only one interval would cancel every dial, and no heartbeat would
		// ever be sent.
		db := xtesting.NewDatabaseWithSlowDials(t, 5*interval)

		nodes := []*Node{
			newNode(t, db, interval, 50*interval),
			newNode(t, db, interval, 50*interval),
		}

		for _, n := range nodes {
			xtesting.GoE(t, n.Run)
		}

		expectEachOther(t, nodes)
	})

	t.Run("it replies to a node that joins", func(t *testing.T) {
		db := setup(t)
		payloads := listenForHeartbeats(t, db)

		// The interval is long enough that any heartbeat after the first is
		// sent in reply to a joining node.
		n := newNode(t, db, time.Hour, 2*time.Hour)
		xtesting.GoE(t, n.Run)

		expectHeartbeatsFrom(t, "first heartbeat", payloads, n.ID)

		joining := uuid.New()
		sendHeartbeat(t, db, joining, time.Hour)

		expectHeartbeatsFrom(t, "heartbeats", payloads, joining, n.ID)
		xtesting.ExpectNoReceive(t, "reply to a known node", payloads)

		sendHeartbeat(t, db, joining, time.Hour)

		expectHeartbeatsFrom(t, "heartbeats", payloads, joining)
		xtesting.ExpectNoReceive(t, "reply to a known node", payloads)
	})

	t.Run("it ignores a heartbeat that isn't valid", func(t *testing.T) {
		nodeID := uuid.New()

		cases := []struct {
			Name    string
			Payload string
		}{
			{
				"not base64",
				"!!!",
			},
			{
				"not a heartbeat",
				base64.StdEncoding.EncodeToString([]byte{0xff, 0xff}),
			},
			{
				"no node ID",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithExpiresAfter(durationpb.New(time.Hour)).
					Build()),
			},
			{
				"the nil UUID as the node ID",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithNodeId(uuidpb.FromStd(uuid.Nil())).
					WithExpiresAfter(durationpb.New(time.Hour)).
					Build()),
			},
			{
				"an invalid expiry",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithNodeId(uuidpb.FromStd(nodeID)).
					WithExpiresAfter(&durationpb.Duration{Seconds: 1, Nanos: -1}).
					Build()),
			},
			{
				"no expiry",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithNodeId(uuidpb.FromStd(nodeID)).
					Build()),
			},
			{
				"a zero expiry",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithNodeId(uuidpb.FromStd(nodeID)).
					WithExpiresAfter(durationpb.New(0)).
					Build()),
			},
			{
				"a negative expiry",
				encodeForTest(t, heartbeatpb.NewHeartbeatBuilder().
					WithNodeId(uuidpb.FromStd(nodeID)).
					WithExpiresAfter(durationpb.New(-time.Hour)).
					Build()),
			},
		}

		for _, c := range cases {
			t.Run(c.Name, func(t *testing.T) {
				db := setup(t)
				payloads := listenForHeartbeats(t, db)

				// The interval is long enough that any heartbeat after the
				// first is sent in reply to a joining node.
				n := newNode(t, db, time.Hour, 2*time.Hour)
				xtesting.GoE(t, n.Run)

				expectHeartbeatsFrom(t, "first heartbeat", payloads, n.ID)

				sendPayload(t, db, c.Payload)

				// The node replies to every node that joins, so it sends no
				// reply only if it ignored the heartbeat.
				xtesting.Expect(t, "payload", <-payloads, c.Payload)
				xtesting.ExpectNoReceive(t, "reply to an invalid heartbeat", payloads)

				expectPeers(t, n)
			})
		}
	})

	t.Run("it accepts a heartbeat that has fields it doesn't know", func(t *testing.T) {
		db := setup(t)
		payloads := listenForHeartbeats(t, db)

		n := newNode(t, db, time.Hour, 2*time.Hour)
		xtesting.GoE(t, n.Run)

		expectHeartbeatsFrom(t, "first heartbeat", payloads, n.ID)

		joining := uuid.New()

		data, err := heartbeatpb.NewHeartbeatBuilder().
			WithNodeId(uuidpb.FromStd(joining)).
			WithExpiresAfter(durationpb.New(time.Hour)).
			Build().
			MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		// Append a field with the number 15, which the heartbeat doesn't
		// define, as a later release might add.
		data = append(data, 0x78, 0x01)
		sendPayload(t, db, base64.StdEncoding.EncodeToString(data))

		expectHeartbeatsFrom(t, "heartbeats", payloads, joining, n.ID)
		expectPeers(t, n, joining)
	})

	t.Run("it expires a node after the expiry in the node's heartbeat", func(t *testing.T) {
		db := setup(t)

		const nodeTimeout = 300 * time.Millisecond

		n := newNode(t, db, interval, nodeTimeout)
		xtesting.GoE(t, n.Run)

		// Heartbeats sent before the node is listening are lost, so wait for
		// the node to hear its own heartbeat.
		expectNodeOwnsWorkload(t, n, uuid.New(), true)

		// The nodes never send another heartbeat, so each expires after the
		// time in its only heartbeat: one shorter than this node's timeout, and
		// one much longer.
		short := uuid.New()
		long := uuid.New()
		sendHeartbeat(t, db, short, nodeTimeout/2)
		sendHeartbeat(t, db, long, time.Hour)

		expectPeers(t, n, short, long)
		expectPeers(t, n, long)

		time.Sleep(2 * nodeTimeout)
		expectPeers(t, n, long)
	})

	t.Run("it tracks nodes as they start and stop", func(t *testing.T) {
		db := setup(t)

		n := newNode(t, db, interval, timeout)
		xtesting.GoE(t, n.Run)

		expectPeers(t, n)

		node1 := newNode(t, db, interval, timeout)
		stopNode1 := xtesting.GoE(t, node1.Run)

		expectPeers(t, n, node1.ID)

		node2 := newNode(t, db, interval, timeout)
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
	rejoin := func(t *testing.T, db *sql.DB, n *Node) {
		r := newNode(t, db, interval, timeout)
		r.ID = n.ID
		start(t, r)
	}

	t.Run("it reports whether this node owns the workload with a key", func(t *testing.T) {
		db := setup(t)

		n := newNode(t, db, interval, timeout)
		other := newNode(t, db, interval, timeout)

		start(t, n)
		stopOther := start(t, other)

		key := keyFor(t, other, n)
		expectNodeOwnsWorkload(t, n, key, false)

		stopOther()
		expectNodeOwnsWorkload(t, n, key, true)

		rejoin(t, db, other)
		expectNodeOwnsWorkload(t, n, key, false)
	})

	t.Run("it makes exactly one node the owner of the workload with each key", func(t *testing.T) {
		db := setup(t)

		nodes := []*Node{
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
		}

		for _, n := range nodes {
			xtesting.GoE(t, n.Run)
		}

		for _, n := range nodes {
			var want []uuid.UUID
			for _, peer := range nodes {
				if peer != n {
					want = append(want, peer.ID)
				}
			}

			expectPeers(t, n, want...)
		}

		for range 20 {
			expectExactlyOneOwner(t, uuid.New(), nodes)
		}
	})

	t.Run("it doesn't own any workloads after it stops", func(t *testing.T) {
		db := setup(t)

		n := newNode(t, db, interval, timeout)
		other := newNode(t, db, interval, timeout)

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
		db := setup(t)

		// If only scores counted, all four nodes would own the workloads whose
		// keys are their own IDs in only one run in 256.
		nodes := []*Node{
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		expectEachOther(t, nodes)

		for _, n := range nodes {
			if got := expectSameWorkloadOwner(t, n.ID, nodes); got != n.ID {
				t.Fatalf("unexpected owner of the workload with key %s: got %s", n.ID, got)
			}
		}
	})

	t.Run("it scores a key normally when no live node has that ID", func(t *testing.T) {
		db := setup(t)

		nodes := []*Node{
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		stopped := newNode(t, db, interval, timeout)
		stop := start(t, stopped)

		expectEachOther(t, append(nodes, stopped))
		stop()
		expectEachOther(t, nodes)

		if got := expectSameWorkloadOwner(t, stopped.ID, nodes); got == stopped.ID {
			t.Fatal("stopped node owns the workload whose key is its own ID")
		}
	})

	t.Run("it returns the same workload owner on every node", func(t *testing.T) {
		db := setup(t)

		nodes := []*Node{
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
			newNode(t, db, interval, timeout),
		}

		for _, n := range nodes {
			start(t, n)
		}

		expectEachOther(t, nodes)

		for range 20 {
			expectSameWorkloadOwner(t, uuid.New(), nodes)
		}
	})

	t.Run("it returns false when there are no live nodes", func(t *testing.T) {
		db := setup(t)

		n := newNode(t, db, interval, timeout)
		stop := start(t, n)

		key := uuid.New()
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
		db := setup(t)

		// The interval and timeout are long enough that no heartbeat is sent
		// during the test other than each node's first and its replies to a
		// joining node, and no node times out.
		existing := newNode(t, db, time.Hour, 2*time.Hour)
		xtesting.GoE(t, existing.Run)

		// Wait for the existing node to hear its own heartbeat, at which point
		// it owns every workload, being the only live node.
		expectNodeOwnsWorkload(t, existing, uuid.New(), true)

		joining := newNode(t, db, time.Hour, 2*time.Hour)
		xtesting.GoE(t, joining.Run)

		expectPeers(t, joining, existing.ID)
		expectPeers(t, existing, joining.ID)
	})
}

func expectPeers(
	t testing.TB,
	n *Node,
	want ...uuid.UUID,
) {
	t.Helper()

	expected := uuidset.New(want...)

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the node to have peers %s", expected),
		n.LiveNodes(),
		func(liveNodes *uuidset.SetR) bool {
			peers := uuidset.New()

			for id := range liveNodes.All() {
				if id != n.ID {
					peers.Add(id)
				}
			}

			return peers.IsEqual(expected.R())
		},
	)
}

func expectNodeOwnsWorkload(
	t testing.TB,
	n *Node,
	key uuid.UUID,
	want bool,
) {
	t.Helper()

	xtesting.WaitForObservable(
		t,
		fmt.Sprintf("the node's ownership of the workload with key %s to be %t", key, want),
		n.LiveNodes(),
		func(liveNodes *uuidset.SetR) bool {
			return n.OwnsWorkload(key, liveNodes) == want
		},
	)
}

func expectExactlyOneOwner(
	t testing.TB,
	key uuid.UUID,
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
func workloadOwners(key uuid.UUID, nodes []*Node) []*Node {
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
) uuid.UUID {
	t.Helper()

	nodes := append([]*Node{owner}, others...)

	for _, n := range nodes {
		var peers []uuid.UUID
		for _, p := range nodes {
			if p != n {
				peers = append(peers, p.ID)
			}
		}

		expectPeers(t, n, peers...)
	}

	for {
		key := uuid.New()

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
		var peers []uuid.UUID
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
	key uuid.UUID,
	nodes []*Node,
) uuid.UUID {
	t.Helper()

	var owner uuid.UUID

	xtesting.WaitFor(
		t,
		fmt.Sprintf("every node to agree on the owner of the workload with key %s", key),
		func() bool {
			owner = uuid.Nil()

			for _, n := range nodes {
				liveNodes, _ := n.LiveNodes().Load()

				got, ok := FindWorkloadOwner(key, liveNodes)
				if !ok {
					return false
				}

				if owner == uuid.Nil() {
					owner = got
				} else if got != owner {
					return false
				}

				isOwner := n.ID == got

				if n.OwnsWorkload(key, liveNodes) != isOwner {
					return false
				}
			}

			return true
		},
	)

	return owner
}

// listenForHeartbeats starts listening on the heartbeat channel of db, and
// returns a channel that receives the payload of each notification that arrives
// after it has started.
func listenForHeartbeats(t testing.TB, db *sql.DB) <-chan string {
	t.Helper()

	payloads := make(chan string, 100)
	listening := make(chan struct{})

	xtesting.GoE(t, func(ctx context.Context) error {
		conn, err := db.Conn(ctx)
		if err != nil {
			return err
		}
		defer conn.Close()

		return conn.Raw(func(raw any) error {
			pgConn := raw.(*stdlib.Conn).Conn()

			// Close the connection so that it isn't returned to the pool with
			// an active LISTEN command.
			defer pgConn.Close(context.Background())

			if _, err := pgConn.Exec(ctx, "LISTEN heartbeat"); err != nil {
				return err
			}

			close(listening)

			for {
				n, err := pgConn.WaitForNotification(ctx)
				if err != nil {
					return err
				}

				payloads <- n.Payload
			}
		})
	})

	select {
	case <-listening:
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting to listen for heartbeats")
	}

	return payloads
}

// expectHeartbeatsFrom asserts that the heartbeats that arrive on payloads are
// from the nodes with the given IDs, in order.
func expectHeartbeatsFrom(
	t testing.TB,
	description string,
	payloads <-chan string,
	want ...uuid.UUID,
) {
	t.Helper()

	got := make([]uuid.UUID, 0, len(want))

	for range want {
		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for %s", description)

		case payload := <-payloads:
			got = append(got, decodePayload(t, payload).GetNodeId().AsStd())
		}
	}

	xtesting.ExpectN(t, description, got, want...)
}

// sendHeartbeat sends a heartbeat as though the node with the given ID had sent
// it.
func sendHeartbeat(
	t testing.TB,
	db *sql.DB,
	nodeID uuid.UUID,
	expiresAfter time.Duration,
) {
	t.Helper()

	sendPayload(
		t,
		db,
		encodeForTest(
			t,
			heartbeatpb.NewHeartbeatBuilder().
				WithNodeId(uuidpb.FromStd(nodeID)).
				WithExpiresAfter(durationpb.New(expiresAfter)).
				Build(),
		),
	)
}

// sendPayload sends a notification on the heartbeat channel.
func sendPayload(t testing.TB, db *sql.DB, payload string) {
	t.Helper()

	if _, err := db.ExecContext(
		t.Context(),
		`SELECT pg_notify('heartbeat', $1)`,
		payload,
	); err != nil {
		t.Fatal(err)
	}
}

// encodeForTest returns the payload for a heartbeat, without validating it.
func encodeForTest(t testing.TB, h *heartbeatpb.Heartbeat) string {
	t.Helper()

	data, err := h.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	return base64.StdEncoding.EncodeToString(data)
}

// decodePayload returns the heartbeat in a payload, without validating it.
func decodePayload(t testing.TB, payload string) *heartbeatpb.Heartbeat {
	t.Helper()

	data, err := base64.StdEncoding.DecodeString(payload)
	if err != nil {
		t.Fatal(err)
	}

	h := &heartbeatpb.Heartbeat{}
	if err := h.UnmarshalBinary(data); err != nil {
		t.Fatal(err)
	}

	return h
}
