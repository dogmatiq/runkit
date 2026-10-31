package cluster_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestHeartbeat(t *testing.T) {
	const (
		interval = 20 * time.Millisecond
		timeout  = 5 * interval
	)

	setup := func(t *testing.T) (*sql.DB, *signal.Listener) {
		db := xtesting.NewDatabase(t)
		signals := xtesting.RunSignalListener(t, db, spruce.NewTestLogger(t))

		return db, signals
	}

	newHeartbeat := func(
		t *testing.T,
		db *sql.DB,
		signals *signal.Listener,
		interval, timeout time.Duration,
	) *Heartbeat {
		return &Heartbeat{
			NodeID:   uuidpb.Generate(),
			DB:       db,
			Signals:  signals,
			Interval: interval,
			Timeout:  timeout,
			Logger:   spruce.NewTestLogger(t),
		}
	}

	t.Run("it sends heartbeats on a topic named after the node", func(t *testing.T) {
		db, signals := setup(t)

		heartbeats := make(chan string, 10)
		defer signals.Subscribe("heartbeat", heartbeats)()

		h := newHeartbeat(t, db, signals, interval, timeout)
		xtesting.GoE(t, h.Run)

		// The first signal may be the listener starting, which carries no
		// node ID; after that, every signal is a heartbeat from this node.
		want := h.NodeID.AsString()

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

		signals := &signal.Listener{
			DB:     db,
			Logger: spruce.NewTestLogger(t),
		}

		h := newHeartbeat(t, db, signals, interval, timeout)

		ctx, cancel := context.WithTimeout(t.Context(), 250*time.Millisecond)
		defer cancel()

		// Run must not return until ctx is canceled, despite every attempt
		// to send a heartbeat failing.
		err := h.Run(ctx)

		if ctx.Err() == nil {
			t.Fatal("heartbeat stopped before context was canceled")
		}

		if !errors.Is(err, ctx.Err()) {
			t.Fatalf("unexpected error: got %v, want %v", err, ctx.Err())
		}
	})

	t.Run("it replies to a node that joins", func(t *testing.T) {
		db, signals := setup(t)

		heartbeats := make(chan string, 10)
		defer signals.Subscribe("heartbeat", heartbeats)()

		// The interval is long enough that any heartbeat after the first is
		// sent in reply to a joining node.
		h := newHeartbeat(t, db, signals, time.Hour, time.Hour)
		xtesting.GoE(t, h.Run)

		want := h.NodeID.AsString()
		xtesting.ExpectToReceiveN(t, "first heartbeat", heartbeats, want)

		joining := uuidpb.Generate().AsString()
		if err := signal.Notify(t.Context(), db, "heartbeat."+joining); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(t, "heartbeats", heartbeats, joining, want)
		xtesting.ExpectNoReceive(t, "reply to a known node", heartbeats)

		if err := signal.Notify(t.Context(), db, "heartbeat."+joining); err != nil {
			t.Fatal(err)
		}

		xtesting.ExpectToReceiveN(t, "heartbeats", heartbeats, joining)
		xtesting.ExpectNoReceive(t, "reply to a known node", heartbeats)
	})

	t.Run("it tracks nodes as they start and stop", func(t *testing.T) {
		db, signals := setup(t)

		h := newHeartbeat(t, db, signals, interval, timeout)
		xtesting.GoE(t, h.Run)

		expectMembership(t, h, h.NodeID)

		node1 := newHeartbeat(t, db, signals, interval, timeout)
		stopNode1 := xtesting.GoE(t, node1.Run)

		expectMembership(t, h, h.NodeID, node1.NodeID)

		node2 := newHeartbeat(t, db, signals, interval, timeout)
		stopNode2 := xtesting.GoE(t, node2.Run)

		expectMembership(t, h, h.NodeID, node1.NodeID, node2.NodeID)

		stopNode1()

		expectMembership(t, h, h.NodeID, node2.NodeID)

		stopNode2()

		expectMembership(t, h, h.NodeID)
	})

	t.Run("it learns about existing nodes without waiting for their next heartbeat", func(t *testing.T) {
		db, signals := setup(t)

		// The interval and timeout are long enough that no heartbeat is sent
		// during the test unless one is requested, and no node times out.
		existing := newHeartbeat(t, db, signals, time.Hour, time.Hour)
		xtesting.GoE(t, existing.Run)

		expectMembership(t, existing, existing.NodeID)

		joining := newHeartbeat(t, db, signals, time.Hour, time.Hour)
		xtesting.GoE(t, joining.Run)

		expectMembership(t, joining, existing.NodeID, joining.NodeID)
		expectMembership(t, existing, existing.NodeID, joining.NodeID)
	})
}

func expectMembership(
	t testing.TB,
	h *Heartbeat,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidpb.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := h.Membership()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected live node IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
