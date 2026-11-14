package cluster_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestNode_AttachSignal(t *testing.T) {
	// setup returns a database, and a function that returns a new node on it.
	setup := func(t *testing.T) (*sql.DB, func() *Node) {
		db := xtesting.NewDatabase(t)

		signals := &signal.Bus{
			DB:                  db,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, signals.Run)

		return db, func() *Node {
			return &Node{
				ID:                  uuidpb.Generate(),
				DB:                  db,
				Signals:             signals,
				HeartbeatInterval:   20 * time.Millisecond,
				HeartbeatTimeout:    100 * time.Millisecond,
				EngineFailurePolicy: xtesting.FailurePolicy(),
				EngineLogger:        spruce.NewTestLogger(t),
			}
		}
	}

	// attachSignal attaches the given cluster-wide signal to a new target, and
	// returns the target.
	attachSignal := func(t *testing.T, n *Node, signal string) *cosyne.Signal {
		t.Helper()

		s := &cosyne.Signal{}
		n.AttachSignal(t.Context(), signal, s)

		return s
	}

	t.Run("it does not notify the target when it is attached", func(t *testing.T) {
		t.Parallel()

		_, newNode := setup(t)
		n := newNode()
		s := attachSignal(t, n, "<signal>")

		expectNotNotified(t, s)
	})

	t.Run("it notifies the target once the node is listening", func(t *testing.T) {
		t.Parallel()

		_, newNode := setup(t)
		n := newNode()
		s := attachSignal(t, n, "<signal>")

		xtesting.GoE(t, n.Run)

		expectNotified(t, s)
	})

	t.Run("it notifies the target on every node when the cluster-wide signal is notified", func(t *testing.T) {
		t.Parallel()

		db, newNode := setup(t)
		nodes := []*Node{newNode(), newNode()}

		var signals []*cosyne.Signal
		for _, n := range nodes {
			s := attachSignal(t, n, "<signal>")
			xtesting.GoE(t, n.Run)
			expectNotified(t, s) // once the node is listening
			signals = append(signals, s)
		}

		sendSignal(t, db, "<signal>")

		for _, s := range signals {
			expectNotified(t, s)
		}
	})

	t.Run("it does not notify the target when another cluster-wide signal is notified", func(t *testing.T) {
		t.Parallel()

		db, newNode := setup(t)
		n := newNode()
		s := attachSignal(t, n, "<signal>")
		sentinel := attachSignal(t, n, "<sentinel>")
		xtesting.GoE(t, n.Run)
		expectNotified(t, s)
		expectNotified(t, sentinel)

		sendSignal(t, db, "<other>")
		sendSignal(t, db, "<signal>.<suffix>")
		sendSignal(t, db, "<signal>suffix")
		sendSignal(t, db, "<sentinel>")

		expectNotified(t, sentinel)
		expectNotNotified(t, s)
	})

	t.Run("it coalesces notifications that arrive while one is pending", func(t *testing.T) {
		t.Parallel()

		db, newNode := setup(t)
		n := newNode()
		s := attachSignal(t, n, "<signal>")
		sentinel := attachSignal(t, n, "<sentinel>")
		xtesting.GoE(t, n.Run)
		expectNotified(t, s)
		expectNotified(t, sentinel)

		sendSignal(t, db, "<signal>")
		sendSignal(t, db, "<signal>")
		sendSignal(t, db, "<signal>")
		sendSignal(t, db, "<sentinel>")

		expectNotified(t, sentinel)
		expectNotified(t, s)
		expectNotNotified(t, s)
	})

	t.Run("it stops notifying the target once detached", func(t *testing.T) {
		t.Parallel()

		db, newNode := setup(t)
		n := newNode()

		s := &cosyne.Signal{}
		detach := n.AttachSignal(t.Context(), "<signal>", s)
		sentinel := attachSignal(t, n, "<sentinel>")
		xtesting.GoE(t, n.Run)
		expectNotified(t, sentinel)

		detach()
		<-s.Chan() // a notification may be pending from before it was detached

		sendSignal(t, db, "<signal>")
		sendSignal(t, db, "<sentinel>")

		expectNotified(t, sentinel)
		expectNotNotified(t, s)
	})

	t.Run("it stops notifying the target once ctx is canceled", func(t *testing.T) {
		t.Parallel()

		db, newNode := setup(t)
		n := newNode()

		ctx, cancel := context.WithCancel(t.Context())
		s := &cosyne.Signal{}
		n.AttachSignal(ctx, "<signal>", s)
		sentinel := attachSignal(t, n, "<sentinel>")
		xtesting.GoE(t, n.Run)
		expectNotified(t, sentinel)

		cancel()
		<-s.Chan() // a notification may be pending from before it was canceled

		// Canceling ctx detaches the target asynchronously, so wait until
		// the cluster-wide signal no longer reaches it.
		xtesting.WaitFor(
			t,
			"the target to be detached",
			func() bool {
				sendSignal(t, db, "<signal>")
				sendSignal(t, db, "<sentinel>")
				expectNotified(t, sentinel)

				select {
				case <-s.Chan():
					return false
				default:
					return true
				}
			},
		)
	})
}

// sendSignal notifies the given cluster-wide signal, as a database trigger
// does.
func sendSignal(t testing.TB, db *sql.DB, signal string) {
	t.Helper()

	if _, err := db.ExecContext(
		t.Context(),
		`SELECT pg_notify('signal', $1)`,
		signal,
	); err != nil {
		t.Fatal(err)
	}
}

// expectNotified fails the test unless the given signal is notified within
// [xtesting.WaitTimeout]. It consumes the notification.
func expectNotified(t testing.TB, s *cosyne.Signal) {
	t.Helper()

	select {
	case <-s.Chan():
	case <-time.After(xtesting.WaitTimeout):
		t.Fatal("timeout waiting for the signal to be notified")
	}
}

// expectNotNotified fails the test if the given signal has a pending
// notification.
func expectNotNotified(t testing.TB, s *cosyne.Signal) {
	t.Helper()

	select {
	case <-s.Chan():
		t.Fatal("unexpected notification")
	default:
	}
}
