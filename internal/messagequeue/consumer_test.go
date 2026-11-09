package messagequeue_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestConsumer(t *testing.T) {
	typeID := uuidpb.Generate()

	// setup returns a database, a running node on it, and a function that
	// returns another running node on it.
	setup := func(t *testing.T) (*sql.DB, *cluster.Node, func() *cluster.Node) {
		db := xtesting.NewDatabase(t)

		signals := &signal.Bus{
			DB:           db,
			EngineLogger: spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, signals.Run)

		newNode := func() *cluster.Node {
			n := &cluster.Node{
				ID:                uuidpb.Generate(),
				DB:                db,
				Signals:           signals,
				HeartbeatInterval: 20 * time.Millisecond,
				HeartbeatTimeout:  1 * time.Second,
				EngineLogger:      spruce.NewTestLogger(t),
			}
			xtesting.GoE(t, n.Run)
			return n
		}

		return db, newNode(), newNode
	}

	// waitForLiveNodes waits until each of the given nodes is in the set of
	// live nodes that n sees.
	waitForLiveNodes := func(t *testing.T, n *cluster.Node, nodes ...*cluster.Node) {
		t.Helper()

		xtesting.WaitForObservable(
			t,
			"live nodes",
			n.LiveNodes(),
			func(liveNodes *uuidcollection.Set) bool {
				for _, x := range nodes {
					if !liveNodes.Has(x.ID) {
						return false
					}
				}
				return true
			},
		)
	}

	// run runs a consumer of messages of typeID on the given node, and returns
	// a function that stops it. The consumer calls the given function to
	// attempt each message.
	run := func(
		t *testing.T,
		db *sql.DB,
		n *cluster.Node,
		attempt func(context.Context, *uuidpb.UUID),
	) func() {
		logger := spruce.NewTestLogger(t)

		queues := &Catalog{
			DB:           db,
			Node:         n,
			EngineLogger: logger,
		}
		xtesting.GoE(t, queues.Run)

		consumer := &Consumer{
			DB:             db,
			Node:           n,
			Queues:         queues,
			MessageTypeIDs: uuidcollection.NewSet(typeID),
			Attempt:        attempt,
			EngineLogger:   logger,
		}

		return xtesting.GoE(t, consumer.Run)
	}

	// addMessage adds a message of the given type to the queue with the given
	// ID, to be delivered at the given time, and returns the message's ID.
	addMessage := func(
		t *testing.T,
		db *sql.DB,
		queueID *uuidpb.UUID,
		typeID *uuidpb.UUID,
		deliverAt time.Time,
	) *uuidpb.UUID {
		t.Helper()

		messageID := uuidpb.Generate()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO messagequeue.messages (
				message_id,
				message_type_id,
				queue_id,
				envelope,
				deliver_at
			) VALUES ($1, $2, $3, '\x00', $4)`,
			xsql.UUID(messageID),
			xsql.UUID(typeID),
			xsql.UUID(queueID),
			deliverAt,
		); err != nil {
			t.Fatal(err)
		}

		return messageID
	}

	// exec executes the given statement, failing the test if it fails.
	exec := func(t *testing.T, db *sql.DB, query string, args ...any) {
		t.Helper()

		if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
			t.Fatal(err)
		}
	}

	// removeMessage removes the message with the given ID from its queue.
	removeMessage := func(t *testing.T, db *sql.DB, messageID *uuidpb.UUID) {
		t.Helper()

		exec(
			t,
			db,
			`DELETE FROM messagequeue.messages
			WHERE message_id = $1`,
			xsql.UUID(messageID),
		)
	}

	// deliver returns an attempt function that sends the ID of each message
	// to the returned channel, then removes the message, as a delivery does.
	deliver := func(t *testing.T, db *sql.DB) (func(context.Context, *uuidpb.UUID), <-chan *uuidpb.UUID) {
		attempted := make(chan *uuidpb.UUID, 100)

		return func(ctx context.Context, messageID *uuidpb.UUID) {
			select {
			case <-ctx.Done():
				return
			case attempted <- messageID:
			}

			if _, err := db.ExecContext(
				ctx,
				`DELETE FROM messagequeue.messages
				WHERE message_id = $1`,
				xsql.UUID(messageID),
			); err != nil && ctx.Err() == nil {
				t.Error(err)
			}
		}, attempted
	}

	// expectAttempt fails the test unless the next message attempted is the
	// message with the given ID.
	expectAttempt := func(t *testing.T, attempted <-chan *uuidpb.UUID, want *uuidpb.UUID) {
		t.Helper()

		select {
		case got := <-attempted:
			if !got.Equal(want) {
				t.Fatalf("unexpected message attempted: got %s, want %s", got, want)
			}
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for message %s to be attempted", want)
		}
	}

	t.Run("it attempts a due message in a queue it is responsible for", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(t, db, n, attempt)

		expectAttempt(t, attempted, messageID)
	})

	t.Run("it ignores a message in a queue another node is responsible for", func(t *testing.T) {
		t.Parallel()

		db, n, newNode := setup(t)
		other := newNode()
		waitForLiveNodes(t, n, n, other)
		attempt, attempted := deliver(t, db)

		// Self-affinity makes each live node responsible for the queue with its
		// own ID. The ignored message is earlier, so it would be attempted
		// first if it were in a queue that n is responsible for.
		now := time.Now()
		addMessage(t, db, other.ID, typeID, now.Add(-1*time.Second))
		sentinel := addMessage(t, db, n.ID, typeID, now)
		run(t, db, n, attempt)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it ignores a message of a type it has no handler for", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)

		now := time.Now()
		addMessage(t, db, n.ID, uuidpb.Generate(), now.Add(-1*time.Second))
		sentinel := addMessage(t, db, n.ID, typeID, now)
		run(t, db, n, attempt)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it attempts a future message when it becomes due", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attemptedAt := make(chan time.Time, 1)

		deliverAt := time.Now().Add(200 * time.Millisecond)
		addMessage(t, db, n.ID, typeID, deliverAt)
		run(
			t,
			db,
			n,
			func(context.Context, *uuidpb.UUID) {
				select {
				case attemptedAt <- time.Now():
				default:
				}
			},
		)

		select {
		case got := <-attemptedAt:
			if got.Before(deliverAt) {
				t.Fatalf("message attempted %s before it was due", deliverAt.Sub(got))
			}
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the message to be attempted")
		}
	})

	t.Run("it attempts an earlier message queued while it waits for a later one", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)

		addMessage(t, db, n.ID, typeID, time.Now().Add(1*time.Hour))
		run(t, db, n, attempt)

		earlier := addMessage(t, db, n.ID, typeID, time.Now())

		expectAttempt(t, attempted, earlier)
	})

	t.Run("it does not attempt a message that moves to another node's queue while it waits", func(t *testing.T) {
		t.Parallel()

		db, n, newNode := setup(t)
		other := newNode()
		waitForLiveNodes(t, n, n, other)
		attempt, attempted := deliver(t, db)

		now := time.Now()
		moved := addMessage(t, db, n.ID, typeID, now.Add(500*time.Millisecond))
		sentinel := addMessage(t, db, n.ID, typeID, now.Add(700*time.Millisecond))
		run(t, db, n, attempt)

		exec(
			t,
			db,
			`UPDATE messagequeue.messages
			SET queue_id = $2
			WHERE message_id = $1`,
			xsql.UUID(moved),
			xsql.UUID(other.ID),
		)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it attempts messages in order of their delivery time", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)

		now := time.Now()
		third := addMessage(t, db, n.ID, typeID, now.Add(300*time.Millisecond))
		first := addMessage(t, db, n.ID, typeID, now.Add(100*time.Millisecond))
		second := addMessage(t, db, n.ID, typeID, now.Add(200*time.Millisecond))
		run(t, db, n, attempt)

		expectAttempt(t, attempted, first)
		expectAttempt(t, attempted, second)
		expectAttempt(t, attempted, third)
	})

	t.Run("it moves on when the message it waits for is removed", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)

		now := time.Now()
		removed := addMessage(t, db, n.ID, typeID, now.Add(300*time.Millisecond))
		sentinel := addMessage(t, db, n.ID, typeID, now.Add(600*time.Millisecond))
		run(t, db, n, attempt)

		// Removing a message sends no signal, so the consumer finds out when
		// the removed message would have been due.
		removeMessage(t, db, removed)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it does not attempt a message that is already being attempted", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempted := make(chan *uuidpb.UUID, 100)

		blocked := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			func(ctx context.Context, messageID *uuidpb.UUID) {
				attempted <- messageID
				<-ctx.Done()
			},
		)
		expectAttempt(t, attempted, blocked)

		// The sentinel's signal wakes the consumer while the blocked message
		// is still being attempted.
		sentinel := addMessage(t, db, n.ID, typeID, time.Now())

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it attempts a message again once its attempt ends", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempted := make(chan *uuidpb.UUID, 100)

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			func(ctx context.Context, messageID *uuidpb.UUID) {
				// The message stays in its queue, as it does when a delivery
				// can't continue.
				select {
				case attempted <- messageID:
				case <-ctx.Done():
				}
			},
		)

		expectAttempt(t, attempted, messageID)
		expectAttempt(t, attempted, messageID)
	})

	t.Run("it keeps running when discovery fails", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempt, attempted := deliver(t, db)
		run(t, db, n, attempt)

		exec(t, db, `ALTER TABLE messagequeue.messages RENAME TO unavailable`)

		// Renaming the table keeps its triggers, so adding a message wakes the
		// consumer, and its discovery fails.
		messageID := uuidpb.Generate()
		exec(
			t,
			db,
			`INSERT INTO messagequeue.unavailable (
				message_id,
				message_type_id,
				queue_id,
				envelope
			) VALUES ($1, $2, $3, '\x00')`,
			xsql.UUID(messageID),
			xsql.UUID(typeID),
			xsql.UUID(n.ID),
		)

		exec(t, db, `ALTER TABLE messagequeue.unavailable RENAME TO messages`)

		expectAttempt(t, attempted, messageID)
	})

	t.Run("it waits for attempts in flight when it stops", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		started := make(chan struct{}, 1)
		release := make(chan struct{})

		addMessage(t, db, n.ID, typeID, time.Now())

		stopped := make(chan struct{})
		stop := run(
			t,
			db,
			n,
			func(context.Context, *uuidpb.UUID) {
				select {
				case started <- struct{}{}:
					<-release
				default:
				}
			},
		)

		select {
		case <-started:
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the message to be attempted")
		}

		go func() {
			stop()
			close(stopped)
		}()

		select {
		case <-stopped:
			t.Fatal("consumer stopped before its attempt ended")
		case <-time.After(50 * time.Millisecond):
		}

		close(release)

		select {
		case <-stopped:
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the consumer to stop")
		}
	})
}
