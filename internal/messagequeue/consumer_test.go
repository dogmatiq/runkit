package messagequeue_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestConsumer(t *testing.T) {
	typeID := stubs.MessageTypeUUID[*stubs.CommandStub[stubs.TypeA]]()
	contentionDelay := 100 * time.Millisecond

	// setup returns a database, a running node on it, and a function that
	// returns another node on it, which isn't running.
	setup := func(t *testing.T) (*sql.DB, *cluster.Node, func() *cluster.Node) {
		db := xtesting.NewDatabase(t)

		signals := &signal.Bus{
			DB:           db,
			EngineLogger: spruce.NewTestLogger(t),
		}
		xtesting.GoE(t, signals.Run)

		newNode := func() *cluster.Node {
			return &cluster.Node{
				ID:                uuidpb.Generate(),
				DB:                db,
				Signals:           signals,
				HeartbeatInterval: 20 * time.Millisecond,
				HeartbeatTimeout:  1 * time.Second,
				EngineLogger:      spruce.NewTestLogger(t),
			}
		}

		n := newNode()
		xtesting.GoE(t, n.Run)

		return db, n, newNode
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

	// run runs a consumer of messages of typeID on the given node, with the
	// given concurrency limit, and returns a function that stops it. The
	// consumer passes each message to the given sink.
	run := func(
		t *testing.T,
		db *sql.DB,
		n *cluster.Node,
		concurrency int,
		s Sink,
	) func() {
		logger := spruce.NewTestLogger(t)

		queues := &Catalog{
			DB:           db,
			Node:         n,
			EngineLogger: logger,
		}
		xtesting.GoE(t, queues.Run)

		var sinks uuidcollection.Map[Sink]
		sinks.Set(typeID, s)

		consumer := &Consumer{
			DB:              db,
			Node:            n,
			Queues:          queues,
			Sinks:           sinks,
			Concurrency:     concurrency,
			ContentionDelay: contentionDelay,
			EngineLogger:    logger,
		}

		return xtesting.GoE(t, consumer.Run)
	}

	// insertMessage adds a message of the given type, with the given envelope,
	// to the queue with the given ID, to be attempted at the given time.
	insertMessage := func(
		t *testing.T,
		db *sql.DB,
		messageID *uuidpb.UUID,
		queueID *uuidpb.UUID,
		typeID *uuidpb.UUID,
		envelope []byte,
		nextAttemptAt time.Time,
	) {
		t.Helper()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO messagequeue.messages (
				message_id,
				message_type_id,
				queue_id,
				envelope,
				next_attempt_at
			) VALUES ($1, $2, $3, $4, $5)`,
			xsql.UUID(messageID),
			xsql.UUID(typeID),
			xsql.UUID(queueID),
			envelope,
			nextAttemptAt,
		); err != nil {
			t.Fatal(err)
		}
	}

	// addMessage adds a message of the given type to the queue with the given
	// ID, to be attempted at the given time, and returns the message's ID.
	addMessage := func(
		t *testing.T,
		db *sql.DB,
		queueID *uuidpb.UUID,
		typeID *uuidpb.UUID,
		nextAttemptAt time.Time,
	) *uuidpb.UUID {
		t.Helper()

		env := xtesting.PackCommand(t, stubs.CommandA1)
		messageID := env.GetBody().GetMessageId()

		data, err := env.MarshalBinary()
		if err != nil {
			t.Fatal(err)
		}

		insertMessage(t, db, messageID, queueID, typeID, data, nextAttemptAt)

		return messageID
	}

	// exec executes the given statement, failing the test if it fails.
	exec := func(t *testing.T, db *sql.DB, query string, args ...any) {
		t.Helper()

		if _, err := db.ExecContext(t.Context(), query, args...); err != nil {
			t.Fatal(err)
		}
	}

	// lockMessage locks the message with the given ID in a transaction, and
	// returns a function that rolls the transaction back.
	lockMessage := func(t *testing.T, db *sql.DB, messageID *uuidpb.UUID) func() {
		t.Helper()

		tx, err := db.BeginTx(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { tx.Rollback() })

		if _, err := tx.ExecContext(
			t.Context(),
			`SELECT 1
			FROM messagequeue.messages
			WHERE message_id = $1
			FOR UPDATE`,
			xsql.UUID(messageID),
		); err != nil {
			t.Fatal(err)
		}

		return func() { tx.Rollback() }
	}

	// expectQueued fails the test unless the message with the given ID comes
	// to satisfy the given SQL condition on its row.
	expectQueued := func(t *testing.T, db *sql.DB, description string, messageID *uuidpb.UUID, cond string) {
		t.Helper()

		xtesting.WaitForQueryResult(
			t,
			description,
			1,
			db,
			`SELECT COUNT(*)
			FROM messagequeue.messages
			WHERE message_id = $1
			AND `+cond,
			xsql.UUID(messageID),
		)
	}

	// expectRemoved fails the test unless the message with the given ID is
	// removed from its queue.
	expectRemoved := func(t *testing.T, db *sql.DB, messageID *uuidpb.UUID) {
		t.Helper()

		xtesting.WaitForQueryResult(
			t,
			"the delivered message to be removed",
			0,
			db,
			`SELECT COUNT(*)
			FROM messagequeue.messages
			WHERE message_id = $1`,
			xsql.UUID(messageID),
		)
	}

	// record returns a sink that sends the ID of each message to the
	// returned channel, then delivers it.
	record := func() (Sink, <-chan *uuidpb.UUID) {
		attempted := make(chan *uuidpb.UUID, 100)

		return sinkFunc(
			func(ctx context.Context, _ *sql.Tx, d Delivery, _ *envelopepb.Envelope) error {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case attempted <- d.MessageID:
					return nil
				}
			},
		), attempted
	}

	// block returns a sink that sends the ID of each message to the
	// returned channel, then delivers it once the returned function is called.
	block := func() (Sink, <-chan *uuidpb.UUID, func()) {
		attempted := make(chan *uuidpb.UUID, 100)
		release := make(chan struct{})

		h := sinkFunc(
			func(ctx context.Context, _ *sql.Tx, d Delivery, _ *envelopepb.Envelope) error {
				attempted <- d.MessageID

				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-release:
					return nil
				}
			},
		)

		return h, attempted, func() { close(release) }
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

	// expectAttempts fails the test unless the next messages attempted are the
	// messages with the given IDs, in any order.
	expectAttempts := func(t *testing.T, attempted <-chan *uuidpb.UUID, want ...*uuidpb.UUID) {
		t.Helper()

		remaining := uuidcollection.NewSet(want...)

		for !remaining.IsEmpty() {
			select {
			case got := <-attempted:
				if !remaining.Has(got) {
					t.Fatalf("unexpected message attempted: got %s, want one of %s", got, remaining)
				}
				remaining.Delete(got)
			case <-time.After(xtesting.WaitTimeout):
				t.Fatalf("timeout waiting for messages %s to be attempted", remaining)
			}
		}
	}

	// expectNoAttempt fails the test if a message is attempted soon.
	expectNoAttempt := func(t *testing.T, attempted <-chan *uuidpb.UUID) {
		t.Helper()

		select {
		case got := <-attempted:
			t.Fatalf("unexpected message attempted: %s", got)
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Run("it attempts a message that is due, if its node owns the queue's workload", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(t, db, n, 10, h)

		expectAttempt(t, attempted, messageID)
	})

	t.Run("it removes a delivered message", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, _ := record()

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(t, db, n, 10, h)

		expectRemoved(t, db, messageID)
	})

	t.Run("it completes a delivery if the sink removed the message within the transaction", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(ctx context.Context, tx *sql.Tx, d Delivery, _ *envelopepb.Envelope) error {
					return xsql.ExecOne(
						ctx,
						tx,
						`DELETE FROM messagequeue.messages
						WHERE message_id = $1`,
						xsql.UUID(d.MessageID),
					)
				},
			),
		)

		// If the attempt ended with an error, its transaction would be rolled
		// back, and the message would stay in its queue.
		expectRemoved(t, db, messageID)
	})

	t.Run("it ignores a message if another node owns the queue's workload", func(t *testing.T) {
		t.Parallel()

		db, n, newNode := setup(t)
		other := newNode()
		xtesting.GoE(t, other.Run)
		waitForLiveNodes(t, n, n, other)
		h, attempted := record()

		// Self-affinity makes each live node own the workload of the queue
		// whose ID is the node's ID. The ignored message is earlier, so it
		// would be attempted first if n owned the workload of its queue.
		now := time.Now()
		addMessage(t, db, other.ID, typeID, now.Add(-1*time.Second))
		sentinel := addMessage(t, db, n.ID, typeID, now)
		run(t, db, n, 10, h)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it ignores a message whose type has no sink", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		now := time.Now()
		addMessage(t, db, n.ID, uuidpb.Generate(), now.Add(-1*time.Second))
		sentinel := addMessage(t, db, n.ID, typeID, now)
		run(t, db, n, 10, h)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it attempts a future message when it becomes due", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attemptedAt := make(chan time.Time, 1)

		nextAttemptAt := time.Now().Add(200 * time.Millisecond)
		addMessage(t, db, n.ID, typeID, nextAttemptAt)
		run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(context.Context, *sql.Tx, Delivery, *envelopepb.Envelope) error {
					select {
					case attemptedAt <- time.Now():
					default:
					}
					return nil
				},
			),
		)

		select {
		case got := <-attemptedAt:
			if got.Before(nextAttemptAt) {
				t.Fatalf("message attempted %s before it was due", nextAttemptAt.Sub(got))
			}
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the message to be attempted")
		}
	})

	t.Run("it attempts an earlier message queued while it waits for a later one", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		addMessage(t, db, n.ID, typeID, time.Now().Add(1*time.Hour))
		run(t, db, n, 10, h)

		earlier := addMessage(t, db, n.ID, typeID, time.Now())

		expectAttempt(t, attempted, earlier)
	})

	t.Run("it does not attempt a message that moves to another node's queue while it waits", func(t *testing.T) {
		t.Parallel()

		db, n, newNode := setup(t)
		other := newNode()
		xtesting.GoE(t, other.Run)
		waitForLiveNodes(t, n, n, other)
		h, attempted := record()

		now := time.Now()
		moved := addMessage(t, db, n.ID, typeID, now.Add(500*time.Millisecond))
		sentinel := addMessage(t, db, n.ID, typeID, now.Add(700*time.Millisecond))
		run(t, db, n, 10, h)

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

	t.Run("it attempts messages in the order in which they're due", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		now := time.Now()
		third := addMessage(t, db, n.ID, typeID, now.Add(300*time.Millisecond))
		first := addMessage(t, db, n.ID, typeID, now.Add(100*time.Millisecond))
		second := addMessage(t, db, n.ID, typeID, now.Add(200*time.Millisecond))

		// With a concurrency limit of one, each attempt is made before the next
		// begins, so the order holds even if several messages are due by the
		// time the consumer starts.
		run(t, db, n, 1, h)

		expectAttempt(t, attempted, first)
		expectAttempt(t, attempted, second)
		expectAttempt(t, attempted, third)
	})

	t.Run("it moves on when the message it waits for is removed", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		now := time.Now()
		removed := addMessage(t, db, n.ID, typeID, now.Add(300*time.Millisecond))
		sentinel := addMessage(t, db, n.ID, typeID, now.Add(600*time.Millisecond))
		run(t, db, n, 10, h)

		// Removing a message doesn't send a signal, so the consumer finds out
		// when the removed message would have been due.
		exec(
			t,
			db,
			`DELETE FROM messagequeue.messages
			WHERE message_id = $1`,
			xsql.UUID(removed),
		)

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it does not attempt a message that is already being attempted", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted, _ := block()

		blocked := addMessage(t, db, n.ID, typeID, time.Now())
		run(t, db, n, 10, h)
		expectAttempt(t, attempted, blocked)

		// The sentinel's signal wakes the consumer while the blocked message
		// is still being attempted.
		sentinel := addMessage(t, db, n.ID, typeID, time.Now())

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it attempts no more messages at once than its concurrency limit", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted, release := block()

		now := time.Now()
		first := addMessage(t, db, n.ID, typeID, now.Add(-3*time.Second))
		second := addMessage(t, db, n.ID, typeID, now.Add(-2*time.Second))
		third := addMessage(t, db, n.ID, typeID, now.Add(-1*time.Second))
		run(t, db, n, 2, h)

		expectAttempts(t, attempted, first, second)
		expectNoAttempt(t, attempted)

		release()

		expectAttempt(t, attempted, third)
	})

	t.Run("it keeps a signal that arrives while it is at its concurrency limit", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted, release := block()

		blocked := addMessage(t, db, n.ID, typeID, time.Now())
		run(t, db, n, 1, h)
		expectAttempt(t, attempted, blocked)

		sentinel := addMessage(t, db, n.ID, typeID, time.Now())
		release()

		expectAttempt(t, attempted, sentinel)
	})

	t.Run("it uses all of the headroom that several attempts free at once", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted, release := block()

		now := time.Now()

		var earlier, later []*uuidpb.UUID
		for i := range 3 {
			at := now.Add(time.Duration(i-6) * time.Second)
			earlier = append(earlier, addMessage(t, db, n.ID, typeID, at))
		}
		for i := range 3 {
			at := now.Add(time.Duration(i-3) * time.Second)
			later = append(later, addMessage(t, db, n.ID, typeID, at))
		}

		run(t, db, n, 3, h)
		expectAttempts(t, attempted, earlier...)

		release()

		expectAttempts(t, attempted, later...)
	})

	t.Run("it attempts other messages while one is locked", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		now := time.Now()
		locked := addMessage(t, db, n.ID, typeID, now.Add(-3*time.Second))
		first := addMessage(t, db, n.ID, typeID, now.Add(-2*time.Second))
		second := addMessage(t, db, n.ID, typeID, now.Add(-1*time.Second))
		unlock := lockMessage(t, db, locked)

		// With a concurrency limit of one, the others can only be attempted if
		// the locked message doesn't take the slot.
		run(t, db, n, 1, h)
		expectAttempt(t, attempted, first)
		expectAttempt(t, attempted, second)

		unlock()

		expectAttempt(t, attempted, locked)
	})

	t.Run("it attempts a locked message again after the contention delay", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		locked := addMessage(t, db, n.ID, typeID, time.Now())
		unlock := lockMessage(t, db, locked)
		run(t, db, n, 1, h)
		expectNoAttempt(t, attempted)

		unlock()

		expectAttempt(t, attempted, locked)
	})

	t.Run("it postpones the delivery after a failed attempt, and increments its failures", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		var failed atomic.Bool

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(ctx context.Context, _ *sql.Tx, _ Delivery, _ *envelopepb.Envelope) error {
					if failed.CompareAndSwap(false, true) {
						return ErrFailed
					}

					// Later attempts block, so that they don't change the row.
					<-ctx.Done()
					return ctx.Err()
				},
			),
		)

		expectQueued(
			t,
			db,
			"the delivery to be postponed after a failure",
			messageID,
			`failures = 1
			AND failing_since IS NOT NULL
			AND next_attempt_at > scheduled_for`,
		)
	})

	t.Run("it postpones the delivery without a failure when the sink needs a row while another transaction holds that row's lock", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		var busy atomic.Bool

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(ctx context.Context, _ *sql.Tx, _ Delivery, _ *envelopepb.Envelope) error {
					if busy.CompareAndSwap(false, true) {
						return ErrBusy
					}

					// Later attempts block, so that they don't change the row.
					<-ctx.Done()
					return ctx.Err()
				},
			),
		)

		expectQueued(
			t,
			db,
			"the delivery to be postponed without a failure",
			messageID,
			`failures = 0
			AND failing_since IS NULL
			AND next_attempt_at > scheduled_for`,
		)
	})

	t.Run("it postpones a message whose envelope can not be unmarshaled", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()

		messageID := uuidpb.Generate()
		insertMessage(t, db, messageID, n.ID, typeID, []byte{0}, time.Now())
		run(t, db, n, 10, h)

		expectQueued(
			t,
			db,
			"the message to be postponed",
			messageID,
			`failures > 0
			AND failing_since IS NOT NULL
			AND next_attempt_at > scheduled_for`,
		)
		expectNoAttempt(t, attempted)
	})

	t.Run("it leaves a message for another attempt when an attempt ends with an error", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		attempted := make(chan *uuidpb.UUID, 100)

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(ctx context.Context, _ *sql.Tx, d Delivery, _ *envelopepb.Envelope) error {
					select {
					case attempted <- d.MessageID:
					case <-ctx.Done():
					}
					return errors.New("<error>")
				},
			),
		)

		expectAttempt(t, attempted, messageID)
		expectAttempt(t, attempted, messageID)
		expectQueued(
			t,
			db,
			"the message to be left without a failure",
			messageID,
			`failures = 0
			AND failing_since IS NULL`,
		)
	})

	t.Run("it lets an attempt finish after its queue's workload moves to another node", func(t *testing.T) {
		t.Parallel()

		db, n, newNode := setup(t)
		other := newNode()
		waitForLiveNodes(t, n, n)
		h, attempted, release := block()

		// Until the other node is live, n is the only live node, so it owns
		// the workload of the queue with the other node's ID. When the other
		// node is live, self-affinity makes it the owner instead.
		messageID := addMessage(t, db, other.ID, typeID, time.Now())
		run(t, db, n, 10, h)
		expectAttempt(t, attempted, messageID)

		xtesting.GoE(t, other.Run)
		waitForLiveNodes(t, n, n, other)
		release()

		expectRemoved(t, db, messageID)
	})

	t.Run("it keeps running when it can't find the next messages", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		h, attempted := record()
		run(t, db, n, 10, h)

		exec(t, db, `ALTER TABLE messagequeue.messages RENAME TO unavailable`)

		// Renaming the table keeps its triggers, so adding a message wakes the
		// consumer, and it can't find the next messages.
		env := xtesting.PackCommand(t, stubs.CommandA1)
		exec(
			t,
			db,
			`INSERT INTO messagequeue.unavailable (
				message_id,
				message_type_id,
				queue_id,
				envelope
			) VALUES ($1, $2, $3, $4)`,
			xsql.UUID(env.GetBody().GetMessageId()),
			xsql.UUID(typeID),
			xsql.UUID(n.ID),
			xsql.Envelope(env),
		)

		exec(t, db, `ALTER TABLE messagequeue.unavailable RENAME TO messages`)

		expectAttempt(t, attempted, env.GetBody().GetMessageId())
	})

	t.Run("it rolls back attempts in flight when it stops", func(t *testing.T) {
		t.Parallel()

		db, n, _ := setup(t)
		started := make(chan struct{}, 1)

		messageID := addMessage(t, db, n.ID, typeID, time.Now())
		stop := run(
			t,
			db,
			n,
			10,
			sinkFunc(
				func(ctx context.Context, tx *sql.Tx, d Delivery, _ *envelopepb.Envelope) error {
					if _, err := tx.ExecContext(
						ctx,
						`UPDATE messagequeue.messages
						SET failures = 99,
							failing_since = clock_timestamp()
						WHERE message_id = $1`,
						xsql.UUID(d.MessageID),
					); err != nil {
						return err
					}

					started <- struct{}{}
					<-ctx.Done()
					return ctx.Err()
				},
			),
		)

		select {
		case <-started:
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for the message to be attempted")
		}

		stop()

		xtesting.ExpectQueryResult(
			t,
			"the attempt's changes to be rolled back",
			1,
			db,
			`SELECT COUNT(*)
			FROM messagequeue.messages
			WHERE message_id = $1
			AND failures = 0`,
			xsql.UUID(messageID),
		)
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
			10,
			sinkFunc(
				func(context.Context, *sql.Tx, Delivery, *envelopepb.Envelope) error {
					select {
					case started <- struct{}{}:
						<-release
					default:
					}
					return nil
				},
			),
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
			t.Fatal("consumer stopped before its attempt was made")
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

// sinkFunc is a [Sink] that delivers each message by calling a
// function.
type sinkFunc func(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	env *envelopepb.Envelope,
) error

func (f sinkFunc) Deliver(
	ctx context.Context,
	tx *sql.Tx,
	d Delivery,
	env *envelopepb.Envelope,
) error {
	return f(ctx, tx, d, env)
}
