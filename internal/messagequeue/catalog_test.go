package messagequeue_test

import (
	"database/sql"
	"testing"
	"time"
	"uuid"

	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCatalog(t *testing.T) {
	setup := func(t *testing.T) (*sql.DB, *Catalog) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		signals := &signal.Bus{
			DB:                  db,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        logger,
		}
		xtesting.GoE(t, signals.Run)

		node := &cluster.Node{
			DB:                  db,
			Signals:             signals,
			ID:                  uuid.New(),
			HeartbeatInterval:   20 * time.Millisecond,
			HeartbeatTimeout:    100 * time.Millisecond,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        logger,
		}
		xtesting.GoE(t, node.Run)

		catalog := &Catalog{
			DB:                  db,
			Node:                node,
			EngineFailurePolicy: xtesting.FailurePolicy(),
			EngineLogger:        logger,
		}

		return db, catalog
	}

	// addMessage adds a message to the queue with the given ID, and returns
	// the message's ID.
	addMessage := func(t *testing.T, db *sql.DB, queueID uuid.UUID) uuid.UUID {
		t.Helper()

		messageID := uuid.New()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO runkit.queued_messages (
				message_id,
				message_type_id,
				queue_id,
				envelope,
				scheduled_for,
				next_attempt_at
			) VALUES ($1, $2, $3, '\x00', $4, $4)`,
			xsql.UUID(messageID),
			xsql.UUID(uuid.New()),
			xsql.UUID(queueID),
			time.Now(),
		); err != nil {
			t.Fatal(err)
		}

		return messageID
	}

	// waitForQueueIDs waits until the catalog's queue IDs are the given IDs.
	waitForQueueIDs := func(t *testing.T, catalog *Catalog, want ...uuid.UUID) {
		t.Helper()

		xtesting.WaitForObservable(
			t,
			"queue IDs",
			catalog.QueueIDs(),
			func(got uuidset.SetR) bool {
				return got.IsEqual(uuidset.New(want...).R())
			},
		)
	}

	t.Run("it reads the queues that already have messages", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		q1 := uuid.New()
		q2 := uuid.New()
		q3 := uuid.New()
		addMessage(t, db, q1)
		addMessage(t, db, q2)
		addMessage(t, db, q2)
		addMessage(t, db, q3)

		xtesting.GoE(t, catalog.Run)

		waitForQueueIDs(t, catalog, q1, q2, q3)
	})

	t.Run("it includes a queue once a message is added to it", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		existing := uuid.New()
		addMessage(t, db, existing)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, existing)

		q := uuid.New()
		addMessage(t, db, q)

		waitForQueueIDs(t, catalog, existing, q)
	})

	t.Run("it includes the queue that a message moves to, not the one it moved from", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		from := uuid.New()
		messageID := addMessage(t, db, from)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, from)

		to := uuid.New()
		if _, err := db.ExecContext(
			t.Context(),
			`UPDATE runkit.queued_messages
			SET queue_id = $2
			WHERE message_id = $1`,
			xsql.UUID(messageID),
			xsql.UUID(to),
		); err != nil {
			t.Fatal(err)
		}

		waitForQueueIDs(t, catalog, to)
	})

	t.Run("it excludes a queue that has no messages after the next signal", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		emptied := uuid.New()
		messageID := addMessage(t, db, emptied)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, emptied)

		if _, err := db.ExecContext(
			t.Context(),
			`DELETE FROM runkit.queued_messages
			WHERE message_id = $1`,
			xsql.UUID(messageID),
		); err != nil {
			t.Fatal(err)
		}

		q := uuid.New()
		addMessage(t, db, q)

		waitForQueueIDs(t, catalog, q)
	})
}
