package messagequeue_test

import (
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
			ID:                  uuidpb.Generate(),
			DB:                  db,
			Signals:             signals,
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
	addMessage := func(t *testing.T, db *sql.DB, queueID *uuidpb.UUID) *uuidpb.UUID {
		t.Helper()

		messageID := uuidpb.Generate()

		if _, err := db.ExecContext(
			t.Context(),
			`INSERT INTO messagequeue.messages (
				message_id,
				message_type_id,
				queue_id,
				envelope
			) VALUES ($1, $2, $3, '\x00')`,
			xsql.UUID(messageID),
			xsql.UUID(uuidpb.Generate()),
			xsql.UUID(queueID),
		); err != nil {
			t.Fatal(err)
		}

		return messageID
	}

	// waitForQueueIDs waits until the catalog's queue IDs are the given IDs.
	waitForQueueIDs := func(t *testing.T, catalog *Catalog, want ...*uuidpb.UUID) {
		t.Helper()

		xtesting.WaitForObservable(
			t,
			"queue IDs",
			catalog.QueueIDs(),
			func(got *uuidcollection.Set) bool {
				return got.IsEqual(uuidcollection.NewSet(want...))
			},
		)
	}

	t.Run("it reads the queues that already have messages", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		q1 := uuidpb.Generate()
		q2 := uuidpb.Generate()
		addMessage(t, db, q1)
		addMessage(t, db, q2)

		xtesting.GoE(t, catalog.Run)

		waitForQueueIDs(t, catalog, q1, q2)
	})

	t.Run("it includes a queue once a message is added to it", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		existing := uuidpb.Generate()
		addMessage(t, db, existing)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, existing)

		q := uuidpb.Generate()
		addMessage(t, db, q)

		waitForQueueIDs(t, catalog, existing, q)
	})

	t.Run("it includes the queue that a message moves to, not the one it moved from", func(t *testing.T) {
		t.Parallel()

		db, catalog := setup(t)
		from := uuidpb.Generate()
		messageID := addMessage(t, db, from)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, from)

		to := uuidpb.Generate()
		if _, err := db.ExecContext(
			t.Context(),
			`UPDATE messagequeue.messages
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
		emptied := uuidpb.Generate()
		messageID := addMessage(t, db, emptied)

		xtesting.GoE(t, catalog.Run)
		waitForQueueIDs(t, catalog, emptied)

		if _, err := db.ExecContext(
			t.Context(),
			`DELETE FROM messagequeue.messages
			WHERE message_id = $1`,
			xsql.UUID(messageID),
		); err != nil {
			t.Fatal(err)
		}

		q := uuidpb.Generate()
		addMessage(t, db, q)

		waitForQueueIDs(t, catalog, q)
	})
}
