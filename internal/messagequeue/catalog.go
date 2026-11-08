package messagequeue

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Catalog keeps track of which queues have messages, as seen from this node.
//
// It reads the whole set from the database each time a message queue signal
// arrives, so a signal dropped by the [signal.Bus] is covered by the next.
type Catalog struct {
	// DB is the database that stores the message queue.
	DB *sql.DB

	// Signals is the bus on which changes to the message queue are signaled.
	Signals *signal.Bus

	// EngineLogger is the target for the engine's records about observing
	// the queues.
	EngineLogger *slog.Logger

	// queueIDs is the set of IDs of the queues that have messages.
	queueIDs cosyne.Observable[*uuidcollection.Set]
}

// QueueIDs returns an observable of the IDs of the queues that have messages.
//
// The set is nil until it is first read from the database. It must not be
// modified.
func (c *Catalog) QueueIDs() cosyne.ObservableR[*uuidcollection.Set] {
	return &c.queueIDs
}

// Run observes the queues until ctx is canceled.
//
// It must be called at most once.
func (c *Catalog) Run(ctx context.Context) error {
	return supervisor.Run(ctx, c.EngineLogger, "observe queues", c.run)
}

// run reads the set of queues each time a message queue signal arrives, and
// publishes it whenever it changes.
func (c *Catalog) run(ctx context.Context) error {
	signals, stop := c.Signals.Watch(ctx, "messagequeue", 1)
	defer stop()

	for range signals {
		queueIDs, err := c.load(ctx)
		if err != nil {
			return err
		}

		current, _ := c.queueIDs.Load()
		if current == nil || !current.IsEqual(queueIDs) {
			c.queueIDs.Store(queueIDs)
		}
	}

	return ctx.Err()
}

// load returns the IDs of the queues that have messages.
func (c *Catalog) load(ctx context.Context) (*uuidcollection.Set, error) {
	rows, err := c.DB.QueryContext(
		ctx,
		`SELECT DISTINCT queue_id
		FROM messagequeue.messages`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query queues: %w", err)
	}
	defer rows.Close()

	queueIDs := uuidcollection.NewSet()

	for rows.Next() {
		queueID := &uuidpb.UUID{}

		if err := rows.Scan(
			xsql.UUID(queueID),
		); err != nil {
			return nil, fmt.Errorf("unable to scan queue: %w", err)
		}

		queueIDs.Add(queueID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate queues: %w", err)
	}

	return queueIDs, nil
}
