package messagequeue

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"uuid"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Catalog keeps track of which queues have messages, as seen from this node.
type Catalog struct {
	// DB is the database that stores the message queue.
	DB *sql.DB

	// Node is the node that receives the cluster-wide signal sent when
	// messages are added to queues or moved between them.
	Node *cluster.Node

	// EngineFailurePolicy is how the engine responds to failures to observe the queues.
	EngineFailurePolicy failure.Policy

	// EngineLogger is the target for the engine's records about observing
	// the queues.
	EngineLogger *slog.Logger

	// queueIDs is the set of IDs of the queues that have messages.
	queueIDs cosyne.Observable[uuidset.SetR]
}

// QueueIDs returns an observable set containing the ID of each queue that has
// messages.
//
// The set is empty until it is first read from the database.
func (c *Catalog) QueueIDs() cosyne.ObservableR[uuidset.SetR] {
	return &c.queueIDs
}

// Run observes the queues until ctx is canceled.
//
// It must be called at most once.
func (c *Catalog) Run(ctx context.Context) error {
	return supervisor.Run(
		ctx,
		"observe queues",
		c.EngineFailurePolicy,
		c.EngineLogger,
		c.run,
	)
}

// run reads the set of queues straight away, and again each time a message is
// added to a queue or moved between them, and publishes it whenever it
// changes.
func (c *Catalog) run(ctx context.Context) error {
	var changed cosyne.Signal

	detach := c.Node.AttachSignal(ctx, "messages_enqueued", &changed)
	defer detach()

	for {
		queueIDs, err := c.load(ctx)
		if err != nil {
			return err
		}

		if current, _ := c.queueIDs.Load(); !queueIDs.IsEqual(current) {
			c.queueIDs.Store(queueIDs.R())
		}

		if err := changed.WaitContext(ctx); err != nil {
			return err
		}
	}
}

// load returns the IDs of the queues that have messages.
func (c *Catalog) load(ctx context.Context) (*uuidset.Set, error) {
	// The query reads one entry of the queued_messages_by_queue index for each
	// queue, so its cost depends on the number of queues, not the number of
	// messages. PostgreSQL can't skip from one distinct value to the next in
	// an index by itself (a "loose index scan"), so the query does the
	// skipping: it finds the lowest queue ID, then repeatedly finds the
	// lowest queue ID that is greater than the previous one. The last step
	// finds no greater queue ID and yields NULL. The outer SELECT discards
	// that NULL.
	rows, err := c.DB.QueryContext(
		ctx,
		`WITH RECURSIVE queues AS (
			(
				SELECT queue_id
				FROM runkit.queued_messages
				ORDER BY queue_id
				LIMIT 1
			)
			UNION ALL
			SELECT (
				SELECT m.queue_id
				FROM runkit.queued_messages AS m
				WHERE m.queue_id > q.queue_id
				ORDER BY m.queue_id
				LIMIT 1
			)
			FROM queues AS q
			WHERE q.queue_id IS NOT NULL
		)
		SELECT queue_id
		FROM queues
		WHERE queue_id IS NOT NULL`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query queues: %w", err)
	}
	defer rows.Close()

	queueIDs := &uuidset.Set{}

	for rows.Next() {
		var queueID uuid.UUID

		if err := rows.Scan(
			xsql.UUID(&queueID),
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
