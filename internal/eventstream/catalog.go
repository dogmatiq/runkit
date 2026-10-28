package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Catalog tracks the set of available event streams.
type Catalog struct {
	// DB is the database connection used to discover event streams.
	DB *sql.DB

	// Signals is the listener used to receive signals that event streams have
	// been created.
	Signals *signal.Listener

	// Logger is the target for log messages produced by the catalog.
	Logger *slog.Logger

	streamIDs xsync.Observable[*uuidpb.Set]
}

// StreamIDs returns the IDs of the known event streams, and a channel that is
// closed when more streams become known.
//
// Streams are never removed, so each set is a superset of the sets returned
// before it. The returned set must not be modified.
func (c *Catalog) StreamIDs() (*uuidpb.Set, <-chan struct{}) {
	return c.streamIDs.Load()
}

// Run discovers event streams until ctx is canceled.
//
// It must be called at most once.
func (c *Catalog) Run(ctx context.Context) {
	supervisor.Run(ctx, c.Logger, c.discover)
}

// discover reads the set of event streams, then waits for a stream to be
// created and reads it again.
func (c *Catalog) discover(ctx context.Context) error {
	// Subscribe to signals about new streams _before_ querying, so that a
	// stream created between the query and the subscription is not missed.
	//
	// The signal identifies the stream that was created, but that is not
	// used; each query reads the complete set of streams.
	created := make(chan string, 1)
	unsubscribe := c.Signals.Subscribe("eventstream.create", created)
	defer unsubscribe()

	for {
		streamIDs, err := c.loadStreamIDs(ctx)
		if err != nil {
			return err
		}

		// Streams are never removed, so the set has changed if and only if
		// its size has.
		known, _ := c.streamIDs.Load()

		if streamIDs.Len() != known.Len() {
			for streamID := range streamIDs.Diff(known).All() {
				c.Logger.DebugContext(
					ctx,
					"event stream discovered",
					xslog.UUID("stream_id", streamID),
				)
			}

			c.streamIDs.Store(streamIDs)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-created:
		}
	}
}

// loadStreamIDs returns the IDs of all event streams.
func (c *Catalog) loadStreamIDs(ctx context.Context) (*uuidpb.Set, error) {
	rows, err := c.DB.QueryContext(
		ctx,
		`SELECT id
		FROM eventstream.streams`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	streamIDs := uuidpb.NewSet()

	for rows.Next() {
		streamID := &uuidpb.UUID{}

		if err := rows.Scan(
			xsql.UUID(streamID),
		); err != nil {
			return nil, fmt.Errorf("unable to scan event stream: %w", err)
		}

		streamIDs.Add(streamID)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return streamIDs, nil
}
