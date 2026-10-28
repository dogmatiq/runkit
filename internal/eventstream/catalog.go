package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Catalog tracks the set of event streams and notifies subscribers as new
// streams become available.
type Catalog struct {
	// DB is the database connection used to discover event streams.
	DB *sql.DB

	// Notifications is the listener used to receive notifications about new
	// event streams being created.
	Notifications *notification.Listener

	// Logger is the target for log messages produced by the catalog and the
	// supervisor of the discovery session it starts.
	Logger *slog.Logger

	once       sync.Once
	sub, unsub chan *catalogSubscriber
	done       xsync.Latch

	group       supervisor.Group
	discovered  chan *uuidpb.Set
	streamIDs   uuidpb.Set
	subscribers map[*catalogSubscriber]struct{}
}

// catalogSubscriber encapsulates the state of a single subscriber to
// notifications about new event streams.
type catalogSubscriber struct {
	StreamIDs    chan<- *uuidpb.UUID
	Unsubscribed xsync.Latch
}

// Subscribe registers ch to receive the ID of each event stream as it becomes
// available, beginning with those that already exist.
//
// It returns a function that unsubscribes.
func (c *Catalog) Subscribe(ch chan<- *uuidpb.UUID) func() {
	c.init()

	sub := &catalogSubscriber{
		StreamIDs: ch,
	}

	select {
	case <-c.done.Chan():
	case c.sub <- sub:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			sub.Unsubscribed.Set()

			select {
			case <-c.done.Chan():
			case c.unsub <- sub:
			}
		})
	}
}

func (c *Catalog) init() {
	c.once.Do(func() {
		c.sub = make(chan *catalogSubscriber)
		c.unsub = make(chan *catalogSubscriber)
		c.subscribers = map[*catalogSubscriber]struct{}{}
	})
}

// Run executes the catalog until ctx is canceled.
//
// Run must be called at most once; after it returns, Subscribe has no effect.
func (c *Catalog) Run(ctx context.Context) {
	c.init()
	defer func() {
		c.done.Set()
		c.group.Wait()
	}()

	c.discovered = make(chan *uuidpb.Set)
	c.group.Go(ctx, c.Logger, c.discover)

	for ctx.Err() == nil {
		c.tick(ctx)
	}
}

// tick waits for the next request or discovery result and handles it, or
// returns when ctx is canceled.
func (c *Catalog) tick(ctx context.Context) {
	select {
	case <-ctx.Done():
	case streamIDs := <-c.discovered:
		c.handleDiscovered(ctx, streamIDs)
	case sub := <-c.sub:
		c.handleSubscribe(ctx, sub)
	case sub := <-c.unsub:
		delete(c.subscribers, sub)
	}
}

// handleDiscovered handles the result of a discovery query by notifying
// subscribers of any streams that were not previously known.
//
// Streams are never removed, so the known set only ever grows.
func (c *Catalog) handleDiscovered(ctx context.Context, streamIDs *uuidpb.Set) {
	for streamID := range streamIDs.All() {
		if c.streamIDs.Has(streamID) {
			continue
		}

		c.streamIDs.Add(streamID)

		c.Logger.DebugContext(
			ctx,
			"event stream discovered",
			xslog.UUID("stream_id", streamID),
		)

		c.broadcast(ctx, streamID)
	}
}

// handleSubscribe registers a new subscriber and sends it the ID of every
// known event stream.
func (c *Catalog) handleSubscribe(ctx context.Context, sub *catalogSubscriber) {
	c.subscribers[sub] = struct{}{}

	for streamID := range c.streamIDs.All() {
		if !c.unicast(ctx, sub, streamID) {
			return
		}
	}
}

// broadcast sends a stream ID to every subscriber.
func (c *Catalog) broadcast(ctx context.Context, streamID *uuidpb.UUID) {
	var group sync.WaitGroup

	for sub := range c.subscribers {
		group.Go(func() {
			c.unicast(ctx, sub, streamID)
		})
	}

	group.Wait()
}

// unicast sends a stream ID to a single subscriber.
//
// It returns false if the subscriber can no longer receive, either because it
// has unsubscribed or because ctx is canceled.
func (c *Catalog) unicast(
	ctx context.Context,
	sub *catalogSubscriber,
	streamID *uuidpb.UUID,
) bool {
	select {
	case <-ctx.Done():
		return false
	case <-sub.Unsubscribed.Chan():
		return false
	case sub.StreamIDs <- streamID:
		return true
	}
}

// discover is the catalog's supervised session.
//
// It reads the set of event streams and sends it on c.discovered, then waits
// for a stream to be created and reads it again. It is restartable: it holds
// no state, and touches nothing owned by the catalog's loop other than
// c.discovered.
func (c *Catalog) discover(ctx context.Context) error {
	// Subscribe to notifications about new streams _before_ querying, so that
	// a stream created between the query and the subscription is not missed.
	// The notification's payload is not used; each query reads the complete
	// set of streams, so the notification is only a signal to query again.
	created := make(chan string, 1)
	unsubscribe := c.Notifications.SubscribeLossy("eventstream.create", created)
	defer unsubscribe()

	for {
		streamIDs, err := c.loadStreamIDs(ctx)
		if err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case c.discovered <- streamIDs:
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
