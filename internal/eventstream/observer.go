package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/backoff"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Observer notifies subscribers when new event streams become available.
type Observer struct {
	// Notifications is the source of real-time event stream notifications.
	Notifications *notification.Listener

	// DB is the database connection used to query the set of existing event
	// streams.
	DB *sql.DB

	// Logger is the target for log messages produced by the observer.
	Logger *slog.Logger

	done      xsync.Latch
	backoff   backoff.Backoff
	streamIDs uuidpb.Set

	once        sync.Once
	sub, unsub  chan *streamSubscriber
	subscribers map[*streamSubscriber]struct{}
}

// streamSubscriber encapsulates the state of a single subscriber to
// event stream notifications.
type streamSubscriber struct {
	StreamIDs    chan<- *uuidpb.UUID
	Unsubscribed xsync.Latch
}

func (o *Observer) Run(ctx context.Context) {
	o.init()
	defer o.done.Set()

	streamIDs := make(chan string, 1)
	unsubscribe := o.Notifications.Subscribe("eventstream.create", streamIDs)
	defer unsubscribe()

	for {
		err := o.load(ctx)

		if err == nil {
			break
		}

		if ctx.Err() != nil {
			return
		}

		o.Logger.ErrorContext(
			ctx,
			"unable to load event streams",
			xslog.Error(err),
		)

		select {
		case <-ctx.Done():
			return
		case <-o.backoff.Chan():
		case <-streamIDs:
			// If we successfully receive a notification, there's a good chance
			// the load operation will now also succeed. This also ensures the
			// notification listener is not blocked attempting to write to the
			// channel.
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case streamID := <-streamIDs:
			o.update(ctx, streamID)
		case sub := <-o.sub:
			o.subscribe(ctx, sub)
		case sub := <-o.unsub:
			delete(o.subscribers, sub)
		}
	}
}

// Subscribe registers ch to receive notifications about event streams as they
// become available.
func (o *Observer) Subscribe(ch chan<- *uuidpb.UUID) func() {
	o.init()

	sub := &streamSubscriber{
		StreamIDs: ch,
	}

	select {
	case <-o.done.Chan():
	case o.sub <- sub:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			sub.Unsubscribed.Set()

			select {
			case <-o.done.Chan():
			case o.unsub <- sub:
			}
		})
	}
}

func (o *Observer) init() {
	o.once.Do(func() {
		o.sub = make(chan *streamSubscriber)
		o.unsub = make(chan *streamSubscriber)
		o.subscribers = make(map[*streamSubscriber]struct{})
	})
}

func (o *Observer) load(ctx context.Context) error {
	rows, err := o.DB.QueryContext(
		ctx,
		`SELECT id
		FROM eventstream.streams`,
	)
	if err != nil {
		return fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		streamID := &uuidpb.UUID{}

		if err := rows.Scan(
			xsql.UUID(streamID),
		); err != nil {
			return fmt.Errorf("unable to scan event stream: %w", err)
		}

		o.streamIDs.Add(streamID)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return nil
}

// subscribe registers a new subscriber and sends it the current set of event
// streams.
func (o *Observer) subscribe(ctx context.Context, sub *streamSubscriber) {
	o.subscribers[sub] = struct{}{}

	for streamID := range o.streamIDs.All() {
		select {
		case <-ctx.Done():
			return
		case <-sub.Unsubscribed.Chan():
			return
		case sub.StreamIDs <- streamID:
		}
	}
}

// update updates the set of available event streams with the given stream ID.
func (o *Observer) update(ctx context.Context, idString string) {
	streamID, err := uuidpb.Parse(idString)
	if err != nil {
		o.Logger.ErrorContext(
			ctx,
			"ignored invalid ID in event stream notification",
			slog.String("stream_id", idString),
			slog.Any("error", err),
		)
		return
	}

	if o.streamIDs.Has(streamID) {
		return
	}

	o.streamIDs.Add(streamID)

	var group sync.WaitGroup

	for sub := range o.subscribers {
		group.Go(func() {
			select {
			case <-ctx.Done():
			case <-sub.Unsubscribed.Chan():
			case sub.StreamIDs <- streamID:
			}
		})
	}

	group.Wait()
}
