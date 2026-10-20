package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/backoff"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/ringbuffer"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// contemporaryBufferSize is the number of most recent events that are
// considered "contemporary" and are retained in memory for new subscribers.
const contemporaryBufferSize = 10

// contemporaryReader reads new events from a specific event stream and
// broadcasts them to each [readerSubscriber].
type contemporaryReader struct {
	DB            *sql.DB
	Notifications *notification.Listener
	StreamID      *uuidpb.UUID
	Subscribe     chan *readerSubscriber
	Unsubscribe   chan *readerSubscriber
	Lagged        chan<- *readerSubscriber
	Logger        *slog.Logger

	backoff     backoff.Backoff
	offset      uint64
	buffer      *ringbuffer.Ring[event]
	subscribers map[*readerSubscriber]struct{}
}

func (r *contemporaryReader) Run(ctx context.Context) {
	r.buffer = ringbuffer.New[event](contemporaryBufferSize)
	r.subscribers = map[*readerSubscriber]struct{}{}

	topic := "eventstream.append." + r.StreamID.AsString()
	appended := make(chan string, 1)
	unsubscribe := r.Notifications.SubscribeLossy(topic, appended)
	defer unsubscribe()

	pollNow := make(chan time.Time)
	close(pollNow)

	poll := (<-chan time.Time)(pollNow)

	for {
		select {
		case <-ctx.Done():
			return

		case <-poll:
			if err := r.tryPoll(ctx); err != nil {
				r.Logger.ErrorContext(
					ctx,
					"unable to read events",
					xslog.Error(err),
				)
				poll = r.backoff.Chan()
			} else {
				poll = nil
			}

		case <-appended:
			poll = pollNow

		case sub := <-r.Subscribe:
			r.handleSubscribe(ctx, sub)

		case sub := <-r.Unsubscribe:
			delete(r.subscribers, sub)
		}
	}
}

func (r *contemporaryReader) tryPoll(ctx context.Context) error {
	if err := r.loadOffset(ctx); err != nil {
		return fmt.Errorf("unable to load next offset: %w", err)
	}

	if err := r.readEvents(ctx); err != nil {
		return fmt.Errorf("unable to read events: %w", err)
	}

	return nil
}

func (r *contemporaryReader) loadOffset(ctx context.Context) error {
	if r.offset != 0 {
		return nil
	}

	row := r.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(r.StreamID),
	)

	if err := row.Scan(&r.offset); err != nil {
		return fmt.Errorf("unable to scan next offset: %w", err)
	}

	if r.offset < contemporaryBufferSize {
		r.offset = 0
	} else {
		r.offset -= contemporaryBufferSize
	}

	return nil
}

func (r *contemporaryReader) readEvents(ctx context.Context) error {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT
			stream_offset,
			envelope
		FROM eventstream.events
		WHERE stream_id = $1
		AND stream_offset >= $2
		LIMIT $3`,
		xsql.UUID(r.StreamID),
		r.offset,
		contemporaryBufferSize,
	)
	if err != nil {
		return fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		ev := event{
			Envelope: &envelopepb.Envelope{},
		}

		if err := rows.Scan(
			&ev.Offset,
			xsql.Envelope(ev.Envelope),
		); err != nil {
			return fmt.Errorf("unable to scan event: %w", err)
		}

		if ev.Offset != r.offset {
			return fmt.Errorf("event stream is corrupt: unexpected event offset: got %d, want %d", ev.Offset, r.offset)
		}

		envelopepb.SetExtension(
			ev.Envelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(r.StreamID).
				WithOffset(ev.Offset).
				Build(),
		)

		r.broadcast(ctx, ev)

		r.buffer.Push(ev)
		r.offset++
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate events: %w", err)
	}

	return nil
}

func (r *contemporaryReader) handleSubscribe(_ context.Context, sub *readerSubscriber) {
	r.subscribers[sub] = struct{}{}

	// if sub.Offset >= r.nextOffset {
	// 	return
	// }

	// 	firstOffset := r.nextOffset - uint64(r.recentEvents.Len())

	// 	if sub.Offset < firstOffset {
	// 		r.abandon(ctx, sub)
	// 		return
	// 	}

	// 	idx := int(sub.Offset - firstOffset)
	// 	for _, env := range r.recentEvents.Slice(idx, r.recentEvents.Len()) {
	// 		r.send(ctx, sub, env)
	// 	}
	// }

	// // abandon removes a subscriber that is lagging behind the contemporary events.
	//
	//	func (r *contemporaryReader) abandon(ctx context.Context, sub *readSubscriber) {
	//		select {
	//		case <-ctx.Done():
	//		case <-sub.Unsubscribed.Chan():
	//		case r.Abandoned <- sub:
	//			delete(r.subscribers, sub)
	//		}
}

func (r *contemporaryReader) unicast(ctx context.Context, sub *readerSubscriber, ev event) {
	if sub.Offset > ev.Offset {
		return
	}

	if sub.Offset < ev.Offset {
		panic(
			fmt.Sprintf(
				"subscriber offset %d is behind event offset %d",
				sub.Offset,
				ev.Offset,
			),
		)
	}

	typeID := ev.Envelope.GetBody().GetMessage().GetTypeId()

	if !sub.Types.Has(typeID) {
		sub.Offset++
		return
	}

	select {
	case <-ctx.Done():
	case <-sub.Unsubscribed.Chan():
	case sub.Envelopes <- ev.Envelope:
		sub.Offset++
	default:
		select {
		case <-ctx.Done():
		case r.Lagged <- sub:
			delete(r.subscribers, sub)
		}
	}
}

func (r *contemporaryReader) broadcast(ctx context.Context, ev event) {
	for sub := range r.subscribers {
		r.unicast(ctx, sub, ev)
	}
}
