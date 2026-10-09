package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/backoff"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

type contemporaryReader struct {
	StreamID      *uuidpb.UUID
	DB            *sql.DB
	Notifications *notification.Listener
	Logger        *slog.Logger

	Lagging chan<- *readSubscriber

	once       sync.Once
	sub, unsub chan *readSubscriber

	backoff      backoff.Backoff
	nextOffset   uint64
	recentEvents [10]*envelopepb.Envelope
	index, size  int
	subscribers  map[*readSubscriber]struct{}
}

func (r *contemporaryReader) Run(ctx context.Context) {
	appends := make(chan string, 1)
	unsubscribe := r.Notifications.Subscribe("eventstream.append."+r.StreamID.AsString(), appends)
	defer unsubscribe()

	for {
		err := r.load(ctx)

		if err == nil {
			break
		}

		if ctx.Err() != nil {
			return
		}

		r.Logger.ErrorContext(
			ctx,
			"unable to load events",
			xslog.Error(err),
		)

		select {
		case <-ctx.Done():
			return
		case <-r.backoff.Chan():
		case <-appends:
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
		case payload := <-appends:
			r.update(ctx, payload)
			// case sub := <-o.sub:
			// o.subscribe(ctx, sub)
			// case sub := <-o.unsub:
			// delete(o.subscribers, sub)
		}
	}
}

func (r *contemporaryReader) load(ctx context.Context) error {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT * FROM (
			SELECT
				envelope,
				stream_offset
			FROM eventstream.events
			WHERE stream_id = $1
			ORDER BY stream_offset DESC
			LIMIT $2
		) AS latest
		ORDER BY stream_offset ASC`,
		xsql.UUID(r.StreamID),
		len(r.recentEvents),
	)
	if err != nil {
		return fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			envelope = &envelopepb.Envelope{}
			offset   uint64
		)

		if err := rows.Scan(
			xsql.Envelope(envelope),
			&offset,
		); err != nil {
			return fmt.Errorf("unable to scan event: %w", err)
		}

		r.pushRecent(envelope, offset)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate events: %w", err)
	}

	return nil
}

func (r *contemporaryReader) update(ctx context.Context, payload string) error {
	offset, err := strconv.ParseUint(payload, 10, 64)
	if err != nil {
		r.Logger.ErrorContext(
			ctx,
			"ignored invalid offset in append notification",
			slog.String("offset", payload),
			xslog.Error(err),
		)
		return nil
	}

	if offset <= r.nextOffset {
		// We already knew about this offset.
		return nil
	}

	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT
			envelope,
			stream_offset
		FROM eventstream.events
		WHERE stream_id = $1
		AND stream_offset >= $2
		ORDER BY stream_offset`,
		xsql.UUID(r.StreamID),
		r.nextOffset,
	)
	if err != nil {
		return fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	var envelopes []*envelopepb.Envelope

	for rows.Next() {
		var (
			envelope = &envelopepb.Envelope{}
			offset   uint64
		)

		if err := rows.Scan(
			xsql.Envelope(envelope),
			&offset,
		); err != nil {
			return fmt.Errorf("unable to scan event: %w", err)
		}

		r.pushRecent(envelope, offset)
		envelopes = append(envelopes, envelope)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate events: %w", err)
	}

	r.dispatch(ctx, envelopes)

	return nil
}

func (r *contemporaryReader) pushRecent(envelope *envelopepb.Envelope, offset uint64) {
	envelopepb.SetExtension(
		envelope.GetBody(),
		envelopepb.NewEventStreamPositionBuilder().
			WithStreamId(r.StreamID).
			WithOffset(offset).
			Build(),
	)

	r.nextOffset = offset + 1
	r.recentEvents[r.index] = envelope
	r.index = (r.index + 1) % len(r.recentEvents)

	if r.size < len(r.recentEvents) {
		r.size++
	}
}

func (r *contemporaryReader) dispatch(
	ctx context.Context,
	envelopes []*envelopepb.Envelope,
) {
	var group sync.WaitGroup

	for sub := range r.subscribers {
		group.Go(func() {
			for _, envelope := range envelopes {
				typeID := envelope.GetBody().GetMessage().GetTypeId()

				if sub.Types.Has(typeID) {
					select {
					case <-ctx.Done():
					case <-sub.Unsubscribed.Chan():
					case sub.Events <- envelope:
						sub.Offset++
					default:
						select {
						case <-ctx.Done():
						case <-sub.Unsubscribed.Chan():
						case r.Lagging <- sub:
							delete(r.subscribers, sub)
							return
						}
					}
				}
			}
		})
	}

	group.Wait()
}
