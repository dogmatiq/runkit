package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/backoff"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const historicalBatchSize = 10

type historicalReader struct {
	DB           *sql.DB
	Subscription *readerSubscriber
	Synced       chan<- *readerSubscriber
	Stop         context.CancelFunc
	Logger       *slog.Logger

	backoff backoff.Backoff
	buffer  [historicalBatchSize]event
}

func (r *historicalReader) Run(ctx context.Context) {
	for {
		envelopes, ok := r.readBatch(ctx)
		if !ok {
			return
		}

		if !r.dispatch(ctx, envelopes) {
			return
		}

		// If we received fewer envelopes than our buffer allows, we have
		// successfully synced with the end of the stream.
		if len(envelopes) < cap(r.buffer) {
			select {
			case <-ctx.Done():
			case r.Synced <- r.Subscription:
			}

			return
		}
	}
}

func (r *historicalReader) dispatch(
	ctx context.Context,
	events []event,
) bool {
	for _, ev := range events {
		select {
		case <-ctx.Done():
			return false
		case <-r.Subscription.Unsubscribed.Chan():
			return false
		case r.Subscription.Envelopes <- ev.Envelope:
			r.Subscription.Offset = ev.Offset + 1
		}
	}

	return true
}

func (r *historicalReader) readBatch(ctx context.Context) ([]event, bool) {
	for {
		events, err := r.tryReadBatch(ctx)
		if err == nil {
			return events, true
		}

		r.Logger.ErrorContext(
			ctx,
			"unable to read historical event batch",
			xslog.Error(err),
		)

		if !r.backoff.Wait(ctx) {
			return nil, false
		}
	}
}

func (r *historicalReader) tryReadBatch(ctx context.Context) ([]event, error) {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT
			stream_offset,
			envelope
		FROM eventstream.events
		WHERE stream_id = $1
		AND stream_offset >= $2
		AND message_type_id = ANY($3)
		ORDER BY stream_offset ASC
		LIMIT $4`,
		xsql.UUID(r.Subscription.StreamID),
		r.Subscription.Offset,
		xsql.UUIDSeq(r.Subscription.Types.All()),
		cap(r.buffer),
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	count := 0

	for rows.Next() {
		ev := event{
			Envelope: &envelopepb.Envelope{},
		}

		if err := rows.Scan(
			&ev.Offset,
			xsql.Envelope(ev.Envelope),
		); err != nil {
			return nil, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			ev.Envelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(r.Subscription.StreamID).
				WithOffset(ev.Offset).
				Build(),
		)

		r.buffer[count] = ev
		count++
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate events: %w", err)
	}

	return r.buffer[:count], nil
}
