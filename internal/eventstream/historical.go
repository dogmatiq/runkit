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

const historicalBatchSize = 50

type historicalReader struct {
	DB         *sql.DB
	Subscriber *readSubscriber
	Logger     *slog.Logger

	Done chan<- *readSubscriber

	backoff backoff.Backoff
}

func (r *historicalReader) Run(ctx context.Context) {
	defer func() {
		select {
		case <-ctx.Done():
		case r.Done <- r.Subscriber:
		}
	}()

	for {
		more, err := r.read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			r.Logger.ErrorContext(
				ctx,
				"unable to read historical events",
				xslog.UUID("stream_id", r.Subscriber.StreamID),
				xslog.Error(err),
			)

			if !r.backoff.Wait(ctx) {
				return
			}
		} else if !more {
			return
		}
	}
}

func (r *historicalReader) read(ctx context.Context) (bool, error) {
	rows, err := r.DB.QueryContext(
		ctx,
		`SELECT
			envelope,
			stream_offset
		FROM eventstream.events
		WHERE stream_id = $1
		AND stream_offset >= $2
		AND message_type_id = ANY($3)
		ORDER BY stream_offset ASC
		LIMIT $4`,
		xsql.UUID(r.Subscriber.StreamID),
		r.Subscriber.Offset,
		xsql.UUIDSeq(r.Subscriber.Types.All()),
		historicalBatchSize,
	)
	if err != nil {
		return false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	count := 0

	for rows.Next() {
		var (
			envelope = &envelopepb.Envelope{}
			offset   uint64
		)

		if err := rows.Scan(
			xsql.Envelope(envelope),
			&offset,
		); err != nil {
			return false, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			envelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(r.Subscriber.StreamID).
				WithOffset(offset).
				Build(),
		)

		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-r.Subscriber.Unsubscribed.Chan():
			return false, nil
		case r.Subscriber.Events <- envelope:
			r.Subscriber.Offset = offset + 1
			count++
		}
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("unable to iterate events: %w", err)
	}

	return count == historicalBatchSize, nil
}
