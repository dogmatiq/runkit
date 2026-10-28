package scanner

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Contemporary reads contemporary (near real-time) events from a specific event
// stream as they are appended, and sends them to a channel in batches.
//
// The events within and across batches are contiguous: each batch begins at the
// offset immediately after the previous batch's checkpoint, except when the
// scanner has restarted. The offset of a batch's first event is therefore its
// checkpoint less the number of events it contains.
type Contemporary struct {
	// DB is the database connection used to read events from the stream.
	DB *sql.DB

	// Signals is the listener used to receive signals that events have been
	// appended to the stream.
	Signals *signal.Listener

	// StreamID is the ID of the event stream being read.
	StreamID *uuidpb.UUID

	// Batches is the channel on which batches of events are sent as they are
	// read.
	Batches chan<- Batch

	// BatchSize is the maximum number of events read per query. It must be
	// positive.
	//
	// Each time Run is called, reading begins BatchSize events before the end
	// of the stream, so that the most recent BatchSize events are sent first.
	BatchSize int
}

// Run reads events from the stream and sends them to s.Batches until ctx is
// canceled or an error occurs.
//
// If Run returns an error it may be called again. It holds no state between
// calls; each call begins reading relative to the end of the stream at that
// time, so the first batch it sends is not necessarily contiguous with the last
// batch sent by the previous call. Run must not be called concurrently.
func (s *Contemporary) Run(ctx context.Context) error {
	if s.BatchSize <= 0 {
		panic("batch size must be positive")
	}

	// Subscribe to signals about new events _before_ reading the current state
	// of the stream. This ensures that no events are missed between the time
	// we read the stream's next offset and the time we start reading.
	appended := make(chan string, 1)
	topic := "eventstream.append." + s.StreamID.AsString()
	unsubscribe := s.Signals.Subscribe(topic, appended)
	defer unsubscribe()

	next, err := s.loadStartOffset(ctx)
	if err != nil {
		return err
	}

	// The first read always produces a batch, even if the stream is empty, so
	// that the stream's position is reported even when there are no events.
	next, err = s.catchUp(ctx, next, true)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-appended:
		}

		next, err = s.catchUp(ctx, next, false)
		if err != nil {
			return err
		}
	}
}

// loadStartOffset returns the offset from which reading begins, which is
// s.BatchSize events before the end of the stream, or the beginning of the
// stream if it is shorter than that.
func (s *Contemporary) loadStartOffset(ctx context.Context) (uint64, error) {
	row := s.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(s.StreamID),
	)

	var next uint64
	if err := row.Scan(&next); err != nil {
		return 0, fmt.Errorf("unable to scan next stream offset: %w", err)
	}

	if next > uint64(s.BatchSize) {
		return next - uint64(s.BatchSize), nil
	}

	return 0, nil
}

// catchUp reads events from the given offset until the end of the stream,
// sending each batch to s.Batches. It returns the offset from which to resume.
//
// If first is true, a batch is sent even if there are no events to read.
func (s *Contemporary) catchUp(ctx context.Context, next uint64, first bool) (uint64, error) {
	for {
		envelopes, err := s.readBatch(ctx, next)
		if err != nil {
			return 0, err
		}

		if len(envelopes) == 0 && !first {
			return next, nil
		}

		first = false
		next += uint64(len(envelopes))

		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case s.Batches <- Batch{
			StreamID:   s.StreamID,
			Envelopes:  envelopes,
			Checkpoint: next,
		}:
		}

		// A partial batch means there are no more events on the stream at this
		// time.
		if len(envelopes) < s.BatchSize {
			return next, nil
		}
	}
}

// readBatch returns up to s.BatchSize events starting at the given offset.
func (s *Contemporary) readBatch(ctx context.Context, offset uint64) ([]*envelopepb.Envelope, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		`SELECT
			stream_offset,
			envelope
		FROM eventstream.events
		WHERE stream_id = $1
			AND stream_offset >= $2
		ORDER BY stream_offset
		LIMIT $3`,
		xsql.UUID(s.StreamID),
		offset,
		s.BatchSize,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	var envelopes []*envelopepb.Envelope

	for rows.Next() {
		var (
			offset uint64
			env    = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&offset,
			xsql.Envelope(env),
		); err != nil {
			return nil, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(s.StreamID).
				WithOffset(offset).
				Build(),
		)

		envelopes = append(envelopes, env)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate events: %w", err)
	}

	return envelopes, nil
}
