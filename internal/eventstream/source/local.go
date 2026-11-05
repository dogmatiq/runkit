package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"golang.org/x/sync/errgroup"
)

// Local is a [Source] of the event streams in this application's database.
type Local struct {
	// DB is the database connection used to discover event streams and read
	// events.
	DB *sql.DB

	// Signals is the bus used to receive signals that event streams have been
	// created and that events have been appended to them.
	Signals *signal.Bus

	// Logger is the target for log messages produced by the source.
	Logger *slog.Logger

	// SegmentSize is the maximum number of events delivered in one segment,
	// which is also the number of events read per query. It must be positive.
	SegmentSize int
}

// Watch delivers events to fn as they are appended to the application's
// streams.
//
// Each stream is delivered from its end, as it stands when the stream is
// discovered. It returns an error if any stream can not be delivered any
// further.
func (s *Local) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(Segment) error,
) error {
	panicIfEmpty(types)

	if s.SegmentSize <= 0 {
		panic("segment size must be positive")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// The first stream that can not be delivered any further ends the call,
	// since its delivery can not continue contiguously.
	group, ctx := errgroup.WithContext(ctx)

	err := s.discover(
		ctx,
		func(streamID *uuidpb.UUID) {
			group.Go(func() error {
				if err := s.watchStream(ctx, streamID, types, fn); ctx.Err() == nil {
					return err
				}
				return nil
			})
		},
	)

	cancel()

	if err := group.Wait(); err != nil {
		return err
	}

	return err
}

// Read delivers events from a stream to fn, beginning at the given offset.
func (s *Local) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) error {
	panicIfEmpty(types)

	if s.SegmentSize <= 0 {
		panic("segment size must be positive")
	}

	query := s.buildSegmentQuery(streamID, types)

	for {
		seg, exhausted, err := s.loadSegment(ctx, query, streamID, begin)
		if err != nil {
			return err
		}

		// A segment that does not advance the offset tells the consumer
		// nothing, so it is not delivered.
		if seg.End > seg.Begin {
			if err := fn(seg); err != nil {
				return err
			}

			begin = seg.End
		}

		if exhausted {
			return nil
		}
	}
}

// discover reports each event stream to fn as it is discovered.
//
// It reads the set of event streams, and reads it again each time a stream is
// created, until ctx is canceled.
func (s *Local) discover(
	ctx context.Context,
	fn func(streamID *uuidpb.UUID),
) error {
	known := uuidcollection.NewSet()

	for range s.Signals.Watch(ctx, "eventstream.create", 1) {
		streamIDs, err := s.loadStreamIDs(ctx)
		if err != nil {
			return err
		}

		for streamID := range streamIDs.Diff(known).All() {
			s.Logger.DebugContext(
				ctx,
				"event stream discovered",
				xslog.UUID("stream_id", streamID),
			)

			known.Add(streamID)
			fn(streamID)
		}
	}

	return ctx.Err()
}

// loadStreamIDs returns the IDs of all event streams.
func (s *Local) loadStreamIDs(ctx context.Context) (*uuidcollection.Set, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		`SELECT id
		FROM eventstream.streams`,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	streamIDs := uuidcollection.NewSet()

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

// watchStream delivers segments of a stream to fn as events are appended to it.
//
// It begins at the end of the stream. It returns when ctx is canceled, when fn
// returns an error, which it returns unchanged, or when it is unable to
// deliver the stream any further.
func (s *Local) watchStream(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	fn func(Segment) error,
) error {
	query := s.buildSegmentQuery(streamID, types)

	var (
		next  uint64
		first = true
		err   error
	)

	for range s.Signals.Watch(ctx, "eventstream.append."+streamID.AsString(), 1) {
		if first {
			if next, err = s.loadWatchOffset(ctx, streamID); err != nil {
				return fmt.Errorf("unable to watch event stream %s: %w", streamID, err)
			}
		}

		for {
			seg, exhausted, err := s.loadSegment(ctx, query, streamID, next)
			if err != nil {
				return fmt.Errorf("unable to watch event stream %s: %w", streamID, err)
			}

			// The first segment is delivered even if it is empty, so that the
			// stream is delivered even when there are no events.
			if seg.End == seg.Begin && !first {
				break
			}

			first = false

			if err := fn(seg); err != nil {
				return err
			}

			next = seg.End

			if exhausted {
				break
			}
		}
	}

	return ctx.Err()
}

// loadWatchOffset returns the offset from which watching a stream begins,
// which is the end of the stream.
func (s *Local) loadWatchOffset(
	ctx context.Context,
	streamID *uuidpb.UUID,
) (uint64, error) {
	row := s.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(streamID),
	)

	var next uint64
	if err := row.Scan(&next); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, fmt.Errorf("event stream %s not found", streamID)
		}

		return 0, fmt.Errorf("unable to scan next stream offset: %w", err)
	}

	return next, nil
}

// buildSegmentQuery returns the SQL used to load a segment of a stream.
//
// The query returns two kinds of rows via UNION ALL: the first row contains
// the stream's next offset (and a NULL envelope); subsequent rows contain up
// to SegmentSize events of the given types, or of every type if types is nil.
//
// The stream ID, message type IDs and segment size are embedded as constants so
// that PostgreSQL knows them when planning the query. In particular, it can
// choose between scanning events in offset order and searching directly for
// specific message types. Only the offset is a parameter, as it changes
// between reads.
func (s *Local) buildSegmentQuery(
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
) string {
	var filter string

	if types != nil {
		var typeIDs []string
		for id := range types.All() {
			typeIDs = append(typeIDs, id.AsString())
		}

		// The IDs are sorted so that the same types always produce the same
		// query, which is then prepared only once per connection.
		slices.Sort(typeIDs)

		filter = fmt.Sprintf(
			"AND message_type_id IN ('%s')",
			strings.Join(typeIDs, "','"),
		)
	}

	return fmt.Sprintf(
		`SELECT stream_offset, envelope
		FROM (
			(
				SELECT
					0 AS position,
					next_offset AS stream_offset,
					NULL::bytea AS envelope
				FROM eventstream.streams
				WHERE id = '%s'
			)
			UNION ALL
			(
				SELECT
					1 AS position,
					stream_offset,
					envelope
				FROM eventstream.events
				WHERE stream_id = '%s'
					AND stream_offset >= $1
					%s
				ORDER BY stream_offset
				LIMIT %d
			)
		) AS results
		ORDER BY position, stream_offset`,
		streamID.AsString(),
		streamID.AsString(),
		filter,
		s.SegmentSize,
	)
}

// loadSegment loads a segment of a stream, beginning at the given offset, using
// a query built by [Local.buildSegmentQuery].
//
// The segment holds up to SegmentSize matching events, and ends after the last
// of them. It also returns whether the stream has been exhausted, that is,
// there are no more matching events on the stream at this time. When the
// stream is exhausted, the segment ends at the stream's end, skipping past any
// trailing events of other types, unless the given offset is already beyond
// it.
func (s *Local) loadSegment(
	ctx context.Context,
	query string,
	streamID *uuidpb.UUID,
	begin uint64,
) (Segment, bool, error) {
	rows, err := s.DB.QueryContext(ctx, query, begin)
	if err != nil {
		return Segment{}, false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// The first row is always the stream's next offset. If there are no rows
	// at all, the stream does not exist.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Segment{}, false, fmt.Errorf("unable to advance to first row: %w", err)
		}

		return Segment{}, false, fmt.Errorf("event stream %s not found", streamID)
	}

	var next uint64
	if err := rows.Scan(
		&next,
		new(any),
	); err != nil {
		return Segment{}, false, fmt.Errorf("unable to scan next offset: %w", err)
	}

	seg := Segment{
		StreamID: streamID,
		Begin:    begin,
		End:      begin,
	}

	for rows.Next() {
		var (
			offset uint64
			env    = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&offset,
			xsql.Envelope(env),
		); err != nil {
			return Segment{}, false, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(streamID).
				WithOffset(offset).
				Build(),
		)

		seg.Envelopes = append(seg.Envelopes, env)
		seg.End = offset + 1
	}

	if err := rows.Err(); err != nil {
		return Segment{}, false, fmt.Errorf("unable to iterate events: %w", err)
	}

	exhausted := len(seg.Envelopes) < s.SegmentSize

	if exhausted {
		seg.End = max(seg.End, next)
	}

	return seg, exhausted, nil
}
