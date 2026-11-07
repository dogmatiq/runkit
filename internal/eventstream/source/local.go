package source

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Local is a [Source] of the event streams in this application's database.
//
// [Local.Run] follows each stream once, however many calls to [Local.Watch]
// are in progress, and delivers each segment that it loads to every one of
// those calls.
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

	// BufferSize is the number of segments buffered for each call to
	// [Local.Watch]. A call that falls further behind than this ends with an
	// error that wraps [ErrNotContiguous]. It must be positive.
	BufferSize int

	// m guards offsets and watchers.
	m sync.Mutex

	// offsets maps the ID of each watched stream to the offset at which its
	// next segment begins, which is the end of the last segment delivered. A
	// call to [Local.Watch] that begins now begins the stream at that offset.
	offsets uuidcollection.Map[uint64]

	// watchers is the set of calls to [Local.Watch] that are receiving
	// segments.
	watchers map[*watcher]struct{}
}

// watcher is a call to [Local.Watch] that is receiving segments.
type watcher struct {
	// Segments receives each segment of every stream, until it is closed
	// because the call can no longer be served.
	Segments chan Segment

	// Err is the reason that the call can no longer be served. It is set
	// before Segments is closed.
	Err error
}

// Run watches the application's streams on behalf of every call to
// [Local.Watch], until ctx is canceled.
//
// It must be called at most once. [Local.Watch] may be called before it, and
// the call is served once it begins.
func (s *Local) Run(ctx context.Context) error {
	if s.SegmentSize <= 0 {
		panic("segment size must be positive")
	}

	group, ctx := supervisor.WithContext(ctx)
	known := uuidcollection.NewSet()

	// Discovery is retried on its own, so that a failure to discover new
	// streams does not stop following those already known.
	err := supervisor.Run(
		ctx,
		s.Logger,
		func(ctx context.Context) error {
			return s.discover(
				ctx,
				known,
				func(streamID *uuidpb.UUID) {
					group.Go(
						s.Logger.With(
							xslog.UUID("stream_id", streamID),
						),
						func(ctx context.Context) error {
							return s.watchStream(ctx, streamID)
						},
					)
				},
			)
		},
	)

	if err := group.Wait(); err != nil {
		return err
	}

	return err
}

// Watch delivers events to fn as they are appended to the application's
// streams.
//
// Each stream is delivered from its end, as it stands when the call begins or
// when the stream is discovered. It returns an error that wraps
// [ErrNotContiguous] if fn falls more than [Local.BufferSize] segments
// behind, and another error if any stream can not be delivered any further.
// The call is served by [Local.Run].
func (s *Local) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(Segment) error,
) error {
	panicIfEmpty(types)

	if s.BufferSize <= 0 {
		panic("buffer size must be positive")
	}

	w, openings := s.register()
	defer s.unregister(w)

	for _, seg := range openings {
		if err := fn(seg); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case seg, ok := <-w.Segments:
			if !ok {
				return w.Err
			}

			if err := fn(filterSegment(seg, types)); err != nil {
				return err
			}
		}
	}
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

// discover reports each event stream that is not in known to fn, and adds it
// to known.
//
// It reads the set of event streams, and reads it again each time a stream is
// created, until ctx is canceled.
func (s *Local) discover(
	ctx context.Context,
	known *uuidcollection.Set,
	fn func(streamID *uuidpb.UUID),
) error {
	signals, stop := s.Signals.Watch(ctx, "eventstream.create", 1)
	defer stop()

	for range signals {
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

// register adds a watcher, and returns it with the opening segment of each
// watched stream, which is empty and begins at that stream's offset in
// offsets.
func (s *Local) register() (*watcher, []Segment) {
	s.m.Lock()
	defer s.m.Unlock()

	w := &watcher{
		Segments: make(chan Segment, s.BufferSize),
	}

	if s.watchers == nil {
		s.watchers = map[*watcher]struct{}{}
	}

	s.watchers[w] = struct{}{}

	var openings []Segment

	for streamID, offset := range s.offsets.All() {
		openings = append(
			openings,
			Segment{
				StreamID: streamID,
				Begin:    offset,
				End:      offset,
			},
		)
	}

	return w, openings
}

// unregister removes a watcher, unless it has already been removed.
func (s *Local) unregister(w *watcher) {
	s.m.Lock()
	defer s.m.Unlock()

	delete(s.watchers, w)
}

// deliver sends a segment to every watcher, and records the segment's end in
// offsets.
//
// A watcher whose buffer is full is removed, as the segment's stream can no
// longer be delivered to it contiguously.
func (s *Local) deliver(seg Segment) {
	s.m.Lock()
	defer s.m.Unlock()

	s.offsets.Set(seg.StreamID, seg.End)

	for w := range s.watchers {
		select {
		case w.Segments <- seg:
		default:
			s.remove(
				w,
				fmt.Errorf("unable to deliver event stream %s: %w", seg.StreamID, ErrNotContiguous),
			)
		}
	}
}

// fail removes every watcher, as the given stream can no longer be delivered to
// any of them contiguously, and removes the stream from offsets until
// watchStream begins following it again.
func (s *Local) fail(streamID *uuidpb.UUID, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	s.offsets.Delete(streamID)

	for w := range s.watchers {
		s.remove(w, err)
	}
}

// remove removes a watcher, recording the given reason that it can no longer be
// served, and closes its channel.
//
// It must be called with s.m held.
func (s *Local) remove(w *watcher, err error) {
	delete(s.watchers, w)
	w.Err = err
	close(w.Segments)
}

// watchStream follows a stream, delivering its segments to every watcher as
// events are appended to it, beginning at the end of the stream.
//
// It returns when ctx is canceled or it is unable to continue. In the second
// case it ends every call to [Local.Watch], because the next attempt begins at
// the end of the stream, which may be after events that those calls have not
// received.
func (s *Local) watchStream(ctx context.Context, streamID *uuidpb.UUID) error {
	err := s.deliverStream(ctx, streamID)

	if ctx.Err() != nil {
		return ctx.Err()
	}

	err = fmt.Errorf("unable to watch event stream %s: %w", streamID, err)
	s.fail(streamID, err)

	return err
}

// deliverStream delivers the segments of a stream to every watcher as events
// are appended to it, beginning at the end of the stream, until ctx is
// canceled or it is unable to continue.
//
// It begins by delivering the stream's opening segment, which is empty, so that
// every watcher registered before the stream was discovered learns that the
// stream exists and where its delivery begins.
func (s *Local) deliverStream(ctx context.Context, streamID *uuidpb.UUID) error {
	query := s.buildSegmentQuery(streamID, nil)

	var (
		next   uint64
		opened bool
	)

	signals, stop := s.Signals.Watch(ctx, "eventstream.append."+streamID.AsString(), 1)
	defer stop()

	for range signals {
		if !opened {
			end, err := s.loadWatchOffset(ctx, streamID)
			if err != nil {
				return err
			}

			next = end
			opened = true

			s.deliver(
				Segment{
					StreamID: streamID,
					Begin:    next,
					End:      next,
				},
			)
		}

		for {
			seg, exhausted, err := s.loadSegment(ctx, query, streamID, next)
			if err != nil {
				return err
			}

			if seg.End > seg.Begin {
				s.deliver(seg)
				next = seg.End
			}

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

		return Segment{}, false, fmt.Errorf("unable to read event stream %s: %w", streamID, ErrUnrecognizedStream)
	}

	var next uint64
	if err := rows.Scan(
		&next,
		new(any),
	); err != nil {
		return Segment{}, false, fmt.Errorf("unable to scan next offset: %w", err)
	}

	if begin > next {
		return Segment{}, false, fmt.Errorf(
			"unable to load event stream %s from offset %d, as it ends at offset %d: %w",
			streamID,
			begin,
			next,
			ErrOffsetOutOfRange,
		)
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

// filterSegment returns seg with only the events of the given types. A nil set
// of types means every type.
func filterSegment(seg Segment, types *uuidcollection.Set) Segment {
	if types == nil {
		return seg
	}

	var envelopes []*envelopepb.Envelope

	for _, env := range seg.Envelopes {
		if types.Has(env.GetBody().GetMessage().GetTypeId()) {
			envelopes = append(envelopes, env)
		}
	}

	seg.Envelopes = envelopes

	return seg
}
