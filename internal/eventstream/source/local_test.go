package source_test

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestLocal(t *testing.T) {
	typesE := uuidcollection.NewSet(
		MessageTypeUUID[*EventStub[TypeE]](),
	)

	// setup returns a local source over a new database, with the given batch
	// size, and the database.
	setup := func(t *testing.T, batchSize int) (*Local, *sql.DB) {
		db := xtesting.NewDatabase(t)
		logger := spruce.NewTestLogger(t)

		signals := &signal.Bus{
			DB:     db,
			Logger: logger,
		}

		xtesting.GoE(t, signals.Run)

		src := &Local{
			DB:          db,
			Signals:     signals,
			Logger:      logger,
			SegmentSize: batchSize,
		}

		return src, db
	}

	// watch runs src.Watch on another goroutine, delivering each segment to
	// the returned channel, and returns a function that stops it.
	watch := func(t *testing.T, src *Local, types *uuidcollection.Set) (<-chan Segment, func()) {
		t.Helper()

		segments := make(chan Segment, 100)

		stop := xtesting.GoE(
			t,
			func(ctx context.Context) error {
				return src.Watch(
					ctx,
					types,
					func(seg Segment) error {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case segments <- seg:
							return nil
						}
					},
				)
			},
		)

		return segments, stop
	}

	// read returns the segments that src.Read delivers.
	read := func(t *testing.T, src *Local, streamID *uuidpb.UUID, types *uuidcollection.Set, begin uint64) []Segment {
		t.Helper()

		var segments []Segment

		err := src.Read(
			t.Context(),
			streamID,
			types,
			begin,
			func(seg Segment) error {
				segments = append(segments, seg)
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		return segments
	}

	// expectEnvelopes asserts that the given envelopes are delivered, in
	// order, across one or more contiguous segments of the given stream, the
	// first of which begins at the given offset, and the last of which ends
	// at the given offset.
	expectEnvelopes := func(
		t testing.TB,
		segments <-chan Segment,
		streamID *uuidpb.UUID,
		begin, end uint64,
		want ...*envelopepb.Envelope,
	) {
		t.Helper()

		var got []*envelopepb.Envelope

		for {
			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatalf("timeout waiting for segments: got %d of %d envelopes", len(got), len(want))
			case seg := <-segments:
				if !seg.StreamID.Equal(streamID) {
					t.Fatalf("unexpected stream ID: got %s, want %s", seg.StreamID, streamID)
				}

				if seg.Begin != begin {
					t.Fatalf("unexpected segment begin: got %d, want %d", seg.Begin, begin)
				}

				begin = seg.End
				got = append(got, seg.Envelopes...)

				if seg.End == end {
					xtesting.Expect(t, "envelopes", got, want)
					return
				}

				if seg.End > end {
					t.Fatalf("unexpected segment end: got %d, want at most %d", seg.End, end)
				}
			}
		}
	}

	// expectNoSegment asserts that no segment is delivered within a short
	// period.
	expectNoSegment := func(t testing.TB, segments <-chan Segment) {
		t.Helper()

		select {
		case seg := <-segments:
			t.Fatalf("unexpected segment [%d, %d)", seg.Begin, seg.End)
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Run("Watch", func(t *testing.T) {
		t.Run("it delivers each stream from its end", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
			)

			segments, _ := watch(t, src, nil)

			expectEnvelopes(t, segments, streamID, 3, 3)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
			)

			expectEnvelopes(t, segments, streamID, 3, 4, want...)
		})

		t.Run("it delivers events appended after it begins", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			segments, _ := watch(t, src, nil)

			// The stream is empty when the source begins, so the empty segment
			// it delivers confirms that it is running before the events under
			// test are appended; otherwise it might begin relative to an end
			// of the stream that already includes them.
			expectEnvelopes(t, segments, streamID, 0, 0)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
			)

			expectEnvelopes(t, segments, streamID, 0, 2, want...)
		})

		t.Run("it delivers in several segments when events exceed the batch size", func(t *testing.T) {
			src, db := setup(t, 2)
			streamID := xtesting.CreateEventStream(t, db)

			segments, _ := watch(t, src, nil)

			expectEnvelopes(t, segments, streamID, 0, 0)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
				EventE1,
			)

			expectEnvelopes(t, segments, streamID, 0, 4, want...)
		})

		t.Run("it delivers streams created after it begins", func(t *testing.T) {
			src, db := setup(t, 10)

			segments, _ := watch(t, src, nil)

			streamID := xtesting.CreateEventStream(t, db)

			expectEnvelopes(t, segments, streamID, 0, 0)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
			)

			expectEnvelopes(t, segments, streamID, 0, 1, want...)
		})

		t.Run("it delivers only events of the given types", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			segments, _ := watch(t, src, typesE)

			expectEnvelopes(t, segments, streamID, 0, 0)

			all := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventX1,
				EventE2,
			)

			expectEnvelopes(t, segments, streamID, 0, 3, all[0], all[2])
		})

		t.Run("it returns fn's error unchanged, and stops watching every stream", func(t *testing.T) {
			src, db := setup(t, 10)
			xtesting.CreateEventStreams(t, db, 2)

			errRejected := errors.New("<rejected>")

			ctx, cancel := context.WithTimeout(t.Context(), xtesting.WaitTimeout)
			defer cancel()

			err := src.Watch(
				ctx,
				nil,
				func(Segment) error {
					return errRejected
				},
			)

			if err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}
		})

		t.Run("it returns an error when it can not continue delivering a stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			began := make(chan struct{})
			result := make(chan error, 1)

			go func() {
				var once sync.Once

				result <- src.Watch(
					t.Context(),
					nil,
					func(Segment) error {
						once.Do(func() { close(began) })
						return nil
					},
				)
			}()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for the stream's first segment")
			case <-began:
			}

			// An event whose envelope can not be decoded stops the stream from
			// being delivered any further.
			tx, err := db.BeginTx(t.Context(), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback()

			xtesting.AppendToEventStream(t, tx, streamID, EventE1)

			if _, err := tx.ExecContext(
				t.Context(),
				`UPDATE eventstream.events
				SET envelope = '\xff'
				WHERE stream_id = $1`,
				streamID.AsString(),
			); err != nil {
				t.Fatal(err)
			}

			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for Watch to return")
			case err := <-result:
				if err == nil {
					t.Fatal("expected an error")
				}
			}
		})

		t.Run("it panics when given an empty set of types", func(t *testing.T) {
			src := &Local{
				SegmentSize: 10,
			}

			defer func() {
				if got := recover(); got != "message types must not be empty" {
					t.Fatalf("unexpected panic: got %v, want %q", got, "message types must not be empty")
				}
			}()

			src.Watch(
				t.Context(),
				uuidcollection.NewSet(),
				func(Segment) error {
					return nil
				},
			)
		})

		t.Run("it delivers an empty segment for an empty stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			segments, _ := watch(t, src, nil)

			expectEnvelopes(t, segments, streamID, 0, 0)
			expectNoSegment(t, segments)
		})
	})

	t.Run("Read", func(t *testing.T) {
		t.Run("it delivers all matching events in one segment that ends at the end of the stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, typesE, 0),
				[]Segment{
					{
						StreamID:  streamID,
						Begin:     0,
						End:       3,
						Envelopes: want,
					},
				},
			)
		})

		t.Run("it delivers in several segments when events exceed the batch size", func(t *testing.T) {
			src, db := setup(t, 2)
			streamID := xtesting.CreateEventStream(t, db)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
				EventE1,
				EventE2,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, typesE, 0),
				[]Segment{
					{
						StreamID:  streamID,
						Begin:     0,
						End:       2,
						Envelopes: want[0:2],
					},
					{
						StreamID:  streamID,
						Begin:     2,
						End:       4,
						Envelopes: want[2:4],
					},
					{
						StreamID:  streamID,
						Begin:     4,
						End:       5,
						Envelopes: want[4:5],
					},
				},
			)
		})

		t.Run("it begins at the given offset", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, typesE, 1),
				[]Segment{
					{
						StreamID:  streamID,
						Begin:     1,
						End:       3,
						Envelopes: want[1:],
					},
				},
			)
		})

		t.Run("it skips events of other types, and ends at the end of the stream past them", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			// End with an event of another type so that the segment must end
			// past it to reach the end of the stream.
			all := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventX1,
				EventE2,
				EventX2,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, typesE, 0),
				[]Segment{
					{
						StreamID:  streamID,
						Begin:     0,
						End:       4,
						Envelopes: []*envelopepb.Envelope{all[0], all[2]},
					},
				},
			)
		})

		t.Run("it delivers every type if none are given", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			want := xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventX1,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, nil, 0),
				[]Segment{
					{
						StreamID:  streamID,
						Begin:     0,
						End:       2,
						Envelopes: want,
					},
				},
			)
		})

		t.Run("it delivers an empty segment that ends at the end of the stream when only events of other types exist", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventX1,
				EventX2,
			)

			xtesting.Expect(
				t,
				"segments",
				read(t, src, streamID, typesE, 0),
				[]Segment{
					{
						StreamID: streamID,
						Begin:    0,
						End:      2,
					},
				},
			)
		})

		t.Run("it delivers nothing from an empty stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			if segments := read(t, src, streamID, typesE, 0); len(segments) != 0 {
				t.Fatalf("unexpected number of segments: got %d, want 0", len(segments))
			}
		})

		t.Run("it delivers nothing from the end of the stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
			)

			if segments := read(t, src, streamID, typesE, 2); len(segments) != 0 {
				t.Fatalf("unexpected number of segments: got %d, want 0", len(segments))
			}
		})

		t.Run("it returns ErrOffsetOutOfRange for an offset beyond the end of the stream", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
			)

			err := src.Read(
				t.Context(),
				streamID,
				typesE,
				3,
				func(Segment) error {
					return nil
				},
			)

			if !errors.Is(err, ErrOffsetOutOfRange) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrOffsetOutOfRange)
			}
		})

		t.Run("it returns fn's error unchanged", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
			)

			errRejected := errors.New("<rejected>")

			err := src.Read(
				t.Context(),
				streamID,
				typesE,
				0,
				func(Segment) error {
					return errRejected
				},
			)

			if err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}
		})

		t.Run("it panics when given an empty set of types", func(t *testing.T) {
			src := &Local{
				SegmentSize: 10,
			}

			defer func() {
				if got := recover(); got != "message types must not be empty" {
					t.Fatalf("unexpected panic: got %v, want %q", got, "message types must not be empty")
				}
			}()

			src.Read(
				t.Context(),
				uuidpb.Generate(),
				uuidcollection.NewSet(),
				0,
				func(Segment) error {
					return nil
				},
			)
		})

		t.Run("it returns ErrUnrecognizedStream for a stream that does not exist", func(t *testing.T) {
			src, _ := setup(t, 10)
			streamID := uuidpb.Generate()

			err := src.Read(
				t.Context(),
				streamID,
				typesE,
				0,
				func(Segment) error {
					return nil
				},
			)

			if !errors.Is(err, ErrUnrecognizedStream) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrUnrecognizedStream)
			}
		})

		t.Run("it uses one prepared statement for repeated reads of the same types", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			streamID := xtesting.CreateEventStream(t, db)

			// Every query runs on the same connection, so that the statements
			// prepared on it can be counted.
			db.SetMaxOpenConns(1)

			src := &Local{
				DB:          db,
				Logger:      spruce.NewTestLogger(t),
				SegmentSize: 10,
			}

			types := uuidcollection.NewSet(
				uuidpb.Generate(),
				uuidpb.Generate(),
				uuidpb.Generate(),
				uuidpb.Generate(),
				uuidpb.Generate(),
			)

			for range 20 {
				read(t, src, streamID, types, 0)
			}

			var n int
			if err := db.QueryRowContext(
				t.Context(),
				`SELECT COUNT(*)
				FROM pg_prepared_statements
				WHERE statement LIKE '%FROM eventstream.events%'
					AND statement NOT LIKE '%pg_prepared_statements%'`,
			).Scan(&n); err != nil {
				t.Fatal(err)
			}

			if n != 1 {
				t.Fatalf("unexpected number of prepared statements: got %d, want 1", n)
			}
		})
	})
}
