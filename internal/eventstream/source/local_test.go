package source_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/dogmatiq/enginekit/enginetest/stubs"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster"
	. "github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestLocal(t *testing.T) {
	typesE := uuidcollection.NewSet(
		MessageTypeUUID[*EventStub[TypeE]](),
	)

	// newSource returns a local source over the given database, with the
	// given segment and buffer sizes. It does not run the source.
	newSource := func(t *testing.T, db *sql.DB, segmentSize, bufferSize int) *Local {
		logger := spruce.NewTestLogger(t)

		signals := &signal.Bus{
			DB:           db,
			EngineLogger: logger,
		}

		xtesting.GoE(t, signals.Run)

		node := &cluster.Node{
			ID:                uuidpb.Generate(),
			DB:                db,
			Signals:           signals,
			HeartbeatInterval: 20 * time.Millisecond,
			HeartbeatTimeout:  100 * time.Millisecond,
			EngineLogger:      logger,
		}

		xtesting.GoE(t, node.Run)

		src := &Local{
			DB:           db,
			Node:         node,
			EngineLogger: logger,
			SegmentSize:  segmentSize,
			BufferSize:   bufferSize,
		}

		return src
	}

	// setup returns a running local source over a new database, with the given
	// batch size, and the database.
	setup := func(t *testing.T, batchSize int) (*Local, *sql.DB) {
		db := xtesting.NewDatabase(t)
		src := newSource(t, db, batchSize, 100)
		xtesting.GoE(t, src.Run)
		return src, db
	}

	// watchBlocked runs src.Watch on another goroutine with an fn that sends
	// each segment to the returned channel, then blocks until the returned
	// function is called. It also returns a channel that receives Watch's
	// error.
	watchBlocked := func(t *testing.T, src *Local) (<-chan Segment, func(), <-chan error) {
		t.Helper()

		segments := make(chan Segment, 100)
		release := make(chan struct{})
		result := make(chan error, 1)

		go func() {
			result <- src.Watch(
				t.Context(),
				nil,
				func(seg Segment) error {
					segments <- seg
					<-release
					return nil
				},
			)
		}()

		return segments, sync.OnceFunc(func() { close(release) }), result
	}

	// expectSegment waits for a segment to be delivered to the given channel,
	// and asserts that it is the given range of the given stream.
	expectSegment := func(
		t testing.TB,
		segments <-chan Segment,
		streamID *uuidpb.UUID,
		begin, end uint64,
	) {
		t.Helper()

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for segment [%d, %d) of stream %s", begin, end, streamID)
		case seg := <-segments:
			if !seg.StreamID.Equal(streamID) || seg.Begin != begin || seg.End != end {
				t.Fatalf(
					"unexpected segment: got [%d, %d) of stream %s, want [%d, %d) of stream %s",
					seg.Begin,
					seg.End,
					seg.StreamID,
					begin,
					end,
					streamID,
				)
			}
		}
	}

	// expectError waits for an error on the given channel and returns it.
	expectError := func(t testing.TB, result <-chan error) error {
		t.Helper()

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatal("timeout waiting for Watch to return")
			return nil
		case err := <-result:
			return err
		}
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

	// failedAttempts is the number of consecutive attempts to run a query that
	// a test fails so that the query fails once. database/sql retries a query
	// that fails before it is sent, up to two more times.
	const failedAttempts = 3

	// isSegmentQuery returns true if the given SQL loads a segment of a stream.
	isSegmentQuery := func(query string) bool {
		return strings.HasPrefix(
			strings.Join(strings.Fields(query), " "),
			"SELECT stream_offset, envelope FROM",
		)
	}

	t.Run("Watch", func(t *testing.T) {
		t.Run("it delivers each stream from its end", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			src := newSource(t, db, 10, 100)
			streamID := xtesting.CreateEventStream(t, db)

			xtesting.AppendToEventStream(
				t,
				db,
				streamID,
				EventE1,
				EventE2,
				EventE3,
			)

			// The source runs after the events are appended, so that its
			// watch of the stream begins after all of them.
			xtesting.GoE(t, src.Run)
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

		t.Run("it keeps a watch call open while it can not continue delivering a stream", func(t *testing.T) {
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
			xtesting.Transact(t, db, func(tx *sql.Tx) {
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
			})

			select {
			case err := <-result:
				t.Fatalf("unexpected return from Watch: %v", err)
			case <-time.After(time.Second):
			}
		})

		t.Run("it resumes a stream from where delivery stopped after a failure", func(t *testing.T) {
			var remaining atomic.Int32

			db := xtesting.NewDatabaseWithFailingQueries(
				t,
				func(query string, _ []any) bool {
					return isSegmentQuery(query) && remaining.Add(-1) >= 0
				},
			)

			src := newSource(t, db, 10, 100)
			xtesting.GoE(t, src.Run)
			streamID := xtesting.CreateEventStream(t, db)

			// The watch fails the test if it returns an error.
			segments, _ := watch(t, src, nil)
			expectEnvelopes(t, segments, streamID, 0, 0)

			remaining.Store(failedAttempts)
			events := xtesting.AppendToEventStream(t, db, streamID, EventE1, EventE2, EventE3)

			xtesting.WaitFor(
				t,
				"delivery to fail",
				func() bool {
					return remaining.Load() <= 0
				},
			)

			events = append(events, xtesting.AppendToEventStream(t, db, streamID, EventE1)...)

			expectEnvelopes(t, segments, streamID, 0, 4, events...)
		})

		t.Run("it ends watch calls when the gap after a failure exceeds the buffer", func(t *testing.T) {
			var remaining atomic.Int32

			db := xtesting.NewDatabaseWithFailingQueries(
				t,
				func(query string, _ []any) bool {
					return isSegmentQuery(query) && remaining.Add(-1) >= 0
				},
			)

			// Each call can buffer 2 segments of 1 event each.
			src := newSource(t, db, 1, 2)
			xtesting.GoE(t, src.Run)
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
				t.Fatal("timeout waiting for the stream's opening segment")
			case <-began:
			}

			remaining.Store(failedAttempts)
			xtesting.AppendToEventStream(t, db, streamID, EventE1, EventE2, EventE3, EventE1, EventE2)

			if err := expectError(t, result); !errors.Is(err, ErrNotContiguous) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrNotContiguous)
			}
		})

		t.Run("it opens a stream that failed before its first delivery as a new stream", func(t *testing.T) {
			var remaining atomic.Int32
			remaining.Store(failedAttempts)

			// Opening a stream is the only query that reads one stream's next
			// offset on its own.
			db := xtesting.NewDatabaseWithFailingQueries(
				t,
				func(query string, _ []any) bool {
					return strings.Join(strings.Fields(query), " ") == "SELECT next_offset FROM eventstream.streams WHERE id = $1" &&
						remaining.Add(-1) >= 0
				},
			)

			src := newSource(t, db, 10, 100)
			xtesting.GoE(t, src.Run)

			// The watch fails the test if it returns an error.
			segments, _ := watch(t, src, nil)

			streamID := xtesting.CreateEventStream(t, db)
			xtesting.AppendToEventStream(t, db, streamID, EventE1, EventE2)

			expectSegment(t, segments, streamID, 2, 2)
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

		t.Run("it delivers each segment to every watcher", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			first, _ := watch(t, src, nil)
			second, _ := watch(t, src, typesE)

			expectEnvelopes(t, first, streamID, 0, 0)
			expectEnvelopes(t, second, streamID, 0, 0)

			all := xtesting.AppendToEventStream(t, db, streamID, EventE1, EventX1)

			expectEnvelopes(t, first, streamID, 0, 2, all...)
			expectEnvelopes(t, second, streamID, 0, 2, all[0])
		})

		t.Run("it ends the call of a watcher that falls behind, after delivering the segments buffered for it", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			src := newSource(t, db, 10, 1)
			xtesting.GoE(t, src.Run)
			streamID := xtesting.CreateEventStream(t, db)

			// The fast watcher shows when each segment has been sent to every
			// watcher.
			fast, _ := watch(t, src, nil)
			expectEnvelopes(t, fast, streamID, 0, 0)

			slow, release, result := watchBlocked(t, src)
			expectSegment(t, slow, streamID, 0, 0)

			// The first segment fills the slow watcher's buffer, and the second
			// does not fit.
			e1 := xtesting.AppendToEventStream(t, db, streamID, EventE1)
			expectEnvelopes(t, fast, streamID, 0, 1, e1...)
			e2 := xtesting.AppendToEventStream(t, db, streamID, EventE2)
			expectEnvelopes(t, fast, streamID, 1, 2, e2...)

			release()

			expectSegment(t, slow, streamID, 0, 1)

			if err := expectError(t, result); !errors.Is(err, ErrNotContiguous) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrNotContiguous)
			}
		})

		t.Run("it delivers a stream created while a watcher's buffer is full to that watcher's next call", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			src := newSource(t, db, 10, 1)
			xtesting.GoE(t, src.Run)
			existing := xtesting.CreateEventStream(t, db)

			fast, _ := watch(t, src, nil)
			expectEnvelopes(t, fast, existing, 0, 0)

			slow, release, result := watchBlocked(t, src)
			expectSegment(t, slow, existing, 0, 0)

			// The first new stream fills the slow watcher's buffer, and the
			// second does not fit.
			first := xtesting.CreateEventStream(t, db)
			expectEnvelopes(t, fast, first, 0, 0)
			second := xtesting.CreateEventStream(t, db)
			expectEnvelopes(t, fast, second, 0, 0)

			release()

			expectSegment(t, slow, first, 0, 0)

			if err := expectError(t, result); !errors.Is(err, ErrNotContiguous) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrNotContiguous)
			}

			// The next call begins with every stream, including the one that
			// did not fit.
			next, _ := watch(t, src, nil)

			want := uuidcollection.NewSet(existing, first, second)
			got := uuidcollection.NewSet()

			for range 3 {
				select {
				case <-time.After(xtesting.WaitTimeout):
					t.Fatal("timeout waiting for the next call's first segments")
				case seg := <-next:
					got.Add(seg.StreamID)
				}
			}

			if !got.IsEqual(want) {
				t.Fatalf("unexpected streams: got %s, want %s", got, want)
			}
		})

		t.Run("it ends only the call whose fn returns an error", func(t *testing.T) {
			src, db := setup(t, 10)
			streamID := xtesting.CreateEventStream(t, db)

			errRejected := errors.New("<rejected>")
			result := make(chan error, 1)

			go func() {
				result <- src.Watch(
					t.Context(),
					nil,
					func(Segment) error {
						return errRejected
					},
				)
			}()

			if err := expectError(t, result); err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}

			segments, _ := watch(t, src, nil)
			expectEnvelopes(t, segments, streamID, 0, 0)

			want := xtesting.AppendToEventStream(t, db, streamID, EventE1)
			expectEnvelopes(t, segments, streamID, 0, 1, want...)
		})

		t.Run("it serves a call made before the source runs once it runs", func(t *testing.T) {
			db := xtesting.NewDatabase(t)
			src := newSource(t, db, 10, 100)
			streamID := xtesting.CreateEventStream(t, db)

			segments, _ := watch(t, src, nil)
			xtesting.GoE(t, src.Run)

			expectEnvelopes(t, segments, streamID, 0, 0)
		})

		t.Run("it keeps delivering every stream when discovering streams fails", func(t *testing.T) {
			var (
				failing  atomic.Bool
				failures atomic.Int32
			)

			// Discovery is the only query that reads the IDs of every stream.
			db := xtesting.NewDatabaseWithFailingQueries(
				t,
				func(query string, _ []any) bool {
					if !failing.Load() {
						return false
					}

					if strings.Join(strings.Fields(query), " ") != "SELECT id FROM eventstream.streams" {
						return false
					}

					failures.Add(1)
					return true
				},
			)

			src := newSource(t, db, 10, 100)
			xtesting.GoE(t, src.Run)
			streamID := xtesting.CreateEventStream(t, db)

			// The watch fails the test if it returns an error.
			segments, _ := watch(t, src, nil)
			expectEnvelopes(t, segments, streamID, 0, 0)

			// Discovery reads the IDs of every stream when a stream is created.
			failing.Store(true)
			xtesting.CreateEventStream(t, db)

			xtesting.WaitFor(
				t,
				"discovery to fail",
				func() bool {
					return failures.Load() > 0
				},
			)

			events := xtesting.AppendToEventStream(t, db, streamID, EventE1)
			expectEnvelopes(t, segments, streamID, 0, 1, events...)
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
				DB:           db,
				EngineLogger: spruce.NewTestLogger(t),
				SegmentSize:  10,
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
