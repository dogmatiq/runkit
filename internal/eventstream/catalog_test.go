package eventstream_test

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/x/xtesting"
	"github.com/dogmatiq/spruce"
)

func TestCatalog(t *testing.T) {
	var (
		streamID = uuidpb.Generate()
		typeA    = uuidpb.Generate()
		typeB    = uuidpb.Generate()
		typesA   = uuidcollection.NewSet(typeA)
	)

	// event returns an envelope for an event of the given type at the given
	// offset.
	event := func(offset uint64, typeID *uuidpb.UUID) *envelopepb.Envelope {
		env := envelopepb.NewEnvelopeBuilder().
			WithBody(
				envelopepb.NewBodyBuilder().
					WithMessage(
						envelopepb.NewMessageBuilder().
							WithTypeId(typeID).
							Build(),
					).
					Build(),
			).
			Build()

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(streamID).
				WithOffset(offset).
				Build(),
		)

		return env
	}

	// segment returns a segment of the stream with the given events.
	segment := func(begin, end uint64, envelopes ...*envelopepb.Envelope) Segment {
		return Segment{
			StreamID:  streamID,
			Begin:     begin,
			End:       end,
			Envelopes: envelopes,
		}
	}

	// setup returns a catalog that caches the given number of events per
	// stream.
	setup := func(t *testing.T, cacheSize int) *Catalog {
		return &Catalog{
			CacheSize: cacheSize,
			Logger:    spruce.NewTestLogger(t),
		}
	}

	// watch runs c.Watch on another goroutine, and returns a function that
	// cancels it and waits for it to return.
	watch := func(
		t *testing.T,
		c *Catalog,
		src source.Source,
		types *uuidcollection.Set,
	) func() {
		t.Helper()

		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})

		go func() {
			defer close(done)
			c.Watch(ctx, src, types)
		}()

		t.Cleanup(cancel)

		return func() {
			t.Helper()

			cancel()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for Watch to return")
			case <-done:
			}
		}
	}

	// consume runs c.Consume on another goroutine, delivering each segment to
	// the returned channel. It returns a function that cancels it, and one that
	// waits for it to return and returns its error.
	consume := func(
		t *testing.T,
		c *Catalog,
		types *uuidcollection.Set,
		begin uint64,
	) (<-chan Segment, func(), func() error) {
		t.Helper()

		ctx, cancel := context.WithCancel(t.Context())
		segments := make(chan Segment)
		result := make(chan error, 1)

		go func() {
			result <- c.Consume(
				ctx,
				streamID,
				types,
				begin,
				func(seg Segment) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case segments <- seg:
						return nil
					}
				},
			)
		}()

		t.Cleanup(cancel)

		return segments, cancel, func() error {
			t.Helper()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for Consume to return")
				return nil
			case err := <-result:
				return err
			}
		}
	}

	// expectSegment waits for a segment to be delivered and asserts that it
	// covers the given range and holds events at the given offsets.
	expectSegment := func(
		t *testing.T,
		segments <-chan Segment,
		begin, end uint64,
		offsets ...uint64,
	) {
		t.Helper()

		select {
		case <-time.After(xtesting.WaitTimeout):
			t.Fatalf("timeout waiting for segment [%d, %d)", begin, end)
		case seg := <-segments:
			if seg.Begin != begin || seg.End != end {
				t.Fatalf("unexpected segment: got [%d, %d), want [%d, %d)", seg.Begin, seg.End, begin, end)
			}

			var got []uint64
			for _, env := range seg.Envelopes {
				got = append(got, envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset())
			}

			if !slices.Equal(got, offsets) {
				t.Fatalf("unexpected offsets in segment [%d, %d): got %v, want %v", begin, end, got, offsets)
			}
		}
	}

	// expectNoSegment asserts that no segment is delivered within a short
	// period.
	expectNoSegment := func(t *testing.T, segments <-chan Segment) {
		t.Helper()

		select {
		case seg := <-segments:
			t.Fatalf("unexpected segment [%d, %d)", seg.Begin, seg.End)
		case <-time.After(100 * time.Millisecond):
		}
	}

	t.Run("Watch", func(t *testing.T) {
		t.Run("it adds a stream when the source first delivers it", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			remove := watch(t, c, src, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)
		})

		t.Run("it removes a stream when the source that delivers it is removed, ending its consumers", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			remove := watch(t, c, src, nil)

			expectCatalogToHave(t, c, streamID)

			_, _, wait := consume(t, c, typesA, 1)

			remove()

			expectCatalogToHave(t, c)

			if err := wait(); !errors.Is(err, ErrStreamNotFound) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrStreamNotFound)
			}
		})

		t.Run("it publishes the stream set before a consumer observes the stream's removal", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			remove := watch(t, c, src, nil)

			expectCatalogToHave(t, c, streamID)

			err := make(chan error, 1)

			go func() {
				err <- c.Consume(
					t.Context(),
					streamID,
					typesA,
					1,
					func(Segment) error {
						return nil
					},
				)
			}()

			remove()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for Consume to return")
			case <-err:
				if streamIDs, _ := c.StreamIDs().Load(); streamIDs.Has(streamID) {
					t.Fatal("expected the stream to have left the catalog")
				}
			}
		})

		t.Run("it watches the source again if watching it fails", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			var calls atomic.Int32

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: src,
					WatchFunc: func(ctx context.Context, fn func(Segment)) error {
						if calls.Add(1) == 1 {
							return errors.New("<error>")
						}

						return src.Watch(ctx, nil, fn)
					},
				},
				nil,
			)
			defer remove()

			expectCatalogToHave(t, c, streamID)
		})

		t.Run("it returns only once the source is no longer watched", func(t *testing.T) {
			c := setup(t, 10)

			watching := make(chan struct{})
			stopped := make(chan struct{})

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: &source.Memory{},
					WatchFunc: func(ctx context.Context, fn func(Segment)) error {
						close(watching)
						<-ctx.Done()
						time.Sleep(50 * time.Millisecond)
						close(stopped)
						return ctx.Err()
					},
				},
				nil,
			)

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for the source to be watched")
			case <-watching:
			}

			remove()

			select {
			case <-stopped:
			default:
				t.Fatal("expected Watch to return only once the source is no longer watched")
			}
		})
	})

	t.Run("Consume", func(t *testing.T) {
		t.Run("it delivers the events that the source delivers, as they are delivered", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 3))

			remove := watch(t, c, src, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 3)
			defer cancel()

			expectNoSegment(t, segments)

			src.Append(segment(3, 5, event(3, typeB), event(4, typeA)))
			expectSegment(t, segments, 3, 5, 4)

			src.Append(segment(5, 6))
			expectSegment(t, segments, 5, 6)
		})

		t.Run("it delivers cached events from a checkpoint within the cached segment, without reading from the source", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			hook := &sourceHook{
				Source: src,
			}

			remove := watch(t, c, hook, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			// The events are delivered to the catalog before the consumer
			// begins, so they are cached and the consumer is served from the
			// cache.
			src.Append(segment(1, 4, event(1, typeA), event(2, typeB), event(3, typeA)))

			segments, cancel, _ := consume(t, c, typesA, 2)
			defer cancel()

			expectSegment(t, segments, 2, 4, 3)

			if n := hook.Reads.Load(); n != 0 {
				t.Fatalf("unexpected number of reads from the source: got %d, want 0", n)
			}
		})

		t.Run("it waits while the checkpoint is beyond the cached segment", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 3))

			remove := watch(t, c, src, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 5)
			defer cancel()

			expectNoSegment(t, segments)

			src.Append(segment(3, 7, event(4, typeA), event(6, typeA)))
			expectSegment(t, segments, 5, 7, 6)
		})

		t.Run("it reads from the source when the checkpoint is before the cached segment, then continues from the cache", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{
				SegmentSize: 2,
			}
			src.Append(segment(0, 5, event(0, typeA), event(1, typeB), event(2, typeA), event(3, typeA), event(4, typeA)))

			remove := watch(t, c, src, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 0)
			defer cancel()

			// The source is watched from the end of the stream, so the events
			// before it are read from the source, in batches.
			expectSegment(t, segments, 0, 3, 0, 2)
			expectSegment(t, segments, 3, 5, 3, 4)
			expectNoSegment(t, segments)

			src.Append(segment(5, 6, event(5, typeA)))
			expectSegment(t, segments, 5, 6, 5)
		})

		t.Run("it keeps reading from the source while the cached segment moves on ahead of the checkpoint", func(t *testing.T) {
			// A cache of one event means that each event appended evicts the
			// one before it.
			c := setup(t, 1)

			src := &source.Memory{
				SegmentSize: 2,
			}
			src.Append(segment(0, 2, event(0, typeA), event(1, typeA)))

			// delivered receives the end offset of each segment that the source
			// delivers to the catalog.
			delivered := make(chan uint64, 10)

			hook := &sourceHook{
				Source: src,
				WatchFunc: func(ctx context.Context, fn func(Segment)) error {
					return src.Watch(
						ctx,
						nil,
						func(seg Segment) {
							fn(seg)
							delivered <- seg.End
						},
					)
				},
			}

			remove := watch(t, c, hook, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 0)
			defer cancel()

			// The consumer is reading from the source, and its first segment
			// is not accepted until the events appended here have both been
			// delivered to the catalog, by which time the cached segment
			// begins at 3.
			src.Append(segment(2, 3, event(2, typeA)))
			src.Append(segment(3, 4, event(3, typeA)))

			for end := uint64(0); end < 4; {
				select {
				case <-time.After(xtesting.WaitTimeout):
					t.Fatal("timeout waiting for the source to deliver the events")
				case end = <-delivered:
				}
			}

			expectSegment(t, segments, 0, 2, 0, 1)

			// The checkpoint, 2, is still before the cached segment, so the
			// read continues, past where the cached segment began when the
			// read started.
			expectSegment(t, segments, 2, 4, 2, 3)

			if n := hook.Reads.Load(); n != 1 {
				t.Fatalf("unexpected number of reads from the source: got %d, want 1", n)
			}
		})

		t.Run("it restarts a stream when the source delivers a segment that does not follow the previous one", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{
				SegmentSize: 3,
			}
			src.Append(segment(0, 2, event(0, typeA), event(1, typeA)))

			restart := make(chan Segment)

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: src,
					WatchFunc: func(ctx context.Context, fn func(Segment)) error {
						fn(segment(2, 2))

						select {
						case <-ctx.Done():
							return ctx.Err()
						case seg := <-restart:
							fn(seg)
						}

						<-ctx.Done()
						return ctx.Err()
					},
				},
				nil,
			)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 0)
			defer cancel()

			expectSegment(t, segments, 0, 2, 0, 1)

			// The source has grown to 6 events, but delivers a segment
			// beginning at 5, so the events from 2 are read from the source, in
			// a batch that ends at 5, and the one at 5 comes from the cache.
			src.Append(segment(2, 6, event(2, typeA), event(3, typeA), event(4, typeA), event(5, typeA)))
			restart <- segment(5, 6, event(5, typeA))

			expectSegment(t, segments, 2, 5, 2, 3, 4)
			expectSegment(t, segments, 5, 6, 5)
		})

		t.Run("it stops reading from the source as soon as the checkpoint reaches the cached segment", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{
				SegmentSize: 1,
			}
			src.Append(segment(0, 4, event(0, typeA), event(1, typeA), event(2, typeA), event(3, typeA)))

			restart := make(chan Segment)
			restarted := make(chan struct{})

			hook := &sourceHook{
				Source: src,
				WatchFunc: func(ctx context.Context, fn func(Segment)) error {
					fn(segment(4, 4))

					for {
						select {
						case <-ctx.Done():
							return ctx.Err()
						case seg := <-restart:
							fn(seg)
							restarted <- struct{}{}
						}
					}
				},
			}

			remove := watch(t, c, hook, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			segments, cancel, _ := consume(t, c, typesA, 0)
			defer cancel()

			expectSegment(t, segments, 0, 1, 0)

			// The source restarts the stream at 2, so once the read reaches 2
			// the checkpoint is within the cached segment, and the read stops
			// even though the source has more to deliver.
			restart <- segment(2, 2)
			<-restarted

			expectSegment(t, segments, 1, 2, 1)
			expectNoSegment(t, segments)

			restart <- segment(2, 3, event(2, typeA))
			<-restarted

			expectSegment(t, segments, 2, 3, 2)

			if n := hook.Reads.Load(); n != 1 {
				t.Fatalf("unexpected number of reads from the source: got %d, want 1", n)
			}
		})

		t.Run("it returns fn's error unchanged", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 2, event(0, typeA), event(1, typeA)))

			remove := watch(t, c, src, nil)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			errRejected := errors.New("<rejected>")

			err := c.Consume(
				t.Context(),
				streamID,
				typesA,
				0,
				func(Segment) error {
					return errRejected
				},
			)

			if err != errRejected {
				t.Fatalf("unexpected error: got %v, want %v", err, errRejected)
			}
		})

		t.Run("it returns the source's error unchanged", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 2))

			errRead := errors.New("<read>")

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: src,
					ReadFunc: func(context.Context) error {
						return errRead
					},
				},
				nil,
			)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			err := c.Consume(
				t.Context(),
				streamID,
				typesA,
				0,
				func(Segment) error {
					return nil
				},
			)

			if err != errRead {
				t.Fatalf("unexpected error: got %v, want %v", err, errRead)
			}
		})

		t.Run("it fails if the source's stream ends before the cached segment", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: src,
					WatchFunc: func(ctx context.Context, fn func(Segment)) error {
						fn(segment(2, 2))
						<-ctx.Done()
						return ctx.Err()
					},
				},
				nil,
			)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			err := c.Consume(
				t.Context(),
				streamID,
				typesA,
				1,
				func(Segment) error {
					return nil
				},
			)

			if err == nil {
				t.Fatal("expected an error")
			}
		})

		t.Run("it cancels a read from the source when the source is removed", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 10))

			reading := make(chan struct{})
			canceled := make(chan struct{})

			remove := watch(
				t,
				c,
				&sourceHook{
					Source: src,
					ReadFunc: func(ctx context.Context) error {
						close(reading)
						<-ctx.Done()
						close(canceled)
						return ctx.Err()
					},
				},
				nil,
			)

			expectCatalogToHave(t, c, streamID)

			_, _, wait := consume(t, c, typesA, 0)

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for the read to begin")
			case <-reading:
			}

			remove()

			select {
			case <-time.After(xtesting.WaitTimeout):
				t.Fatal("timeout waiting for the read to be canceled")
			case <-canceled:
			}

			if err := wait(); !errors.Is(err, ErrStreamNotFound) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrStreamNotFound)
			}
		})

		t.Run("it returns ErrStreamNotFound at once for a stream the catalog does not have", func(t *testing.T) {
			c := setup(t, 10)

			err := c.Consume(
				t.Context(),
				streamID,
				typesA,
				0,
				func(Segment) error {
					return nil
				},
			)

			if !errors.Is(err, ErrStreamNotFound) {
				t.Fatalf("unexpected error: got %v, want %v", err, ErrStreamNotFound)
			}
		})

		t.Run("it panics when consuming a type that the source is not watched for", func(t *testing.T) {
			c := setup(t, 10)

			src := &source.Memory{}
			src.Append(segment(0, 1))

			remove := watch(t, c, src, typesA)
			defer remove()

			expectCatalogToHave(t, c, streamID)

			defer func() {
				if recover() == nil {
					t.Fatal("expected a panic")
				}
			}()

			c.Consume(
				t.Context(),
				streamID,
				uuidcollection.NewSet(typeB),
				0,
				func(Segment) error {
					return nil
				},
			)
		})
	})
}

// sourceHook is a source that counts reads, and may replace the source's Watch
// and Read.
type sourceHook struct {
	source.Source

	// WatchFunc, if non-nil, is called in place of the source's Watch.
	WatchFunc func(ctx context.Context, fn func(Segment)) error

	// ReadFunc, if non-nil, is called in place of the source's Read.
	ReadFunc func(ctx context.Context) error

	// Reads is the number of times Read has been called.
	Reads atomic.Int32
}

func (s *sourceHook) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(Segment),
) error {
	if s.WatchFunc != nil {
		return s.WatchFunc(ctx, fn)
	}

	return s.Source.Watch(ctx, types, fn)
}

func (s *sourceHook) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(Segment) error,
) error {
	s.Reads.Add(1)

	if s.ReadFunc != nil {
		return s.ReadFunc(ctx)
	}

	return s.Source.Read(ctx, streamID, types, begin, fn)
}

// expectCatalogToHave waits for the catalog to have exactly the given streams.
func expectCatalogToHave(
	t testing.TB,
	c *Catalog,
	want ...*uuidpb.UUID,
) {
	t.Helper()

	expected := uuidcollection.NewSet(want...)
	timeout := time.After(xtesting.WaitTimeout)

	for {
		got, changed := c.StreamIDs().Load()

		if got.IsEqual(expected) {
			return
		}

		select {
		case <-timeout:
			t.Fatalf("unexpected stream IDs: got %s, want %s", got, expected)
		case <-changed:
		}
	}
}
