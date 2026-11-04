package source_test

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream/source"
)

func TestMemory(t *testing.T) {
	var (
		streamA = uuidpb.Generate()
		streamB = uuidpb.Generate()
		typeA   = uuidpb.Generate()
		typeB   = uuidpb.Generate()
	)

	// event returns an envelope for an event of the given type at the given
	// offset of the given stream.
	event := func(streamID *uuidpb.UUID, offset uint64, typeID *uuidpb.UUID) *envelopepb.Envelope {
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

	// offsets returns the offsets of the events in seg.
	offsets := func(seg Segment) []uint64 {
		var offsets []uint64

		for _, env := range seg.Envelopes {
			offsets = append(
				offsets,
				envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset(),
			)
		}

		return offsets
	}

	// expectSegment asserts that seg is the given segment of the given stream
	// and holds events at the given offsets.
	expectSegment := func(
		t *testing.T,
		seg Segment,
		streamID *uuidpb.UUID,
		begin, end uint64,
		want ...uint64,
	) {
		t.Helper()

		if !seg.StreamID.Equal(streamID) {
			t.Fatalf("unexpected stream: got %s, want %s", seg.StreamID, streamID)
		}

		if seg.Begin != begin || seg.End != end {
			t.Fatalf("unexpected segment: got [%d, %d), want [%d, %d)", seg.Begin, seg.End, begin, end)
		}

		if got := offsets(seg); !slices.Equal(got, want) {
			t.Fatalf("unexpected offsets: got %v, want %v", got, want)
		}
	}

	// read returns the segments that Read delivers.
	read := func(
		t *testing.T,
		src *Memory,
		streamID *uuidpb.UUID,
		types *uuidcollection.Set,
		begin uint64,
	) []Segment {
		t.Helper()

		var segments []Segment

		err := src.Read(
			t.Context(),
			streamID,
			types,
			begin,
			func(_ context.Context, seg Segment) error {
				segments = append(segments, seg)
				return nil
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		return segments
	}

	t.Run("Read", func(t *testing.T) {
		t.Run("it delivers the events of the given types from the given offset to the end of the stream", func(t *testing.T) {
			src := &Memory{}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      5,
				Envelopes: []*envelopepb.Envelope{
					event(streamA, 0, typeA),
					event(streamA, 1, typeB),
					event(streamA, 3, typeA),
				},
			})

			segments := read(t, src, streamA, uuidcollection.NewSet(typeA), 1)

			if len(segments) != 1 {
				t.Fatalf("unexpected number of segments: got %d, want 1", len(segments))
			}

			expectSegment(t, segments[0], streamA, 1, 5, 3)
		})

		t.Run("it delivers every type if none are given", func(t *testing.T) {
			src := &Memory{}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      2,
				Envelopes: []*envelopepb.Envelope{
					event(streamA, 0, typeA),
					event(streamA, 1, typeB),
				},
			})

			segments := read(t, src, streamA, nil, 0)

			expectSegment(t, segments[0], streamA, 0, 2, 0, 1)
		})

		t.Run("it delivers in batches of the given size", func(t *testing.T) {
			src := &Memory{
				SegmentSize: 2,
			}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      6,
				Envelopes: []*envelopepb.Envelope{
					event(streamA, 0, typeA),
					event(streamA, 1, typeA),
					event(streamA, 2, typeA),
				},
			})

			segments := read(t, src, streamA, nil, 0)

			if len(segments) != 2 {
				t.Fatalf("unexpected number of segments: got %d, want 2", len(segments))
			}

			expectSegment(t, segments[0], streamA, 0, 2, 0, 1)
			expectSegment(t, segments[1], streamA, 2, 6, 2)
		})

		t.Run("it delivers nothing from the end of the stream", func(t *testing.T) {
			src := &Memory{}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      3,
			})

			if segments := read(t, src, streamA, nil, 3); len(segments) != 0 {
				t.Fatalf("unexpected number of segments: got %d, want 0", len(segments))
			}
		})

		t.Run("it fails if the stream does not exist", func(t *testing.T) {
			src := &Memory{}

			err := src.Read(
				t.Context(),
				streamA,
				nil,
				0,
				func(context.Context, Segment) error {
					return nil
				},
			)

			if err == nil {
				t.Fatal("expected an error")
			}
		})
	})

	t.Run("Watch", func(t *testing.T) {
		// watch runs src.Watch on another goroutine, delivering each segment
		// to the returned channel.
		watch := func(t *testing.T, src *Memory, types *uuidcollection.Set) <-chan Segment {
			t.Helper()

			ctx, cancel := context.WithCancel(t.Context())
			segments := make(chan Segment)
			stopped := make(chan struct{})

			go func() {
				defer close(stopped)

				src.Watch(
					ctx,
					types,
					func(ctx context.Context, seg Segment) {
						select {
						case <-ctx.Done():
						case segments <- seg:
						}
					},
				)
			}()

			t.Cleanup(func() {
				cancel()
				<-stopped
			})

			return segments
		}

		// expect waits for a segment to be delivered and returns it.
		expect := func(t *testing.T, segments <-chan Segment) Segment {
			t.Helper()

			select {
			case <-time.After(5 * time.Second):
				t.Fatal("timeout waiting for a segment")
				return Segment{}
			case seg := <-segments:
				return seg
			}
		}

		t.Run("it begins each stream at its end as it stands", func(t *testing.T) {
			src := &Memory{}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      3,
				Envelopes: []*envelopepb.Envelope{
					event(streamA, 1, typeA),
				},
			})

			segments := watch(t, src, nil)

			expectSegment(t, expect(t, segments), streamA, 3, 3)
		})

		t.Run("it delivers segments appended after it begins, of the given types", func(t *testing.T) {
			src := &Memory{}
			src.Append(Segment{
				StreamID: streamA,
				Begin:    0,
				End:      3,
			})

			segments := watch(t, src, uuidcollection.NewSet(typeA))

			expectSegment(t, expect(t, segments), streamA, 3, 3)

			src.Append(Segment{
				StreamID: streamA,
				Begin:    3,
				End:      6,
				Envelopes: []*envelopepb.Envelope{
					event(streamA, 3, typeB),
					event(streamA, 5, typeA),
				},
			})

			expectSegment(t, expect(t, segments), streamA, 3, 6, 5)
		})

		t.Run("it begins a stream created after it begins at that stream's end", func(t *testing.T) {
			src := &Memory{}

			segments := watch(t, src, nil)

			src.Append(Segment{
				StreamID: streamB,
				Begin:    0,
				End:      2,
				Envelopes: []*envelopepb.Envelope{
					event(streamB, 0, typeA),
				},
			})

			expectSegment(t, expect(t, segments), streamB, 2, 2)

			src.Append(Segment{
				StreamID: streamB,
				Begin:    2,
				End:      3,
				Envelopes: []*envelopepb.Envelope{
					event(streamB, 2, typeA),
				},
			})

			expectSegment(t, expect(t, segments), streamB, 2, 3, 2)
		})
	})
}
