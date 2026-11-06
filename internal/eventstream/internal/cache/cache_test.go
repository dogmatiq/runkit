package cache_test

import (
	"slices"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream/internal/cache"
	"pgregory.net/rapid"
)

func TestCache(t *testing.T) {
	var (
		streamID = uuidpb.Generate()
		typeA    = uuidpb.Generate()
		typeB    = uuidpb.Generate()
		typeC    = uuidpb.Generate()
		types    = []*uuidpb.UUID{typeA, typeB, typeC}
	)

	// event describes an event to append, by its offset and type.
	type event struct {
		Offset uint64
		TypeID *uuidpb.UUID
	}

	// envelopes returns an envelope for each of the given events.
	envelopes := func(events ...event) []*envelopepb.Envelope {
		var envs []*envelopepb.Envelope

		for _, e := range events {
			envs = append(envs, newEnvelope(streamID, e.Offset, e.TypeID))
		}

		return envs
	}

	// expectRead asserts that reading from cp yields the given relation, and
	// if that is Within, the given end and offsets.
	expectRead := func(
		t *testing.T,
		c *Cache,
		cp uint64,
		want *uuidcollection.Set,
		relation Relation,
		end uint64,
		offsets ...uint64,
	) {
		t.Helper()

		seg, _ := c.Segment().Load()
		envs, got := seg.Read(cp, want)
		gotEnd := seg.End

		if got != relation {
			t.Fatalf("unexpected relation reading from %d: got %d, want %d", cp, got, relation)
		}

		if relation != Within {
			return
		}

		if gotEnd != end {
			t.Fatalf("unexpected end of segment: got %d, want %d", gotEnd, end)
		}

		var gotOffsets []uint64
		for _, env := range envs {
			gotOffsets = append(gotOffsets, offsetOf(env))
		}

		if !slices.Equal(gotOffsets, offsets) {
			t.Fatalf("unexpected offsets: got %v, want %v", gotOffsets, offsets)
		}
	}

	t.Run("it begins with an empty segment at the given offset", func(t *testing.T) {
		c := New(10, 5)

		expectRead(t, c, 4, uuidcollection.NewSet(typeA), Before, 0)
		expectRead(t, c, 5, uuidcollection.NewSet(typeA), After, 0)
		expectRead(t, c, 6, uuidcollection.NewSet(typeA), After, 0)
	})

	t.Run("it reports where a checkpoint lies in relation to the segment at each boundary", func(t *testing.T) {
		c := New(10, 5)
		c.Append(5, 8, envelopes(event{5, typeA}, event{7, typeB}))

		want := uuidcollection.NewSet(typeA, typeB)

		expectRead(t, c, 4, want, Before, 0)
		expectRead(t, c, 5, want, Within, 8, 5, 7)
		expectRead(t, c, 6, want, Within, 8, 7)
		expectRead(t, c, 7, want, Within, 8, 7)
		expectRead(t, c, 8, want, After, 0)
		expectRead(t, c, 9, want, After, 0)
	})

	t.Run("it filters the events by type", func(t *testing.T) {
		c := New(10, 0)
		c.Append(0, 3, envelopes(event{0, typeA}, event{1, typeB}, event{2, typeA}))

		expectRead(t, c, 0, uuidcollection.NewSet(typeA), Within, 3, 0, 2)
		expectRead(t, c, 0, uuidcollection.NewSet(typeB), Within, 3, 1)
		expectRead(t, c, 0, uuidcollection.NewSet(typeC), Within, 3)
	})

	t.Run("it extends the segment with an empty range", func(t *testing.T) {
		c := New(10, 0)
		c.Append(0, 3, nil)

		expectRead(t, c, 0, uuidcollection.NewSet(typeA), Within, 3)
		expectRead(t, c, 3, uuidcollection.NewSet(typeA), After, 0)
	})

	t.Run("it keeps the events held when extended with an empty range", func(t *testing.T) {
		c := New(10, 0)
		c.Append(0, 3, envelopes(event{1, typeA}))
		c.Append(3, 6, nil)

		expectRead(t, c, 0, uuidcollection.NewSet(typeA), Within, 6, 1)
	})

	t.Run("it evicts the oldest events when over capacity, and moves the segment past them", func(t *testing.T) {
		c := New(2, 0)
		c.Append(0, 10, envelopes(event{2, typeA}, event{5, typeA}, event{7, typeA}))

		want := uuidcollection.NewSet(typeA)

		// The event at offset 2 was evicted, so the cache is complete only
		// from offset 3.
		expectRead(t, c, 2, want, Before, 0)
		expectRead(t, c, 3, want, Within, 10, 5, 7)
	})

	t.Run("it evicts the oldest of the events held to make room", func(t *testing.T) {
		c := New(2, 0)
		c.Append(0, 4, envelopes(event{1, typeA}, event{3, typeA}))
		c.Append(4, 6, envelopes(event{4, typeA}))

		want := uuidcollection.NewSet(typeA)

		expectRead(t, c, 1, want, Before, 0)
		expectRead(t, c, 2, want, Within, 6, 3, 4)
	})

	t.Run("it evicts every event held when the new events fill the cache", func(t *testing.T) {
		c := New(2, 0)
		c.Append(0, 2, envelopes(event{1, typeA}))
		c.Append(2, 5, envelopes(event{2, typeA}, event{4, typeA}))

		want := uuidcollection.NewSet(typeA)

		expectRead(t, c, 1, want, Before, 0)
		expectRead(t, c, 2, want, Within, 5, 2, 4)
	})

	t.Run("it evicts every event held, and the oldest new events, when the new events alone exceed the capacity", func(t *testing.T) {
		c := New(2, 0)
		c.Append(0, 2, envelopes(event{1, typeA}))
		c.Append(2, 7, envelopes(event{2, typeA}, event{4, typeA}, event{6, typeA}))

		want := uuidcollection.NewSet(typeA)

		expectRead(t, c, 2, want, Before, 0)
		expectRead(t, c, 3, want, Within, 7, 4, 6)
	})

	t.Run("it panics when a range does not begin where the segment ends", func(t *testing.T) {
		c := New(10, 0)
		c.Append(0, 3, nil)

		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		c.Append(4, 5, nil)
	})

	t.Run("it discards the events held when reset", func(t *testing.T) {
		c := New(10, 0)
		c.Append(0, 3, envelopes(event{1, typeA}))

		c.Reset(10)

		want := uuidcollection.NewSet(typeA)

		expectRead(t, c, 1, want, Before, 0)
		expectRead(t, c, 10, want, After, 0)
	})

	t.Run("it closes the change channel when appended to or reset", func(t *testing.T) {
		c := New(10, 0)

		expectClosed := func(t *testing.T, ch <-chan struct{}) {
			t.Helper()

			select {
			case <-ch:
			case <-time.After(time.Second):
				t.Fatal("expected the change channel to be closed")
			}
		}

		_, changed := c.Segment().Load()
		c.Append(0, 1, nil)
		expectClosed(t, changed)

		_, changed = c.Segment().Load()
		c.Reset(5)
		expectClosed(t, changed)
	})

	t.Run("it yields exactly the matching events from any checkpoint within the segment", func(t *testing.T) {
		rapid.Check(t, func(t *rapid.T) {
			const capacity = 5

			c := New(capacity, 0)

			// all holds every event appended, so that the expected result can
			// be computed independently of the cache. begin is the offset from
			// which the cache is complete, which is the offset after the last
			// event evicted.
			var (
				all        []event
				begin, end uint64
				held       int
			)

			for range rapid.IntRange(1, 10).Draw(t, "appends") {
				var events []event
				next := end

				for range rapid.IntRange(0, 4).Draw(t, "events") {
					next += uint64(rapid.IntRange(0, 3).Draw(t, "gap"))
					events = append(events, event{
						Offset: next,
						TypeID: rapid.SampledFrom(types).Draw(t, "type"),
					})
					next++
				}

				next += uint64(rapid.IntRange(0, 3).Draw(t, "trailing"))

				c.Append(end, next, envelopes(events...))

				all = append(all, events...)
				held += len(events)
				end = next

				if evict := held - capacity; evict > 0 {
					begin = all[len(all)-held+evict-1].Offset + 1
					held = capacity
				}
			}

			want := uuidcollection.NewSet(
				rapid.SampledFrom([][]*uuidpb.UUID{
					{typeA},
					{typeB},
					{typeA, typeC},
					types,
				}).Draw(t, "types")...,
			)

			cp := uint64(rapid.IntRange(int(begin), int(end)).Draw(t, "checkpoint"))

			var offsets []uint64
			for _, e := range all {
				if e.Offset >= cp && want.Has(e.TypeID) {
					offsets = append(offsets, e.Offset)
				}
			}

			seg, _ := c.Segment().Load()
			envs, relation := seg.Read(cp, want)
			gotEnd := seg.End

			if cp == end {
				if relation != After {
					t.Fatalf("unexpected relation at the end of the segment: got %d", relation)
				}
				return
			}

			if relation != Within {
				t.Fatalf("unexpected relation within the segment [%d, %d) at %d: got %d", begin, end, cp, relation)
			}

			if gotEnd != end {
				t.Fatalf("unexpected end of segment: got %d, want %d", gotEnd, end)
			}

			var got []uint64
			for _, env := range envs {
				got = append(got, offsetOf(env))
			}

			if !slices.Equal(got, offsets) {
				t.Fatalf("unexpected offsets from %d: got %v, want %v", cp, got, offsets)
			}
		})
	})
}

// newEnvelope returns an envelope for an event of the given type at the given
// offset, with only the fields that the cache reads.
func newEnvelope(
	streamID *uuidpb.UUID,
	offset uint64,
	typeID *uuidpb.UUID,
) *envelopepb.Envelope {
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

// offsetOf returns the stream offset of the event in env.
func offsetOf(env *envelopepb.Envelope) uint64 {
	return envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset()
}
