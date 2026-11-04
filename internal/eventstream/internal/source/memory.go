package source

import (
	"context"
	"fmt"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
)

// Memory is a [Source] that holds its streams in memory.
type Memory struct {
	// BatchSize is the maximum number of matching events per segment that
	// Read delivers. Zero means no limit.
	BatchSize int

	m       sync.Mutex
	streams uuidcollection.Map[*memoryStream]
	changed xsync.Observable[struct{}]
}

// memoryStream is a stream held by a [Memory] source.
type memoryStream struct {
	// Envelopes are the stream's events, in offset order.
	Envelopes []*envelopepb.Envelope

	// End is the offset at which the stream ends.
	End uint64
}

// Append appends a segment to the stream it belongs to, creating the stream
// if it is the first segment. The segment must begin where the stream ends.
func (s *Memory) Append(seg Segment) {
	s.m.Lock()
	defer s.m.Unlock()

	ms, ok := s.streams.Get(seg.StreamID)
	if !ok {
		ms = &memoryStream{
			End: seg.Begin,
		}
		s.streams.Set(seg.StreamID, ms)
	}

	if seg.Begin != ms.End {
		panic(fmt.Sprintf(
			"segment begins at offset %d, but the stream ends at offset %d",
			seg.Begin,
			ms.End,
		))
	}

	ms.Envelopes = append(ms.Envelopes, seg.Envelopes...)
	ms.End = seg.End

	s.changed.Store(struct{}{})
}

// Watch delivers the events of the given types from each of the source's
// streams to fn, beginning at the end of each stream as it stands, as
// described by [Source].
func (s *Memory) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(context.Context, Segment),
) error {
	delivered := uuidcollection.Map[uint64]{}

	for {
		s.m.Lock()
		_, changed := s.changed.Load()

		var segments []Segment

		for streamID, ms := range s.streams.All() {
			begin, ok := delivered.Get(streamID)
			if !ok {
				begin = ms.End
			}

			if ok && begin == ms.End {
				continue
			}

			segments = append(segments, s.segment(streamID, ms, types, begin, ms.End))
			delivered.Set(streamID, ms.End)
		}

		s.m.Unlock()

		for _, seg := range segments {
			fn(ctx, seg)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// Read delivers the events of the given types from the given stream to fn, in
// contiguous segments beginning at the given offset, as described by [Source].
func (s *Memory) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(context.Context, Segment) error,
) error {
	s.m.Lock()
	ms, ok := s.streams.Get(streamID)
	s.m.Unlock()

	if !ok {
		return fmt.Errorf("event stream %s not found", streamID)
	}

	for begin < ms.End {
		seg := s.segment(streamID, ms, types, begin, ms.End)

		if s.BatchSize > 0 && len(seg.Envelopes) > s.BatchSize {
			seg.Envelopes = seg.Envelopes[:s.BatchSize]
			seg.End = offsetOf(seg.Envelopes[s.BatchSize-1]) + 1
		}

		if err := fn(ctx, seg); err != nil {
			return err
		}

		begin = seg.End
	}

	return nil
}

// segment returns the segment [begin, end) of the given stream, holding its
// events of the given types.
func (s *Memory) segment(
	streamID *uuidpb.UUID,
	ms *memoryStream,
	types *uuidcollection.Set,
	begin, end uint64,
) Segment {
	seg := Segment{
		StreamID: streamID,
		Begin:    begin,
		End:      end,
	}

	for _, env := range ms.Envelopes {
		offset := offsetOf(env)

		if offset < begin || offset >= end {
			continue
		}

		if types != nil && !types.Has(env.GetBody().GetMessage().GetTypeId()) {
			continue
		}

		seg.Envelopes = append(seg.Envelopes, env)
	}

	return seg
}

// offsetOf returns the offset of the event in env.
func offsetOf(env *envelopepb.Envelope) uint64 {
	return envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset()
}
