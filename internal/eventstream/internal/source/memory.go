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

// Watch delivers events to fn as they are appended to the source's streams,
// as described by [Source].
//
// Each stream is delivered from its end as it stands when Watch is called.
func (s *Memory) Watch(
	ctx context.Context,
	types *uuidcollection.Set,
	fn func(context.Context, Segment),
) error {
	var delivered uuidcollection.Map[uint64]

	for {
		segments, changed := s.undelivered(types, &delivered)

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

// Read delivers events from a stream to fn, beginning at the given offset, as
// described by [Source].
func (s *Memory) Read(
	ctx context.Context,
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
	fn func(context.Context, Segment) error,
) error {
	for {
		seg, ok, err := s.unread(streamID, types, begin)
		if err != nil {
			return err
		}

		if !ok {
			return nil
		}

		if err := fn(ctx, seg); err != nil {
			return err
		}

		begin = seg.End
	}
}

// undelivered returns a segment of each stream that has grown since it was
// last delivered, filtered to the given types, and a channel that is closed
// when the source next changes.
//
// delivered holds the offset up to which each stream has been delivered, and
// is updated with the end of each segment returned. A stream that has not been
// delivered before is delivered from its end.
func (s *Memory) undelivered(
	types *uuidcollection.Set,
	delivered *uuidcollection.Map[uint64],
) ([]Segment, <-chan struct{}) {
	s.m.Lock()
	defer s.m.Unlock()

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

	return segments, changed
}

// unread returns the next segment of the given stream from the given offset,
// filtered to the given types and limited to BatchSize events.
//
// ok is false if the offset is at the end of the stream.
func (s *Memory) unread(
	streamID *uuidpb.UUID,
	types *uuidcollection.Set,
	begin uint64,
) (seg Segment, ok bool, err error) {
	s.m.Lock()
	defer s.m.Unlock()

	ms, ok := s.streams.Get(streamID)
	if !ok {
		return Segment{}, false, fmt.Errorf("event stream %s not found", streamID)
	}

	if begin >= ms.End {
		return Segment{}, false, nil
	}

	seg = s.segment(streamID, ms, types, begin, ms.End)

	if s.BatchSize > 0 && len(seg.Envelopes) > s.BatchSize {
		seg.Envelopes = seg.Envelopes[:s.BatchSize]
		seg.End = offsetOf(seg.Envelopes[s.BatchSize-1]) + 1
	}

	return seg, true, nil
}

// segment returns the segment [begin, end) of the given stream, filtered to
// the given types.
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
