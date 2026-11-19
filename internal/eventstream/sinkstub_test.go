package eventstream_test

import (
	"context"
	"uuid"

	. "github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/uuidset"
)

// sinkStub is a test implementation of [Sink].
type sinkStub struct {
	RendezvousKeyFunc    func(uuid.UUID) uuid.UUID
	EventTypeIDsFunc     func() *uuidset.SetR
	CheckpointOffsetFunc func(context.Context, uuid.UUID) (uint64, error)
	ConsumeSegmentFunc   func(context.Context, uint64, Segment) (uint64, bool, error)
}

var _ Sink = (*sinkStub)(nil)

// RendezvousKey returns the rendezvous key for the stream.
func (s *sinkStub) RendezvousKey(streamID uuid.UUID) uuid.UUID {
	if s.RendezvousKeyFunc != nil {
		return s.RendezvousKeyFunc(streamID)
	}
	return streamID
}

// EventTypeIDs returns the event types that the sink consumes.
func (s *sinkStub) EventTypeIDs() *uuidset.SetR {
	if s.EventTypeIDsFunc != nil {
		return s.EventTypeIDsFunc()
	}
	return uuidset.New(uuid.MustParse("00000000-0000-0000-0000-000000000001")).R()
}

// CheckpointOffset returns the offset from which consumption resumes.
func (s *sinkStub) CheckpointOffset(
	ctx context.Context,
	streamID uuid.UUID,
) (uint64, error) {
	if s.CheckpointOffsetFunc != nil {
		return s.CheckpointOffsetFunc(ctx, streamID)
	}
	return 0, nil
}

// ConsumeSegment handles a segment and returns the new checkpoint offset.
func (s *sinkStub) ConsumeSegment(
	ctx context.Context,
	cp uint64,
	seg Segment,
) (uint64, bool, error) {
	if s.ConsumeSegmentFunc != nil {
		return s.ConsumeSegmentFunc(ctx, cp, seg)
	}
	return seg.End, true, nil
}
