package eventstream_test

import (
	"context"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/eventstream"
)

// sinkStub is a test implementation of [Sink].
type sinkStub struct {
	WorkloadFunc         func(*uuidpb.UUID) *uuidpb.UUID
	EventTypeIDsFunc     func() *uuidcollection.Set
	CheckpointOffsetFunc func(context.Context, *uuidpb.UUID) (uint64, error)
	ConsumeSegmentFunc   func(context.Context, uint64, Segment) (uint64, bool, error)
}

var _ Sink = (*sinkStub)(nil)

// Workload returns the rendezvous workload for the stream.
func (s *sinkStub) Workload(streamID *uuidpb.UUID) *uuidpb.UUID {
	if s.WorkloadFunc != nil {
		return s.WorkloadFunc(streamID)
	}
	return streamID
}

// EventTypeIDs returns the event types that the sink consumes.
func (s *sinkStub) EventTypeIDs() *uuidcollection.Set {
	if s.EventTypeIDsFunc != nil {
		return s.EventTypeIDsFunc()
	}
	return uuidcollection.NewSet(uuidpb.MustParse("00000000-0000-0000-0000-000000000001"))
}

// CheckpointOffset returns the offset from which consumption resumes.
func (s *sinkStub) CheckpointOffset(
	ctx context.Context,
	streamID *uuidpb.UUID,
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
