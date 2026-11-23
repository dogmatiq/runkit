package grpcserver

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/dogmatiq/enginekit/optional"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// EventStreamConsumerService provides the
// [messaginggrpc.EventStreamConsumerAPI] service over the event streams of a
// [source.Local], so that remote engines can consume them.
type EventStreamConsumerService struct {
	messaginggrpc.UnimplementedEventStreamConsumerAPIServer

	// EventStreams is the source of the event streams that the service
	// provides.
	EventStreams *source.Local

	// MaxSegmentBytes is the maximum combined size, in bytes, of the events in
	// each segment that the service sends. WatchEvents and ReadEvents send a
	// segment whose events are larger as several contiguous segments.
	// ReadEventSegment ends its segment before the events that would exceed it.
	// If a single event is larger than this, it is sent on its own. It must be
	// positive.
	MaxSegmentBytes int
}

// ListEventStreams implements the ListEventStreams method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *EventStreamConsumerService) ListEventStreams(
	ctx context.Context,
	_ *messaginggrpc.ListEventStreamsRequest,
) (*messaginggrpc.ListEventStreamsResponse, error) {
	lengths, err := s.EventStreams.Streams(ctx)
	if err != nil {
		return nil, err
	}

	eventStreams := make([]*messaginggrpc.EventStream, 0, len(lengths))

	for id, length := range lengths {
		eventStreams = append(
			eventStreams,
			messaginggrpc.NewEventStreamBuilder().
				WithEventStreamId(uuidpb.FromStd(id)).
				WithLength(length).
				Build(),
		)
	}

	return messaginggrpc.NewListEventStreamsResponseBuilder().
		WithEventStreams(eventStreams).
		Build(), nil
}

// WatchEvents implements the WatchEvents method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *EventStreamConsumerService) WatchEvents(
	req *messaginggrpc.WatchEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
	if s.MaxSegmentBytes <= 0 {
		panic("maximum segment size must be positive")
	}

	types, err := requestedTypes(req.GetFilter())
	if err != nil {
		return err
	}

	// A source may deliver segments of different streams concurrently.
	var m sync.Mutex

	err = s.EventStreams.Watch(
		res.Context(),
		types,
		func(seg source.Segment) error {
			m.Lock()
			defer m.Unlock()

			return splitAndSendSegment(res, seg, s.MaxSegmentBytes)
		},
	)

	if errors.Is(err, source.ErrNotContiguous) {
		return status.Error(codes.Aborted, err.Error())
	}

	return err
}

// ReadEventSegment implements the ReadEventSegment method of
// [messaginggrpc.EventStreamConsumerAPIServer].
//
// The segment it returns can end before the requested end offset. The client
// reads the rest of the range from where the segment ends.
func (s *EventStreamConsumerService) ReadEventSegment(
	ctx context.Context,
	req *messaginggrpc.ReadEventSegmentRequest,
) (*messaginggrpc.EventStreamSegment, error) {
	if s.MaxSegmentBytes <= 0 {
		panic("maximum segment size must be positive")
	}

	if err := req.GetEventStreamId().Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid event stream ID: %s", err)
	}

	types, err := requestedTypes(req.GetFilter())
	if err != nil {
		return nil, err
	}

	end := optional.None[uint64]()

	if req.HasEndOffset() {
		if req.GetBeginOffset() > req.GetEndOffset() {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"begin offset %d is greater than end offset %d",
				req.GetBeginOffset(),
				req.GetEndOffset(),
			)
		}

		end = optional.Some(req.GetEndOffset())
	}

	seg, err := s.EventStreams.ReadSegment(
		ctx,
		req.GetEventStreamId().AsStd(),
		types,
		req.GetBeginOffset(),
		end,
	)
	if err != nil {
		if errors.Is(err, source.ErrUnrecognizedStream) {
			return nil, status.Error(codes.NotFound, err.Error())
		}

		if errors.Is(err, source.ErrOffsetOutOfRange) {
			return nil, status.Error(codes.OutOfRange, err.Error())
		}

		return nil, err
	}

	// The first part is the largest that fits in a message. If the segment is
	// larger, the client reads the rest from where the part ends.
	for part := range splitSegment(seg, s.MaxSegmentBytes) {
		return part, nil
	}

	panic("unreachable: splitSegment always yields a segment")
}

// ReadEvents implements the ReadEvents method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *EventStreamConsumerService) ReadEvents(
	req *messaginggrpc.ReadEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
	if s.MaxSegmentBytes <= 0 {
		panic("maximum segment size must be positive")
	}

	if err := req.GetEventStreamId().Validate(); err != nil {
		return status.Errorf(codes.InvalidArgument, "invalid event stream ID: %s", err)
	}

	types, err := requestedTypes(req.GetFilter())
	if err != nil {
		return err
	}

	err = s.EventStreams.Read(
		res.Context(),
		req.GetEventStreamId().AsStd(),
		types,
		req.GetBeginOffset(),
		func(seg source.Segment) error {
			return splitAndSendSegment(res, seg, s.MaxSegmentBytes)
		},
	)

	if errors.Is(err, source.ErrUnrecognizedStream) {
		return status.Error(codes.NotFound, err.Error())
	}

	if errors.Is(err, source.ErrOffsetOutOfRange) {
		return status.Error(codes.OutOfRange, err.Error())
	}

	return err
}

// requestedTypes returns the set of message types that the given filter
// matches, or nil, meaning every type.
func requestedTypes(filter *messaginggrpc.MessageFilter) (*uuidset.SetR, error) {
	if err := filter.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid message filter: %s", err)
	}

	if filter.GetAll() != nil {
		return nil, nil
	}

	types := &uuidset.Set{}

	for _, id := range filter.GetSpecific().GetIds() {
		types.Add(id.AsStd())
	}

	return types.R(), nil
}

// splitAndSendSegment sends a segment to a client, split into several
// contiguous segments if its events are larger than maxBytes combined.
func splitAndSendSegment(
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
	seg source.Segment,
	maxBytes int,
) error {
	for part := range splitSegment(seg, maxBytes) {
		if err := res.Send(part); err != nil {
			return fmt.Errorf("unable to send segment: %w", err)
		}
	}

	return nil
}

// splitSegment returns the contiguous segments that together cover the given
// segment, each holding events that are no larger than maxBytes combined. If a
// single event is larger than maxBytes, it is in a segment on its own. Each
// segment has the stream length of the given segment, if it has one.
//
// Consecutive events with equal headers share a multi-envelope. The header
// counts towards the size of each segment that holds any of those events.
func splitSegment(
	seg source.Segment,
	maxBytes int,
) iter.Seq[*messaginggrpc.EventStreamSegment] {
	return func(yield func(*messaginggrpc.EventStreamSegment) bool) {
		newPart := func(begin uint64) *messaginggrpc.EventStreamSegment {
			b := messaginggrpc.NewEventStreamSegmentBuilder().
				WithEventStreamId(uuidpb.FromStd(seg.StreamID)).
				WithBeginOffset(begin)

			if length, ok := seg.StreamLength.TryGet(); ok {
				b.WithStreamLength(length)
			}

			return b.Build()
		}

		part := newPart(seg.Begin)

		var (
			multi *envelopepb.MultiEnvelope
			size  int
		)

		for _, env := range seg.Envelopes {
			headerSize := proto.Size(env.GetHeader())
			bodySize := proto.Size(env.GetBody())

			if size > 0 && size+headerSize+bodySize > maxBytes {
				part.SetEndOffset(offsetOf(env))

				if !yield(part) {
					return
				}

				part = newPart(part.GetEndOffset())
				multi = nil
				size = 0
			}

			if multi == nil || !multi.TryAppendEnvelope(env) {
				multi = env.AsMultiEnvelope()
				part.SetEnvelopes(append(part.GetEnvelopes(), multi))
				size += headerSize
			}

			size += bodySize
		}

		part.SetEndOffset(seg.End)
		yield(part)
	}
}

// offsetOf returns the offset of the event in env.
func offsetOf(env *envelopepb.Envelope) uint64 {
	return envelopepb.MustGetExtension[*envelopepb.EventStreamPosition](env).GetOffset()
}
