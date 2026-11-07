package source

import (
	"errors"
	"fmt"
	"iter"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Server provides the [messaginggrpc.EventStreamConsumerAPI] service over the
// event streams of a [Source], so that remote engines can consume them.
type Server struct {
	// EventStreams is the source of the event streams that the server
	// provides.
	EventStreams Source

	// MaxSegmentBytes is the maximum combined size, in bytes, of the events in
	// each segment that the server sends. A segment whose events are larger is
	// sent as several contiguous segments. If a single event is larger than
	// this, it is sent on its own. It must be positive.
	MaxSegmentBytes int
}

// WatchEvents implements the WatchEvents method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *Server) WatchEvents(
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
		func(seg Segment) error {
			m.Lock()
			defer m.Unlock()

			return splitAndSendSegment(res, seg, s.MaxSegmentBytes)
		},
	)

	if errors.Is(err, ErrNotContiguous) {
		return status.Error(codes.Aborted, err.Error())
	}

	return err
}

// ReadEvents implements the ReadEvents method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *Server) ReadEvents(
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
		req.GetEventStreamId(),
		types,
		req.GetBeginOffset(),
		func(seg Segment) error {
			return splitAndSendSegment(res, seg, s.MaxSegmentBytes)
		},
	)

	if errors.Is(err, ErrUnrecognizedStream) {
		return status.Error(codes.NotFound, err.Error())
	}

	if errors.Is(err, ErrOffsetOutOfRange) {
		return status.Error(codes.OutOfRange, err.Error())
	}

	return err
}

// requestedTypes returns the set of message types that the given filter
// matches, or nil, meaning every type.
func requestedTypes(filter *messaginggrpc.MessageFilter) (*uuidcollection.Set, error) {
	if err := filter.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid message filter: %s", err)
	}

	if filter.GetAll() != nil {
		return nil, nil
	}

	return uuidcollection.NewSet(filter.GetSpecific().GetIds()...), nil
}

// splitAndSendSegment sends a segment to a client, split into several
// contiguous segments if its events are larger than maxBytes combined.
func splitAndSendSegment(
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
	seg Segment,
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
// single event is larger than maxBytes, it is in a segment on its own.
//
// Consecutive events with equal headers share a multi-envelope. The header
// counts towards the size of each segment that holds any of those events.
func splitSegment(
	seg Segment,
	maxBytes int,
) iter.Seq[*messaginggrpc.EventStreamSegment] {
	return func(yield func(*messaginggrpc.EventStreamSegment) bool) {
		part := messaginggrpc.NewEventStreamSegmentBuilder().
			WithEventStreamId(seg.StreamID).
			WithBeginOffset(seg.Begin).
			Build()

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

				part = messaginggrpc.NewEventStreamSegmentBuilder().
					WithEventStreamId(seg.StreamID).
					WithBeginOffset(part.GetEndOffset()).
					Build()
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
