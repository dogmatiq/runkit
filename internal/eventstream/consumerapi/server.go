package consumerapi

import (
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Server is a gRPC server that implements the
// [messaginggrpc.EventStreamConsumerAPI] service.
//
// It serves the application's own event streams, never those of a remote
// engine, so that it only delivers events that the application recorded.
type Server struct {
	// EventStreams is the source of the application's event streams.
	EventStreams *source.Local

	// Logger is the target for log messages produced by the server.
	Logger *slog.Logger
}

// WatchEvents implements the WatchEvents method of
// [messaginggrpc.EventStreamConsumerAPIServer].
func (s *Server) WatchEvents(
	req *messaginggrpc.WatchEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
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

			return sendSegment(res, seg)
		},
	)

	if errors.Is(err, source.ErrConsumerTooSlow) {
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
	types, err := requestedTypes(req.GetFilter())
	if err != nil {
		return err
	}

	err = s.EventStreams.Read(
		res.Context(),
		req.GetEventStreamId(),
		types,
		req.GetBeginOffset(),
		func(seg source.Segment) error {
			return sendSegment(res, seg)
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
func requestedTypes(filter *messaginggrpc.MessageFilter) (*uuidcollection.Set, error) {
	if err := filter.Validate(); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "invalid message filter: %s", err)
	}

	if filter.GetAll() != nil {
		return nil, nil
	}

	return uuidcollection.NewSet(filter.GetSpecific().GetIds()...), nil
}

// sendSegment sends a segment to a client.
func sendSegment(
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
	seg source.Segment,
) error {
	envelopes := slices.Collect(
		envelopepb.AsMultiEnvelopes(
			slices.Values(seg.Envelopes),
		),
	)

	response := messaginggrpc.NewEventStreamSegmentBuilder().
		WithEventStreamId(seg.StreamID).
		WithBeginOffset(seg.Begin).
		WithEndOffset(seg.End).
		WithEnvelopes(envelopes).
		Build()

	if err := res.Send(response); err != nil {
		return fmt.Errorf("unable to send segment: %w", err)
	}

	return nil
}
