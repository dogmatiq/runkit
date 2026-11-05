package consumerapi

import (
	"errors"
	"fmt"
	"log/slog"
	"sync"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
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

// WatchEvents delivers events of the requested types as they are appended to
// any of the application's event streams, until the client disconnects.
//
// Every stream is delivered from its end. If a stream can no longer be
// delivered contiguously, it returns an UNAVAILABLE error, after which the
// client is expected to reconnect.
func (s *Server) WatchEvents(
	req *messaginggrpc.WatchEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
	var (
		m    sync.Mutex
		ends uuidcollection.Map[uint64]
	)

	return s.EventStreams.Watch(
		res.Context(),
		requestedTypes(req.GetMessageTypeIds()),
		func(seg source.Segment) error {
			// The source delivers segments of different streams concurrently.
			m.Lock()
			defer m.Unlock()

			if end, ok := ends.Get(seg.StreamID); ok && seg.Begin != end {
				return status.Errorf(
					codes.Unavailable,
					"event stream %s can not be delivered contiguously: segment begins at offset %d, but the previous segment ended at offset %d",
					seg.StreamID,
					seg.Begin,
					end,
				)
			}

			if err := sendSegment(res, seg); err != nil {
				return err
			}

			ends.Set(seg.StreamID, seg.End)

			return nil
		},
	)
}

// ReadEvents delivers the events of the requested types from one of the
// application's event streams, beginning at the requested offset, until it
// reaches the end of the stream.
//
// If the stream is not recognized it returns a NOT_FOUND error with an attached
// [messaginggrpc.UnrecognizedEventStream] value. If the offset is beyond the
// end of the stream it returns an OUT_OF_RANGE error.
func (s *Server) ReadEvents(
	req *messaginggrpc.ReadEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
) error {
	streamID := req.GetEventStreamId()

	err := s.EventStreams.Read(
		res.Context(),
		streamID,
		requestedTypes(req.GetMessageTypeIds()),
		req.GetBeginOffset(),
		func(seg source.Segment) error {
			return sendSegment(res, seg)
		},
	)

	if errors.Is(err, source.ErrUnrecognizedStream) {
		return messaginggrpc.UnrecognizedEventStreamError(streamID)
	}

	if errors.Is(err, source.ErrOffsetOutOfRange) {
		return status.Error(codes.OutOfRange, err.Error())
	}

	return err
}

// loadNextOffset returns the next offset of the event stream with the given ID.
//
// ok is false if the stream does not exist.
// requestedTypes returns the set of message types requested by a client, or
// nil, meaning every type, if the client requested none.
func requestedTypes(ids []*uuidpb.UUID) *uuidcollection.Set {
	if len(ids) == 0 {
		return nil
	}

	return uuidcollection.NewSet(ids...)
}

// sendSegment sends a segment to a client.
func sendSegment(
	res grpc.ServerStreamingServer[messaginggrpc.EventStreamSegment],
	seg source.Segment,
) error {
	envelopes := make([]*envelopepb.MultiEnvelope, 0, len(seg.Envelopes))

	for _, env := range seg.Envelopes {
		envelopes = append(envelopes, env.AsMultiEnvelope())
	}

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
