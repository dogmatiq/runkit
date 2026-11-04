package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ConsumerAPI is a gRPC server that implements the
// [messaginggrpc.EventStreamConsumerAPI] service.
type ConsumerAPI struct {
	// DB is the database connection used to query event streams.
	DB *sql.DB

	// EventStreams is the catalog of event streams served to clients.
	EventStreams *Catalog

	// RecordableEventTypes is the set of message type IDs of every event type
	// that the application can record. A client that requests only event types
	// outside this set is sent any existing events, after which its stream is
	// closed, as no further events can ever be produced for it.
	RecordableEventTypes *uuidcollection.Set

	// Logger is the target for log messages produced by the server.
	Logger *slog.Logger
}

// ListEventStreams returns the event streams offered by the server.
func (s *ConsumerAPI) ListEventStreams(
	_ *messaginggrpc.ListEventStreamsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ListEventStreamsResponse],
) error {
	ctx := res.Context()
	var sent uuidcollection.Set

	for {
		streamIDs, changed := s.EventStreams.StreamIDs().Load()

		for id := range streamIDs.Diff(&sent).All() {
			stream, err := s.loadEventStream(ctx, id)
			if err != nil {
				return err
			}

			response := messaginggrpc.NewListEventStreamsResponseBuilder().
				WithEventStream(stream).
				Build()

			if err := res.Send(response); err != nil {
				return fmt.Errorf("unable to send response: %w", err)
			}

			sent.Add(id)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// ConsumeEvents returns event messages from a single stream, in order,
// starting from a specific offset within an event stream.
//
// If the stream is not recognized it returns a NOT_FOUND error with an attached
// [messaginggrpc.UnrecognizedEventStream] value. If the checkpoint offset is
// beyond the end of the stream it returns an OUT_OF_RANGE error.
//
// If none of the requested event types can be recorded by the application, any
// existing events are delivered and then the gRPC stream is closed. Otherwise
// events are delivered as they are appended, until the client disconnects.
func (s *ConsumerAPI) ConsumeEvents(
	req *messaginggrpc.ConsumeEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ConsumeEventsResponse],
) error {
	ctx := res.Context()
	streamID := req.GetEventStreamId()
	checkpoint := req.GetCheckpointOffset()

	nextOffset, ok, err := s.loadNextOffset(ctx, streamID)
	if err != nil {
		return err
	}

	if !ok {
		return messaginggrpc.UnrecognizedEventStreamError(streamID)
	}

	if checkpoint > nextOffset {
		return status.Errorf(
			codes.OutOfRange,
			"checkpoint offset %d is beyond the end of the stream, which is at offset %d",
			checkpoint,
			nextOffset,
		)
	}

	// Events of types that the application no longer records may still exist
	// on the stream, so every requested type is consumed.
	// The recordable types are only used to decide whether to wait for new
	// events once the existing ones have been delivered.
	types := uuidcollection.NewSet(req.GetMessageTypeIds()...)

	if types.IsEmpty() {
		return nil
	}

	// If the application can not record any of the requested types, no events
	// of those types will be appended beyond the current head of the stream,
	// so the stream is closed once the client has been delivered everything up
	// to it.
	closeAtHead := !s.canRecordAny(types)

	if closeAtHead && checkpoint >= nextOffset {
		return nil
	}

	// The stream exists, but the catalog may not have discovered it yet.
	if err := s.waitForStream(ctx, streamID); err != nil {
		return err
	}

	err = s.EventStreams.Consume(
		ctx,
		streamID,
		types,
		checkpoint,
		func(seg Segment) error {
			if err := sendSegment(res, seg); err != nil {
				return err
			}

			if closeAtHead && seg.End >= nextOffset {
				return errAtHead
			}

			return nil
		},
	)

	switch {
	case errors.Is(err, errAtHead):
		return nil
	case errors.Is(err, ErrStreamNotFound):
		return status.Error(codes.Unavailable, "the server is shutting down")
	default:
		return err
	}
}

// errAtHead is returned by the function given to [Catalog.Consume] to stop
// consuming once the client has been sent every event up to the head of the
// stream.
var errAtHead = errors.New("consumed up to the head of the stream")

// waitForStream blocks until the catalog has the stream with the given ID, or
// ctx is canceled.
func (s *ConsumerAPI) waitForStream(ctx context.Context, id *uuidpb.UUID) error {
	for {
		streamIDs, changed := s.EventStreams.StreamIDs().Load()

		if streamIDs.Has(id) {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-changed:
		}
	}
}

// canRecordAny returns true if the application can record events of at least
// one of the given types.
func (s *ConsumerAPI) canRecordAny(types *uuidcollection.Set) bool {
	for id := range types.All() {
		if s.RecordableEventTypes.Has(id) {
			return true
		}
	}

	return false
}

// sendSegment sends a single response containing the events in a segment,
// with the end of the segment as the client's checkpoint offset.
//
// A segment with no events is still sent, as it advances the client's
// checkpoint past events that are not of a type it is interested in.
func sendSegment(
	res grpc.ServerStreamingServer[messaginggrpc.ConsumeEventsResponse],
	seg Segment,
) error {
	envelopes := make([]*envelopepb.MultiEnvelope, 0, len(seg.Envelopes))

	for _, env := range seg.Envelopes {
		envelopes = append(envelopes, env.AsMultiEnvelope())
	}

	response := messaginggrpc.NewConsumeEventsResponseBuilder().
		WithCheckpointOffset(seg.End).
		WithEnvelopes(envelopes).
		Build()

	if err := res.Send(response); err != nil {
		return fmt.Errorf("unable to send response: %w", err)
	}

	return nil
}

// loadEventStream returns a description of the event stream with the given ID.
func (s *ConsumerAPI) loadEventStream(ctx context.Context, id *uuidpb.UUID) (*messaginggrpc.EventStream, error) {
	nextOffset, ok, err := s.loadNextOffset(ctx, id)
	if err != nil {
		return nil, err
	}

	if !ok {
		return nil, messaginggrpc.UnrecognizedEventStreamError(id)
	}

	return messaginggrpc.NewEventStreamBuilder().
		WithEventStreamId(id).
		WithNextOffset(nextOffset).
		Build(), nil
}

// loadNextOffset returns the next offset of the event stream with the given ID.
//
// ok is false if the stream does not exist.
func (s *ConsumerAPI) loadNextOffset(
	ctx context.Context,
	id *uuidpb.UUID,
) (nextOffset uint64, ok bool, err error) {
	row := s.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(id),
	)

	if err := row.Scan(&nextOffset); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, false, nil
		}

		return 0, false, fmt.Errorf("unable to scan event stream: %w", err)
	}

	return nextOffset, true, nil
}
