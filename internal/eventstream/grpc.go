package eventstream

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// consumeBufferSize is the number of batches buffered for each client of
// [ConsumerAPI.ConsumeEvents]. When the buffer is full the client is returned
// to reading historical events, which blocks until the client catches up.
const consumeBufferSize = 10

// ConsumerAPI is a gRPC server that implements the
// [messaginggrpc.EventStreamConsumerAPI] service.
type ConsumerAPI struct {
	// DB is the database connection used to query event streams.
	DB *sql.DB

	// Catalog tracks the set of available event streams.
	Catalog *Catalog

	// Reader delivers events to clients.
	Reader *Reader

	// RecordableEventTypes is the set of message type IDs of every event type
	// that the application can record. A client that requests only event types
	// outside this set is sent any existing events, after which its stream is
	// closed, as no further events can ever be produced for it.
	RecordableEventTypes *uuidpb.Set

	// Logger is the target for log messages produced by the server.
	Logger *slog.Logger
}

// ListEventStreams returns the event streams offered by the server.
func (s *ConsumerAPI) ListEventStreams(
	_ *messaginggrpc.ListEventStreamsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ListEventStreamsResponse],
) error {
	streamIDs := make(chan *uuidpb.UUID)
	unsubscribe := s.Catalog.Subscribe(streamIDs)
	defer unsubscribe()

	for {
		select {
		case <-res.Context().Done():
			return res.Context().Err()
		case id := <-streamIDs:
			stream, err := s.loadEventStream(res.Context(), id)
			if err != nil {
				return err
			}

			response := messaginggrpc.NewListEventStreamsResponseBuilder().
				WithEventStream(stream).
				Build()

			if err := res.Send(response); err != nil {
				return fmt.Errorf("unable to send response: %w", err)
			}
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
	// on the stream, so the consumer always subscribes to every requested type.
	// The recordable types are only used to decide whether to wait for new
	// events once the existing ones have been delivered.
	types := uuidpb.NewSet(req.GetMessageTypeIds()...)

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

	batches := make(chan Batch, consumeBufferSize)
	unsubscribe := s.Reader.Subscribe(streamID, checkpoint, types, batches)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case batch := <-batches:
			if err := sendBatch(res, batch); err != nil {
				return err
			}

			if closeAtHead && batch.Checkpoint >= nextOffset {
				return nil
			}
		}
	}
}

// canRecordAny returns true if the application can record events of at least
// one of the given types.
func (s *ConsumerAPI) canRecordAny(types *uuidpb.Set) bool {
	for id := range types.All() {
		if s.RecordableEventTypes.Has(id) {
			return true
		}
	}

	return false
}

// sendBatch sends a single response containing the events in the batch.
//
// A batch with no events is still sent, as it advances the client's checkpoint
// past events that are not of a type it is interested in.
func sendBatch(
	res grpc.ServerStreamingServer[messaginggrpc.ConsumeEventsResponse],
	batch Batch,
) error {
	envelopes := make([]*envelopepb.MultiEnvelope, 0, len(batch.Envelopes))

	for _, env := range batch.Envelopes {
		envelopes = append(envelopes, env.AsMultiEnvelope())
	}

	response := messaginggrpc.NewConsumeEventsResponseBuilder().
		WithCheckpointOffset(batch.Checkpoint).
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
