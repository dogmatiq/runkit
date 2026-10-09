package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/grpc"
)

// ConsumerAPI is a gRPC server that implements the
// [messaginggrpc.EventStreamConsumerAPI] service.
type ConsumerAPI struct {
	DB           *sql.DB
	EventStreams *Observer
	Logger       *slog.Logger
}

// ListEventStreams returns the event streams offered by the server.
func (s *ConsumerAPI) ListEventStreams(
	_ *messaginggrpc.ListEventStreamsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ListEventStreamsResponse],
) error {
	streamIDs := make(chan *uuidpb.UUID)
	unsubscribe := s.EventStreams.Subscribe(streamIDs)
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
func (s *ConsumerAPI) ConsumeEvents(
	req *messaginggrpc.ConsumeEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ConsumeEventsResponse],
) error {
	for {
		ok, err := s.pollEvents(req, res)
		if err != nil {
			return err
		}

		if !ok {
			select {
			case <-res.Context().Done():
				return res.Context().Err()
			case <-time.After(25 * time.Millisecond):
				// wait before polling
			}
		}
	}
}

func (s *ConsumerAPI) loadEventStream(ctx context.Context, id *uuidpb.UUID) (*messaginggrpc.EventStream, error) {
	row := s.DB.QueryRowContext(
		ctx,
		`SELECT next_offset
		FROM eventstream.streams
		WHERE id = $1`,
		xsql.UUID(id),
	)

	var nextOffset uint64

	if err := row.Scan(&nextOffset); err != nil {
		return nil, fmt.Errorf("unable to scan event stream: %w", err)
	}

	return messaginggrpc.NewEventStreamBuilder().
		WithEventStreamId(id).
		WithNextOffset(nextOffset).
		Build(), nil
}

func (s *ConsumerAPI) pollEvents(
	req *messaginggrpc.ConsumeEventsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ConsumeEventsResponse],
) (bool, error) {
	rows, err := s.DB.QueryContext(
		res.Context(),
		`SELECT
			envelope,
			stream_offset
		FROM eventstream.events
		WHERE stream_id = $1
		AND stream_offset >= $2
		AND message_type_id = ANY($3)
		ORDER BY stream_offset ASC
		LIMIT 100`,
		xsql.UUID(req.GetEventStreamId()),
		req.GetCheckpointOffset(),
		xsql.UUIDs(req.GetMessageTypeIds()...),
	)
	if err != nil {
		return false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	checkpoint := req.GetCheckpointOffset()

	for rows.Next() {
		var (
			envelope = &envelopepb.Envelope{}
			offset   uint64
		)

		if err := rows.Scan(
			xsql.Envelope(envelope),
			&offset,
		); err != nil {
			return false, fmt.Errorf("unable to scan event: %w", err)
		}

		checkpoint = offset + 1

		envelopepb.SetExtension(
			envelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(req.GetEventStreamId()).
				WithOffset(offset).
				Build(),
		)

		if err := res.Send(
			messaginggrpc.NewConsumeEventsResponseBuilder().
				WithCheckpointOffset(checkpoint).
				WithEnvelopes(
					[]*envelopepb.MultiEnvelope{
						envelope.AsMultiEnvelope(),
					},
				).
				Build(),
		); err != nil {
			return false, fmt.Errorf("unable to send event: %w", err)
		}
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("unable to iterate events: %w", err)
	}

	if checkpoint > req.GetCheckpointOffset() {
		req.SetCheckpointOffset(checkpoint)
		return true, nil
	}

	return false, nil
}
