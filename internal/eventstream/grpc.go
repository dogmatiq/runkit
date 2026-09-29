package eventstream

import (
	"database/sql"
	"fmt"
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
	DB *sql.DB
}

// ListEventStreams returns the event streams offered by the server.
func (s *ConsumerAPI) ListEventStreams(
	_ *messaginggrpc.ListEventStreamsRequest,
	res grpc.ServerStreamingServer[messaginggrpc.ListEventStreamsResponse],
) error {
	var seen uuidpb.Set

	for {
		if err := s.pollEventStreams(res, &seen); err != nil {
			return err
		}

		select {
		case <-res.Context().Done():
			return res.Context().Err()
		case <-time.After(25 * time.Millisecond):
			// wait before polling
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

func (s *ConsumerAPI) pollEventStreams(
	res grpc.ServerStreamingServer[messaginggrpc.ListEventStreamsResponse],
	seen *uuidpb.Set,
) error {
	rows, err := s.DB.QueryContext(
		res.Context(),
		`SELECT
			id,
			next_offset
		FROM eventstream.streams
		WHERE id != ALL($1)
		AND NOT is_foreign`,
		xsql.UUIDSeq(seen.All()),
	)
	if err != nil {
		return fmt.Errorf("unable to query event streams: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			streamID   = &uuidpb.UUID{}
			nextOffset uint64
		)

		if err := rows.Scan(
			xsql.UUID(streamID),
			&nextOffset,
		); err != nil {
			return fmt.Errorf("unable to scan event stream: %w", err)
		}

		if err := res.Send(
			messaginggrpc.NewListEventStreamsResponseBuilder().
				WithEventStream(
					messaginggrpc.NewEventStreamBuilder().
						WithEventStreamId(streamID).
						WithNextOffset(nextOffset).
						Build(),
				).
				Build(),
		); err != nil {
			return fmt.Errorf("unable to send event stream: %w", err)
		}

		seen.Add(streamID)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate event streams: %w", err)
	}

	return nil
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
