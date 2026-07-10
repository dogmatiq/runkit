package eventstream

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/grpc/eventstreamgrpc"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/grpc"
)

type ConsumeAPIServer struct {
	DB *sql.DB
}

var _ eventstreamgrpc.ConsumeAPIServer = (*ConsumeAPIServer)(nil)

// ListStreams lists the streams that the server provides.
func (s *ConsumeAPIServer) ListStreams(
	_ *eventstreamgrpc.ListStreamsRequest,
	res grpc.ServerStreamingServer[eventstreamgrpc.ListStreamsResponse],
) error {
	var seen uuidpb.Set

	poll := func() error {
		rows, err := s.DB.QueryContext(
			res.Context(),
			`SELECT
				id,
				next_offset
			FROM eventstream.streams
			WHERE id != ALL($1)`,
			xsql.UUIDSeq(seen.All()),
		)
		if err != nil {
			return fmt.Errorf("unable to query streams: %s", err)
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
				return fmt.Errorf("unable to scan stream: %s", err)
			}

			if err := res.Send(
				eventstreamgrpc.NewListStreamsResponseBuilder().
					WithStream(
						eventstreamgrpc.NewStreamBuilder().
							WithId(streamID).
							WithNextOffset(nextOffset).
							Build(),
					).
					Build(),
			); err != nil {
				return fmt.Errorf("unable to send response: %s", err)
			}

			seen.Add(streamID)
		}

		if err := rows.Err(); err != nil {
			return fmt.Errorf("unable to iterate streams: %s", err)
		}

		return nil
	}

	for {
		if err := poll(); err != nil {
			return err
		}

		select {
		case <-res.Context().Done():
			return res.Context().Err()
		case <-time.After(25 * time.Millisecond):
		}
	}
}

// ConsumeEvents starts consuming from a specific offset within an event stream.
func (s *ConsumeAPIServer) ConsumeEvents(
	req *eventstreamgrpc.ConsumeEventsRequest,
	res grpc.ServerStreamingServer[eventstreamgrpc.ConsumeEventsResponse],
) error {
	checkpointOffset := req.GetCheckpointOffset()

	poll := func() (bool, error) {
		rows, err := s.DB.QueryContext(
			res.Context(),
			`SELECT
				s.next_offset,
				e.stream_offset,
				e.envelope
			FROM eventstream.streams AS s
			LEFT JOIN eventstream.events AS e
				ON e.stream_id = s.id
				AND e.stream_offset >= $2
				AND e.message_type_id = ANY($3)
			WHERE s.id = $1
			ORDER BY e.stream_offset`,
			xsql.UUID(req.GetStreamId()),
			checkpointOffset,
			xsql.UUIDs(req.GetMessageTypeIds()...),
		)
		if err != nil {
			return false, fmt.Errorf("unable to query events: %s", err)
		}
		defer rows.Close()

		var (
			nextOffset uint64
			envelopes  []*envelopepb.MultiEnvelope
		)

		for rows.Next() {
			var (
				streamOffset *uint64
				envelope     = &envelopepb.Envelope{}
			)

			if err := rows.Scan(
				&nextOffset,
				&streamOffset,
				xsql.Envelope(envelope),
			); err != nil {
				return false, fmt.Errorf("unable to scan event: %s", err)
			}

			if streamOffset == nil {
				break
			}

			envelopepb.SetExtension(
				envelope.GetBody(),
				envelopepb.NewEventStreamPositionBuilder().
					WithStreamId(req.GetStreamId()).
					WithOffset(*streamOffset).
					Build(),
			)

			if len(envelopes) != 0 {
				if envelopes[len(envelopes)-1].TryAppendEnvelope(envelope) {
					continue
				}
			}

			envelopes = append(
				envelopes,
				envelope.AsMultiEnvelope(),
			)
		}

		if err := rows.Err(); err != nil {
			return false, fmt.Errorf("unable to iterate events: %s", err)
		}

		if nextOffset == checkpointOffset {
			return false, nil
		}

		if err := res.Send(
			eventstreamgrpc.NewConsumeEventsResponseBuilder().
				WithEnvelopes(envelopes).
				WithCheckpointOffset(nextOffset).
				Build(),
		); err != nil {
			return false, fmt.Errorf("unable to send events: %s", err)
		}

		checkpointOffset = nextOffset

		return len(envelopes) != 0, nil
	}

	for {
		didWork, err := poll()
		if err != nil {
			return err
		}

		if !didWork {
			select {
			case <-res.Context().Done():
				return res.Context().Err()
			case <-time.After(25 * time.Millisecond):
			}
		}
	}
}
