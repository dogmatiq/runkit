package eventstream

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// HistoricalScanner reads historical events from a specific event stream and
// delivers them to a single consumer until it catches up to the tail of the
// stream.
type HistoricalScanner struct {
	// DB is the database connection used to read events from the stream.
	DB *sql.DB

	// Consumer is the consumer to which events are delivered. Its Checkpoint is
	// advanced as events are delivered. Its MessageTypes must not be empty.
	Consumer *Consumer

	// Synced is a channel on which the consumer is sent once it has caught up
	// to the tail of the stream. At that point its Checkpoint is the stream's
	// next offset as of the final read, which is where contemporary consumption
	// should begin.
	Synced chan<- *Consumer

	// BatchSize is the maximum number of events to read per query. It must be
	// positive.
	BatchSize int
}

// historicalEvent is an event read from the stream along with its offset.
type historicalEvent struct {
	Offset   uint64
	Envelope *envelopepb.Envelope
}

// Run delivers historical events to the consumer until it has caught up to the
// tail of the stream, at which point the consumer is sent to s.Synced and Run
// returns nil.
//
// It returns nil without sending to s.Synced if the consumer is gone before it
// catches up.
//
// If Run returns an error it may be called again and will resume from the
// consumer's checkpoint. Run must not be called concurrently.
func (s *HistoricalScanner) Run(ctx context.Context) error {
	if s.Consumer.MessageTypes.IsEmpty() {
		panic("consumer message types must not be empty")
	}

	if s.BatchSize <= 0 {
		panic("batch size must be positive")
	}

	query := s.buildQuery()

	for {
		events, next, err := s.readBatch(ctx, query)
		if err != nil {
			return err
		}

		for _, ev := range events {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-s.Consumer.Gone.Chan():
				return nil
			case s.Consumer.Envelopes <- ev.Envelope:
			}

			s.Consumer.Checkpoint.Store(ev.Offset + 1)
		}

		// A partial batch means there are no more relevant events on the
		// stream at this time.
		if len(events) < s.BatchSize {
			// Skip past any trailing events of irrelevant types so that the
			// consumer resumes at the tail of the stream.
			//
			// The checkpoint is never moved backwards, as the consumer may
			// already be ahead of the stream's next offset as we observed it.
			//
			// Events may be appended between this read and the consumer being
			// registered with a ContemporaryScanner. That scanner serves the
			// consumer from its cache if it can, or sends it back to historical
			// consumption if the cache has already moved past it.
			s.Consumer.Checkpoint.Store(max(s.Consumer.Checkpoint.Load(), next))

			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.Synced <- s.Consumer:
				return nil
			}
		}
	}
}

// buildQuery returns the SQL used to read a batch of events.
//
// The query returns two kinds of rows via UNION ALL: the first row contains the
// stream's next offset (and a NULL envelope); subsequent rows contain up to
// s.BatchSize events of the consumer's message types.
//
// If a full batch is returned, reading resumes at the offset after the last
// event in the batch. Otherwise, there are no more relevant events on the
// stream at this time, so the consumer can skip to the stream's next offset,
// past any remaining events of irrelevant types.
//
// The stream ID, message type IDs and batch size are embedded as constants so
// that PostgreSQL knows them when planning the query. In particular, it can
// choose between scanning events in offset order and searching directly for
// specific message types. Only the checkpoint is a parameter, as it changes
// between reads.
func (s *HistoricalScanner) buildQuery() string {
	var messageTypeIDs []string
	for id := range s.Consumer.MessageTypes.All() {
		messageTypeIDs = append(messageTypeIDs, id.AsString())
	}

	return fmt.Sprintf(
		`SELECT stream_offset, envelope
		FROM (
			(
				SELECT
					0 AS position,
					next_offset AS stream_offset,
					NULL::bytea AS envelope
				FROM eventstream.streams
				WHERE id = '%s'
			)
			UNION ALL
			(
				SELECT
					1 AS position,
					stream_offset,
					envelope
				FROM eventstream.events
				WHERE stream_id = '%s'
					AND stream_offset >= $1
					AND message_type_id IN ('%s')
				ORDER BY stream_offset
				LIMIT %d
			)
		) AS results
		ORDER BY position, stream_offset`,
		s.Consumer.StreamID.AsString(),
		s.Consumer.StreamID.AsString(),
		strings.Join(messageTypeIDs, "','"),
		s.BatchSize,
	)
}

// readBatch reads up to s.BatchSize relevant events starting at the consumer's
// checkpoint. It also returns the stream's next offset as of the read.
func (s *HistoricalScanner) readBatch(
	ctx context.Context,
	query string,
) ([]historicalEvent, uint64, error) {
	rows, err := s.DB.QueryContext(
		ctx,
		query,
		s.Consumer.Checkpoint.Load(),
	)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// The first row is always the stream's next offset. If there are no rows at
	// all, the stream does not exist.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, 0, fmt.Errorf("unable to advance to first row: %w", err)
		}
		return nil, 0, fmt.Errorf("event stream not found")
	}

	var next uint64
	if err := rows.Scan(
		&next,
		new(any),
	); err != nil {
		return nil, 0, fmt.Errorf("unable to scan next offset: %w", err)
	}

	var events []historicalEvent

	for rows.Next() {
		ev := historicalEvent{
			Envelope: &envelopepb.Envelope{},
		}

		if err := rows.Scan(
			&ev.Offset,
			xsql.Envelope(ev.Envelope),
		); err != nil {
			return nil, 0, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			ev.Envelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(s.Consumer.StreamID).
				WithOffset(ev.Offset).
				Build(),
		)

		events = append(events, ev)
	}

	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("unable to iterate events: %w", err)
	}

	return events, next, nil
}
