package scanner

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Historical reads historical events from a specific event stream and
// delivers them to a single consumer until it catches up to the tail of the
// stream.
type Historical struct {
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

// Run delivers historical events to the consumer until it has caught up to the
// tail of the stream, at which point the consumer is sent to s.Synced and Run
// returns nil.
//
// It returns nil without sending to s.Synced if the consumer is gone before it
// catches up.
//
// If Run returns an error it may be called again and will resume from the
// consumer's checkpoint. Run must not be called concurrently.
func (s *Historical) Run(ctx context.Context) error {
	if s.Consumer.MessageTypes.IsEmpty() {
		panic("consumer message types must not be empty")
	}

	if s.BatchSize <= 0 {
		panic("batch size must be positive")
	}

	// Stop as soon as the consumer is gone, even if a query is in progress.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		select {
		case <-ctx.Done():
		case <-s.Consumer.Gone.Chan():
			cancel()
		}
	}()

	err := s.run(ctx)

	// The consumer being gone is not a failure; there is simply nothing left
	// to do. Reporting an error would cause the scanner to be restarted.
	if s.Consumer.Gone.IsSet() {
		return nil
	}

	return err
}

// run delivers batches until the consumer catches up, as described by [Run].
func (s *Historical) run(ctx context.Context) error {
	query := s.buildQuery()

	for {
		batch, exhausted, err := s.readBatch(ctx, query)
		if err != nil {
			return err
		}

		// An empty batch that does not advance the checkpoint tells the
		// consumer nothing, so it is not delivered.
		if len(batch.Envelopes) != 0 || batch.Checkpoint != s.Consumer.Checkpoint.Load() {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case s.Consumer.Batches <- batch:
			}

			s.Consumer.Checkpoint.Store(batch.Checkpoint)
		}

		if exhausted {
			// Events may be appended between this read and the consumer being
			// registered with a [Contemporary] scanner. That scanner serves the
			// consumer from its cache if it can, or sends it back to historical
			// consumption if the cache has already moved past it.
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
func (s *Historical) buildQuery() string {
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
// checkpoint.
//
// It returns a batch containing the events and the checkpoint from which the
// consumer should resume. It also returns a boolean indicating whether the
// stream has been exhausted, that is, there are no more relevant events on the
// stream at this time.
//
// When the stream is exhausted, the batch's checkpoint is the stream's next
// offset, skipping past any trailing events of irrelevant types. The
// checkpoint is never moved backwards, as the consumer may already be ahead of
// the stream's next offset as we observed it.
func (s *Historical) readBatch(
	ctx context.Context,
	query string,
) (Batch, bool, error) {
	checkpoint := s.Consumer.Checkpoint.Load()

	rows, err := s.DB.QueryContext(
		ctx,
		query,
		checkpoint,
	)
	if err != nil {
		return Batch{}, false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// The first row is always the stream's next offset. If there are no rows at
	// all, the stream does not exist.
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return Batch{}, false, fmt.Errorf("unable to advance to first row: %w", err)
		}
		return Batch{}, false, fmt.Errorf("event stream not found")
	}

	var next uint64
	if err := rows.Scan(
		&next,
		new(any),
	); err != nil {
		return Batch{}, false, fmt.Errorf("unable to scan next offset: %w", err)
	}

	batch := Batch{
		StreamID:   s.Consumer.StreamID,
		Checkpoint: checkpoint,
	}

	for rows.Next() {
		var (
			offset uint64
			env    = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&offset,
			xsql.Envelope(env),
		); err != nil {
			return Batch{}, false, fmt.Errorf("unable to scan event: %w", err)
		}

		envelopepb.SetExtension(
			env.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(s.Consumer.StreamID).
				WithOffset(offset).
				Build(),
		)

		batch.Envelopes = append(batch.Envelopes, env)
		batch.Checkpoint = offset + 1
	}

	if err := rows.Err(); err != nil {
		return Batch{}, false, fmt.Errorf("unable to iterate events: %w", err)
	}

	exhausted := len(batch.Envelopes) < s.BatchSize

	if exhausted {
		batch.Checkpoint = max(batch.Checkpoint, next)
	}

	return batch, exhausted, nil
}
