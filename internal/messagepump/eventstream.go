package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// AcquireEventDelivery attempts to acquire the next pending event for the
// handler.
//
// If there are no relevant events available on the chosen stream, it advances
// the checkpoint offset to the end of the stream so that the stream is not
// re-acquired until new events arrive.
func AcquireEventDelivery(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	handlerKey *uuidpb.UUID,
	messageTypeIDs *uuidpb.Set,
) (Delivery, bool, error) {
	// Acquire a stream for the handler to read from, then find the next pending
	// event on that stream (if any) in a single round-trip.
	//
	// The "inserted" CTE tracks a previously untracked stream by inserting a
	// new checkpoint row; the inserted row is implicitly locked by this
	// transaction. If every stream is already tracked, "inserted" is empty and
	// the "selected" CTE locks the existing checkpoint row with the largest gap
	// between the stream's next offset and the handler's checkpoint offset,
	// using FOR UPDATE SKIP LOCKED to avoid contending with other workers.
	//
	// At most one of the two CTEs produces a row. The outer SELECT then joins
	// the acquired stream to the events table to fetch the next event of a
	// relevant type at or after the checkpoint offset.
	row := tx.QueryRowContext(
		ctx,
		`WITH inserted AS (
			INSERT INTO eventstream.handler_checkpoints (
				handler_key,
				stream_id
			)
			SELECT
				$1,
				s.id
			FROM eventstream.streams AS s
			WHERE NOT EXISTS (
				SELECT 1
				FROM eventstream.handler_checkpoints AS h
				WHERE h.handler_key = $1
				AND h.stream_id = s.id
			)
			ORDER BY random()
			LIMIT 1
			ON CONFLICT DO NOTHING
			RETURNING
				stream_id,
				checkpoint_offset,
				failures
		), selected AS (
			SELECT
				s.id,
				h.checkpoint_offset,
				h.failures
			FROM eventstream.streams AS s
			INNER JOIN eventstream.handler_checkpoints AS h
				ON h.stream_id = s.id
			WHERE h.handler_key = $1
				AND h.resume_at <= clock_timestamp()
				AND s.next_offset > h.checkpoint_offset
				AND NOT EXISTS (SELECT 1 FROM inserted)
			ORDER BY (s.next_offset - h.checkpoint_offset) DESC
			FOR UPDATE OF h SKIP LOCKED
			LIMIT 1
		), checkpoint AS (
			SELECT * FROM inserted
			UNION ALL
			SELECT * FROM selected
		)
		SELECT
			s.id,
			COALESCE(e.stream_offset, 0) AS event_offset,
			c.checkpoint_offset,
			c.failures,
			e.message_id,
			e.message_type_id,
			e.envelope,
			e.message_id IS NOT NULL AS has_event,
			s.next_offset
		FROM checkpoint AS c
		INNER JOIN eventstream.streams AS s
			ON s.id = c.stream_id
		LEFT JOIN eventstream.events AS e
			ON e.stream_id = c.stream_id
			AND e.stream_offset >= c.checkpoint_offset
			AND e.message_type_id = ANY($2)
		ORDER BY e.stream_offset
		LIMIT 1`,
		xsql.UUID(handlerKey),
		xsql.UUIDSeq(messageTypeIDs.All()),
	)

	var (
		delivery = Delivery{
			MessageID:     &uuidpb.UUID{},
			MessageTypeID: &uuidpb.UUID{},
			Stream: &DeliveryStream{
				ID: &uuidpb.UUID{},
			},
		}
		hasEvent   bool
		nextOffset uint64
	)
	if err := row.Scan(
		xsql.UUID(delivery.Stream.ID),
		&delivery.Stream.EventOffset,
		&delivery.Stream.CheckpointOffset,
		&delivery.Failures,
		xsql.UUID(delivery.MessageID),
		xsql.UUID(delivery.MessageTypeID),
		&delivery.EnvelopeBytes,
		&hasEvent,
		&nextOffset,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Delivery{}, false, nil
		}

		return Delivery{}, false, fmt.Errorf("unable to acquire delivery: %w", err)
	}

	if hasEvent {
		return delivery, true, nil
	}

	// No matching events are available on this stream.
	//
	// Advance the checkpoint to the end of the stream so that this stream is
	// not re-acquired unless new events are appended.
	if nextOffset > delivery.Stream.CheckpointOffset {
		if err := AdvanceStreamCheckpoint(
			ctx,
			tx,
			logger,
			handlerKey,
			delivery.Stream.ID,
			delivery.Stream.CheckpointOffset,
			nextOffset,
		); err != nil {
			return Delivery{}, false, err
		}
	}

	return Delivery{}, false, nil
}

// AdvanceStreamCheckpoint updates the checkpoint offset of a handler's row in
// eventstream.handler_checkpoints and resets its failure counter.
func AdvanceStreamCheckpoint(
	ctx context.Context,
	tx *sql.Tx,
	logger *slog.Logger,
	handlerKey, streamID *uuidpb.UUID,
	oldCheckpointOffset, newCheckpointOffset uint64,
) error {
	if newCheckpointOffset <= oldCheckpointOffset {
		panic(fmt.Sprintf(
			"new checkpoint offset (%d) must be greater than old checkpoint offset (%d)",
			newCheckpointOffset,
			oldCheckpointOffset,
		))
	}

	if err := SetCheckpointOffset(
		ctx,
		tx,
		handlerKey,
		streamID,
		oldCheckpointOffset,
		newCheckpointOffset,
	); err != nil {
		return err
	}

	logger.DebugContext(
		ctx,
		"advanced stream checkpoint offset",
		xslog.UUID("stream_id", streamID),
		slog.Uint64("old_checkpoint_offset", oldCheckpointOffset),
		slog.Uint64("new_checkpoint_offset", newCheckpointOffset),
	)

	return nil
}

// SetCheckpointOffset sets the checkpoint offset of a handler's row in
// eventstream.handler_checkpoints and resets its failure counter.
//
// Unlike [AdvanceStreamCheckpoint], the new offset need not be greater than the
// old offset. It is used when the new offset is supplied by the handler itself
// (via optimistic concurrency control), which may set it to any value.
func SetCheckpointOffset(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey, streamID *uuidpb.UUID,
	oldCheckpointOffset, newCheckpointOffset uint64,
) error {
	// The checkpoint_offset clause is purely defensive, the row should already
	// be locked FOR UPDATE by [AcquireEventDelivery], so the observed value
	// must still be current. If this UPDATE affects 0 rows, another transaction
	// has modified the row concurrently, which would indicate a locking bug in
	// the engine.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3
			AND checkpoint_offset = $4`,
		newCheckpointOffset,
		xsql.UUID(handlerKey),
		xsql.UUID(streamID),
		oldCheckpointOffset,
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return nil
}

// PostponeEventDelivery reschedules consumption of a stream after delay, and
// sets the failure count to the given value.
func PostponeEventDelivery(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
	delivery Delivery,
	delay time.Duration,
) error {
	// The checkpoint_offset clause is purely defensive, the row should already
	// be locked FOR UPDATE by [AcquireEventDelivery], so the observed value
	// must still be current. If this UPDATE affects 0 rows, another transaction
	// has modified the row concurrently, which would indicate a locking bug.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			failures = $1,
			resume_at = clock_timestamp() + $2
		WHERE handler_key = $3
			AND stream_id = $4
			AND checkpoint_offset = $5`,
		delivery.Failures,
		delay,
		xsql.UUID(handlerKey),
		xsql.UUID(delivery.Stream.ID),
		delivery.Stream.CheckpointOffset,
	); err != nil {
		return fmt.Errorf("unable to postpone stream consumption: %w", err)
	}

	return nil
}
