package messagepump

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

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
			"advanced checkpoint offset (%d) must be greater than expected checkpoint offset (%d) for handler %s on stream %s",
			newCheckpointOffset,
			oldCheckpointOffset,
			handlerKey.AsString(),
			streamID.AsString(),
		))
	}

	// The checkpoint_offset clause is purely defensive, the row should already
	// be locked FOR UPDATE by eventstream.acquire_for_read(), so the observed
	// value must still be current. If this UPDATE affects 0 rows, another
	// transaction has modified the row concurrently, which would indicate a
	// locking bug.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3
			AND COALESCE(checkpoint_offset, 0) = $4`,
		newCheckpointOffset,
		xsql.UUID(handlerKey),
		xsql.UUID(streamID),
		oldCheckpointOffset,
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
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
	// be locked FOR UPDATE by eventstream.acquire_for_read(), so the observed
	// value must still be current. If this UPDATE affects 0 rows, another
	// transaction has modified the row concurrently, which would indicate a
	// locking bug.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			failures = $1,
			resume_at = clock_timestamp() + $2
		WHERE handler_key = $3
			AND stream_id = $4
			AND COALESCE(checkpoint_offset, 0) = $5`,
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
