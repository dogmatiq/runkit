package projection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/concurrency"
	"github.com/dogmatiq/reference-engine/internal/messagepump"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// EventPump is a [messagepump.Driver] that delivers pending events to a
// projection message handler.
type EventPump struct {
	DB           *sql.DB
	Handler      dogma.ProjectionMessageHandler
	Identity     *identitypb.Identity
	Concurrency  dogma.ConcurrencyPreference
	EventTypeIDs []string
	Logger       *slog.Logger
}

// AcquireDelivery attempts to acquire the next pending event for the handler on
// one of its tracked event streams.
//
// If there are no relevant events available on the chosen stream, it advances
// the checkpoint offset to the end of the stream so that the stream is not
// re-acquired until new events arrive.
func (p *EventPump) AcquireDelivery(
	ctx context.Context,
	tx *sql.Tx,
) (messagepump.Delivery, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			a.stream_id,
			s.next_offset,
			a.checkpoint_offset,
			a.failures,
			COALESCE(e.stream_offset, s.next_offset),
			e.message_id,
			e.message_type_id,
			e.envelope
		FROM eventstream.acquire_for_read($1) AS a
		INNER JOIN eventstream.streams AS s
			ON s.id = a.stream_id
		LEFT JOIN eventstream.events AS e
			ON e.stream_id = a.stream_id
			AND e.stream_offset >= a.checkpoint_offset
			AND e.message_type_id = ANY($2)
		ORDER BY e.stream_offset
		LIMIT 1`,
		xsql.UUID(p.Identity.GetKey()),
		p.EventTypeIDs,
	)

	var (
		nextStreamOffset uint64
		checkpointOffset *uint64
	)

	delivery := messagepump.Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
		Stream: &messagepump.DeliveryStream{
			ID: &uuidpb.UUID{},
		},
	}

	if err := row.Scan(
		xsql.UUID(delivery.Stream.ID),
		&nextStreamOffset,
		&checkpointOffset,
		&delivery.Failures,
		&delivery.Stream.EventOffset,
		xsql.UUID(delivery.MessageID),
		xsql.UUID(delivery.MessageTypeID),
		&delivery.EnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return messagepump.Delivery{}, false, nil
		}

		return messagepump.Delivery{}, false, fmt.Errorf("unable to acquire delivery: %w", err)
	}

	// If the engine has no recorded checkpoint for this stream yet, ask the
	// handler for its authoritative offset, persist it, and defer delivery to
	// the next acquisition cycle.
	if checkpointOffset == nil {
		return messagepump.Delivery{}, false, p.initializeCheckpoint(ctx, tx, delivery.Stream.ID)
	}

	delivery.Stream.CheckpointOffset = *checkpointOffset

	// If we found a pending event, return a delivery for it.
	if delivery.Stream.EventOffset < nextStreamOffset {
		return delivery, true, nil
	}

	// No matching events are available on this stream. Advance the checkpoint
	// to the end of the stream so that the stream is not re-acquired until
	// new events arrive.
	if nextStreamOffset > *checkpointOffset {
		if err := messagepump.AdvanceStreamCheckpoint(
			ctx,
			tx,
			p.Logger,
			p.Identity.GetKey(),
			delivery.Stream.ID,
			*checkpointOffset,
			nextStreamOffset,
		); err != nil {
			return messagepump.Delivery{}, false, err
		}
	}

	return messagepump.Delivery{}, false, nil
}

// initializeCheckpoint asks the handler for its committed checkpoint offset on
// a stream the engine has not seen before and persists that offset so that
// future acquisitions can read it directly.
func (p *EventPump) initializeCheckpoint(
	ctx context.Context,
	tx *sql.Tx,
	streamID *uuidpb.UUID,
) error {
	var handlerCheckpoint uint64

	if err := xerrors.ConvertPanicToError(
		func() error {
			var err error
			handlerCheckpoint, err = p.Handler.CheckpointOffset(ctx, streamID.AsString())
			return err
		},
	); err != nil {
		return fmt.Errorf("unable to get checkpoint offset from handler: %w", err)
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
			checkpoint_offset = $1
		WHERE handler_key = $2
			AND stream_id = $3
			AND checkpoint_offset IS NULL`,
		handlerCheckpoint,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(streamID),
	); err != nil {
		return fmt.Errorf("unable to persist handler checkpoint: %w", err)
	}

	p.Logger.DebugContext(
		ctx,
		"synced initial checkpoint offset with handler",
		xslog.UUID("stream_id", streamID),
		slog.Uint64("checkpoint_offset", handlerCheckpoint),
	)

	return nil
}

// HandleDelivery dispatches an event to the projection handler.
func (p *EventPump) HandleDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	envelope *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var event dogma.Event

	if err := xmessage.UnpackMessage(
		envelope,
		&event,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal event",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	var nextCheckpointOffset uint64

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		tx,
		p.Identity.GetKey(),
		p.Concurrency,
		func() error {
			return xerrors.ConvertPanicToError(
				func() error {
					var err error
					nextCheckpointOffset, err = p.Handler.HandleEvent(
						ctx,
						&eventScope{
							streamID:         delivery.Stream.ID.AsString(),
							offset:           delivery.Stream.EventOffset,
							recordedAt:       envelope.GetBody().GetCreatedAt().AsTime(),
							checkpointOffset: delivery.Stream.CheckpointOffset,
							logger:           logger,
						},
						event,
					)
					return err
				},
			)
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	if nextCheckpointOffset != delivery.Stream.EventOffset+1 {
		logger.WarnContext(
			ctx,
			"optimistic concurrency conflict",
			slog.Uint64("handler_checkpoint_offset", nextCheckpointOffset),
		)
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
			AND checkpoint_offset = $4`,
		nextCheckpointOffset,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(delivery.Stream.ID),
		delivery.Stream.CheckpointOffset,
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	p.Logger.DebugContext(
		ctx,
		"synced checkpoint offset with handler",
		xslog.UUID("stream_id", delivery.Stream.ID),
		slog.Uint64("checkpoint_offset", nextCheckpointOffset),
	)

	return nil
}

// PostponeDelivery reschedules consumption of the stream after delay.
func (p *EventPump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	return messagepump.PostponeStreamDelivery(
		ctx,
		tx,
		p.Identity.GetKey(),
		delivery.Stream.ID,
		delivery.Stream.CheckpointOffset,
		delivery.Failures,
		delay,
	)
}
