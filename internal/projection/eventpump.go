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
func (p *EventPump) AcquireDelivery(ctx context.Context, tx *sql.Tx) (messagepump.Delivery, bool, error) {
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

	del := messagepump.Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
		Stream: &messagepump.Stream{
			ID: &uuidpb.UUID{},
		},
	}

	if err := row.Scan(
		xsql.UUID(del.Stream.ID),
		&nextStreamOffset,
		&checkpointOffset,
		&del.Failures,
		&del.Stream.EventOffset,
		xsql.UUID(del.MessageID),
		xsql.UUID(del.MessageTypeID),
		&del.EnvelopeBytes,
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
		return messagepump.Delivery{}, false, p.initializeCheckpoint(ctx, tx, del.Stream.ID, nextStreamOffset)
	}

	del.Stream.CheckpointOffset = *checkpointOffset

	// If we found a pending event, return a delivery for it.
	if del.Stream.EventOffset < nextStreamOffset {
		return del, true, nil
	}

	// No matching events are available on this stream. Advance the checkpoint
	// to the end of the stream so that the stream is not re-acquired until
	// new events arrive.
	if nextStreamOffset > *checkpointOffset {
		if err := p.advanceCheckpoint(ctx, tx, del.Stream.ID, nextStreamOffset); err != nil {
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
	nextStreamOffset uint64,
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

	if handlerCheckpoint > nextStreamOffset {
		p.Logger.WarnContext(
			ctx,
			"handler reported checkpoint offset beyond the end of the stream",
			xslog.UUID("stream_id", streamID),
			slog.Uint64("handler_checkpoint_offset", handlerCheckpoint),
			slog.Uint64("stream_next_offset", nextStreamOffset),
		)
	}

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1
		WHERE handler_key = $2
			AND stream_id = $3`,
		handlerCheckpoint,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(streamID),
	); err != nil {
		return fmt.Errorf("unable to persist handler checkpoint: %w", err)
	}

	return nil
}

// advanceCheckpoint advances the handler's checkpoint offset for the stream to
// the given offset, which is the offset after the last event on the stream
// known at the time of acquisition.
func (p *EventPump) advanceCheckpoint(
	ctx context.Context,
	tx *sql.Tx,
	streamID *uuidpb.UUID,
	offset uint64,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3`,
		offset,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(streamID),
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return nil
}

// HandleDelivery dispatches an event to the projection handler.
func (p *EventPump) HandleDelivery(ctx context.Context, dc *messagepump.DeliveryContext) error {
	var (
		eventEnvelope = &envelopepb.Envelope{}
		event         dogma.Event
	)

	if err := xmessage.Unpack(
		dc.EnvelopeBytes,
		eventEnvelope,
		&event,
	); err != nil {
		dc.Logger.ErrorContext(
			ctx,
			"unable to unmarshal event",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	logger := dc.Logger.With(
		slog.Uint64("checkpoint_offset", dc.Stream.CheckpointOffset),
		xslog.Envelope("event", eventEnvelope),
	)

	var nextCheckpointOffset uint64

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		dc.Tx,
		p.Identity.GetKey(),
		p.Concurrency,
		func() error {
			return xerrors.ConvertPanicToError(
				func() error {
					var err error
					nextCheckpointOffset, err = p.Handler.HandleEvent(
						ctx,
						&eventScope{
							streamID:         dc.Stream.ID.AsString(),
							offset:           dc.Stream.EventOffset,
							recordedAt:       eventEnvelope.GetBody().GetCreatedAt().AsTime(),
							checkpointOffset: dc.Stream.CheckpointOffset,
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

	if nextCheckpointOffset != dc.Stream.EventOffset+1 {
		logger.WarnContext(
			ctx,
			"optimistic concurrency conflict",
			slog.Uint64("engine_checkpoint_offset", dc.Stream.CheckpointOffset),
			slog.Uint64("handler_checkpoint_offset", nextCheckpointOffset),
		)
	}

	row := dc.Tx.QueryRowContext(
		ctx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3
		RETURNING (
			SELECT next_offset
			FROM eventstream.streams
			WHERE id = $3
		)`,
		nextCheckpointOffset,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(dc.Stream.ID),
	)

	var nextStreamOffset uint64
	if err := row.Scan(&nextStreamOffset); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	if nextCheckpointOffset > nextStreamOffset {
		logger.WarnContext(
			ctx,
			"handler reported checkpoint offset beyond the end of the stream",
			slog.Uint64("handler_checkpoint_offset", nextCheckpointOffset),
			slog.Uint64("stream_next_offset", nextStreamOffset),
		)
	}

	return nil
}

// PostponeDelivery reschedules consumption of the stream after delay,
// recording failures as the checkpoint's new failure count.
func (p *EventPump) PostponeDelivery(
	ctx context.Context,
	dc *messagepump.DeliveryContext,
	failures uint64,
	delay time.Duration,
) error {
	if err := xsql.ExecOne(
		ctx,
		dc.Tx,
		`UPDATE eventstream.handler_checkpoints SET
			failures = $3,
			resume_at = clock_timestamp() + $4
		WHERE handler_key = $1
			AND stream_id = $2`,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(dc.Stream.ID),
		failures,
		delay,
	); err != nil {
		return fmt.Errorf("unable to postpone stream consumption: %w", err)
	}

	return nil
}
