package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/messagepump"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// EventPump is a [messagepump.Driver] that delivers pending events to a
// process message handler.
type EventPump struct {
	DB                   *sql.DB
	Handler              dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity             *identitypb.Identity
	Packer               *envelopepb.Packer
	EventTypeIDs         []string
	OutboundMessageTypes map[reflect.Type]struct{}
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
			s.id,
			s.next_offset,
			COALESCE(a.checkpoint_offset, 0),
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
			AND e.stream_offset >= COALESCE(a.checkpoint_offset, 0)
			AND e.message_type_id = ANY($2)
		ORDER BY e.stream_offset
		LIMIT 1`,
		xsql.UUID(p.Identity.GetKey()),
		p.EventTypeIDs,
	)

	var nextStreamOffset uint64

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
		&del.Stream.CheckpointOffset,
		&del.Failures,
		&del.Stream.EventOffset,
		xsql.UUID(del.MessageID),
		xsql.UUID(del.MessageTypeID),
		&del.EnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return messagepump.Delivery{}, false, nil
		}

		return messagepump.Delivery{}, false, fmt.Errorf("unable to acquire pending event: %w", err)
	}

	// If we found a pending event, return a delivery for it.
	if del.Stream.EventOffset < nextStreamOffset {
		return del, true, nil
	}

	// Otherwise, no matching events are available on this stream. Advance the
	// checkpoint offset to the end of the stream so that the stream is not
	// re-acquired until new events arrive.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3`,
		nextStreamOffset,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(del.Stream.ID),
	); err != nil {
		return messagepump.Delivery{}, false, fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return messagepump.Delivery{}, false, nil
}

// HandleDelivery dispatches an event to the process handler.
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
		xslog.Envelope("event", eventEnvelope),
	)

	instanceID, ok, err := p.routeEventToInstance(ctx, logger, event)
	if err != nil {
		return err
	}

	if !ok {
		return p.advanceCheckpoint(ctx, dc)
	}

	logger = logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
	)

	root := p.Handler.New()

	ok, err = loadInstance(
		ctx,
		dc.Tx,
		p.Identity.GetKey(),
		instanceID,
		root,
		logger,
	)
	if err != nil {
		return err
	}

	if !ok {
		return p.advanceCheckpoint(ctx, dc)
	}

	scope := &eventScope{
		messageScope{
			instanceID: instanceID,
			root:       root,
			commandPacker: p.Packer.PackEffects(
				eventEnvelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			deadlinePacker: p.Packer.PackEffects(
				eventEnvelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			logger:               logger,
			outboundMessageTypes: p.OutboundMessageTypes,
		},
		eventEnvelope.GetBody().GetCreatedAt().AsTime(),
	}

	if err := xerrors.ConvertPanicToError(
		func() error {
			return p.Handler.HandleEvent(
				ctx,
				root,
				scope,
				event,
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

	if err := addCommandsToQueue(ctx, dc.Tx, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		if err := endInstance(
			ctx,
			dc.Tx,
			p.Identity.GetKey(),
			instanceID,
		); err != nil {
			return err
		}
	} else {
		if scope.mutated {
			if err := saveInstance(
				ctx,
				dc.Tx,
				p.Identity.GetKey(),
				instanceID,
				root,
				logger,
			); err != nil {
				return err
			}
		}

		if err := persistDeadlines(ctx, dc.Tx, scope.deadlinePacker); err != nil {
			return err
		}
	}

	return p.advanceCheckpoint(ctx, dc)
}

// routeEventToInstance routes the event to a process instance by calling the
// handler's RouteEventToInstance() method.
func (p *EventPump) routeEventToInstance(
	ctx context.Context,
	logger *slog.Logger,
	event dogma.Event,
) (instanceID string, ok bool, err error) {
	if err := xerrors.ConvertPanicToError(
		func() error {
			instanceID, ok, err = p.Handler.RouteEventToInstance(ctx, event)
			if err != nil {
				return err
			}

			if ok && instanceID == "" {
				return fmt.Errorf("handler returned empty instance ID")
			}

			return nil
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to route event to instance",
			xslog.Error(err),
		)

		return "", false, messagepump.ErrFailed
	}

	return instanceID, ok, nil
}

// advanceCheckpoint updates the handler's checkpoint offset for this stream.
func (p *EventPump) advanceCheckpoint(ctx context.Context, dc *messagepump.DeliveryContext) error {
	if err := xsql.ExecOne(
		ctx,
		dc.Tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
		AND stream_id = $3`,
		dc.Stream.EventOffset+1,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(dc.Stream.ID),
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
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
