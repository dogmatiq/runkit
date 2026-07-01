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
func (p *EventPump) AcquireDelivery(
	ctx context.Context,
	tx *sql.Tx,
) (messagepump.Delivery, bool, error) {
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
		&delivery.Stream.CheckpointOffset,
		&delivery.Failures,
		&delivery.Stream.EventOffset,
		xsql.UUID(delivery.MessageID),
		xsql.UUID(delivery.MessageTypeID),
		&delivery.EnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return messagepump.Delivery{}, false, nil
		}

		return messagepump.Delivery{}, false, fmt.Errorf("unable to acquire pending event: %w", err)
	}

	// If we found a pending event, return a delivery for it.
	if delivery.Stream.EventOffset < nextStreamOffset {
		return delivery, true, nil
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
		xsql.UUID(delivery.Stream.ID),
	); err != nil {
		return messagepump.Delivery{}, false, fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return messagepump.Delivery{}, false, nil
}

// HandleDelivery dispatches an event to the process handler.
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

	instanceID, ok, err := p.routeEventToInstance(ctx, logger, event)
	if err != nil {
		return err
	}

	if !ok {
		return p.advanceCheckpoint(ctx, tx, delivery)
	}

	instanceLogger := logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
	)

	root, err := newRoot(ctx, p.Handler, instanceLogger)
	if err != nil {
		return err
	}

	ok, err = loadInstance(
		ctx,
		tx,
		p.Identity.GetKey(),
		instanceID,
		root,
		instanceLogger,
	)
	if err != nil {
		return err
	}

	if !ok {
		return p.advanceCheckpoint(ctx, tx, delivery)
	}

	instanceLogger = logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
			slog.String("description", root.ProcessInstanceDescription(false)),
		),
	)

	scope := &eventScope{
		messageScope{
			instanceID: instanceID,
			root:       root,
			commandPacker: p.Packer.PackEffects(
				envelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			deadlinePacker: p.Packer.PackEffects(
				envelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			logger:               instanceLogger,
			outboundMessageTypes: p.OutboundMessageTypes,
		},
		envelope.GetBody().GetCreatedAt().AsTime(),
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
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	if err := addCommandsToQueue(ctx, tx, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		if err := endInstance(
			ctx,
			tx,
			p.Identity.GetKey(),
			instanceID,
		); err != nil {
			return err
		}
	} else {
		if scope.mutated {
			if err := saveInstance(
				ctx,
				tx,
				p.Identity.GetKey(),
				instanceID,
				root,
				instanceLogger,
			); err != nil {
				return err
			}
		}

		if err := persistDeadlines(ctx, tx, scope.deadlinePacker); err != nil {
			return err
		}
	}

	return p.advanceCheckpoint(ctx, tx, delivery)
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
func (p *EventPump) advanceCheckpoint(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
		AND stream_id = $3`,
		delivery.Stream.EventOffset+1,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(delivery.Stream.ID),
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	return nil
}

// PostponeDelivery reschedules consumption of the stream after delay.
func (p *EventPump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			failures = $3,
			resume_at = clock_timestamp() + $4
		WHERE handler_key = $1
			AND stream_id = $2`,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(delivery.Stream.ID),
		delivery.Failures,
		delay,
	); err != nil {
		return fmt.Errorf("unable to postpone stream consumption: %w", err)
	}

	return nil
}
