package process

import (
	"context"
	"database/sql"
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
)

// EventPump is a [messagepump.Driver] that delivers pending events to a
// process message handler.
type EventPump struct {
	DB                   *sql.DB
	Handler              dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity             *identitypb.Identity
	Packer               *envelopepb.Packer
	EventTypeIDs         *uuidpb.Set
	OutboundMessageTypes map[reflect.Type]struct{}
	Logger               *slog.Logger
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
	return messagepump.AcquireEventDelivery(
		ctx,
		tx,
		p.Logger,
		p.Identity.GetKey(),
		p.EventTypeIDs,
	)
}

// PostponeDelivery reschedules consumption of the stream after delay.
func (p *EventPump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	return messagepump.PostponeEventDelivery(
		ctx,
		tx,
		p.Identity.GetKey(),
		delivery,
		delay,
	)
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
		return messagepump.AdvanceStreamCheckpoint(
			ctx,
			tx,
			logger,
			p.Identity.GetKey(),
			delivery.Stream.ID,
			delivery.Stream.CheckpointOffset,
			delivery.Stream.EventOffset+1,
		)
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
		return messagepump.AdvanceStreamCheckpoint(
			ctx,
			tx,
			logger,
			p.Identity.GetKey(),
			delivery.Stream.ID,
			delivery.Stream.CheckpointOffset,
			delivery.Stream.EventOffset+1,
		)
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

	if err := xerrors.Recover(
		func() error {
			return p.Handler.HandleEvent(ctx, root, scope, event)
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

	return messagepump.AdvanceStreamCheckpoint(
		ctx,
		tx,
		logger,
		p.Identity.GetKey(),
		delivery.Stream.ID,
		delivery.Stream.CheckpointOffset,
		delivery.Stream.EventOffset+1,
	)
}

// routeEventToInstance routes the event to a process instance by calling the
// handler's RouteEventToInstance() method.
func (p *EventPump) routeEventToInstance(
	ctx context.Context,
	logger *slog.Logger,
	event dogma.Event,
) (string, bool, error) {
	instanceID, err := xerrors.RecoverT(
		func() (string, error) {
			instanceID, ok, err := p.Handler.RouteEventToInstance(ctx, event)
			if !ok || err != nil {
				return "", err
			}

			if instanceID == "" {
				return "", fmt.Errorf("handler returned empty instance ID")
			}

			return instanceID, nil
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to route event to instance",
			xslog.Error(err),
		)

		return "", false, messagepump.ErrFailed
	}

	return instanceID, instanceID != "", nil
}
