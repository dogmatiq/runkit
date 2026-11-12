package process

import (
	"context"
	"database/sql"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// DeadlineSink is a [messagequeue.Sink] that delivers deadlines to a
// [dogma.ProcessMessageHandler].
type DeadlineSink struct {
	// Handler is the process message handler to which deadlines are
	// delivered.
	Handler dogma.ProcessMessageHandler[dogma.ProcessRoot]

	// Identity is the identity of the process handler.
	Identity *identitypb.Identity

	// Packer packs commands and deadlines produced by the handler.
	Packer *envelope.Packer

	// QueueRouter chooses the queues for commands and deadlines produced by the
	// handler.
	QueueRouter *messagequeue.Router

	// OutboundMessageTypes contains the types the handler is allowed to produce.
	OutboundMessageTypes map[reflect.Type]struct{}

	// HandleTimeout is the maximum time the handler is given to handle each
	// deadline.
	HandleTimeout time.Duration
}

// Deliver delivers the deadline in the given envelope to the handler.
//
// It returns [messagequeue.ErrBusy] without calling the handler if another
// transaction holds the lock on the deadline's process instance. It returns nil
// without calling the handler if the instance has ended, because ending an
// instance cancels its deadlines.
func (s *DeadlineSink) Deliver(
	ctx context.Context,
	tx *sql.Tx,
	d messagequeue.Delivery,
	env *envelopepb.Envelope,
	engineLogger *slog.Logger,
	appLogger *slog.Logger,
) error {

	instanceID := env.GetHeader().GetSource().GetInstanceId()

	instanceLogger := engineLogger.With(
		slog.Group("process_instance", slog.String("id", instanceID)),
	)

	inst, exists, err := tryLockInstance(
		ctx,
		tx,
		s.Handler,
		s.QueueRouter,
		s.Identity.GetKey(),
		instanceID,
		instanceLogger,
	)
	if err != nil {
		return err
	}

	// The event sink stores the instance's row in the same transaction as any
	// deadlines that the handler schedules for it, and nothing deletes the row,
	// so it always exists.
	if !exists {
		instanceLogger.ErrorContext(ctx, "process instance not found")
		return messagequeue.ErrFailed
	}

	// The instance has ended, so the deadline is canceled. Ending the instance
	// didn't remove this deadline, because an attempt had claimed it, so the
	// consumer removes it when Deliver returns nil.
	if inst.Ended {
		instanceLogger.DebugContext(
			ctx,
			"deadline canceled because the process instance has ended",
		)
		return nil
	}

	var deadline dogma.Deadline

	if err := xmessage.UnpackMessage(
		env,
		&deadline,
	); err != nil {
		engineLogger.ErrorContext(
			ctx,
			"unable to unmarshal deadline",
			xslog.Error(err),
		)
		return messagequeue.ErrFailed
	}

	scope := &deadlineScope{
		messageScope{
			instanceID: inst.ID,
			root:       inst.Root,
			commandPacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			deadlinePacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			engineLogger:         engineLogger,
			appLogger:            appLogger,
			outboundMessageTypes: s.OutboundMessageTypes,
		},
		env.GetBody().GetScheduledFor().AsTime(),
	}

	if err := xerrors.Recover(
		func() error {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleDeadline(ctx, inst.Root, scope, deadline)
		},
	); err != nil {
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle deadline",
			xslog.Error(err),
		)

		return messagequeue.ErrFailed
	}

	if err := addCommandsToQueues(ctx, tx, s.QueueRouter, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		return inst.End(ctx, tx)
	}

	if scope.mutated {
		if err := inst.Save(ctx, tx, instanceLogger); err != nil {
			return err
		}
	}

	return inst.AddDeadlinesToQueue(ctx, tx, scope.deadlinePacker)
}

// LogAttr returns the attribute that identifies the handler in log records.
func (s *DeadlineSink) LogAttr() slog.Attr {
	return xslog.HandlerIdentity(s.Identity, config.ProcessHandlerType)
}
