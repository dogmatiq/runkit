package process

import (
	"context"
	"database/sql"
	"fmt"
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
	// Packer packs commands and deadlines produced by the handler, and
	// OutboundMessageTypes contains the types that the handler is allowed to
	// produce.
	Packer               *envelope.Packer
	OutboundMessageTypes map[reflect.Type]struct{}

	// QueueRouter chooses the queues for commands and deadlines produced by the
	// handler.
	QueueRouter *messagequeue.Router

	// Handler is the process message handler to which deadlines are delivered, and
	// Identity is its identity.
	Handler  dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity *identitypb.Identity

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
		return fmt.Errorf("unable to load process instance %q: %w", instanceID, err)
	}

	// The event sink stores the instance's row in the same transaction as any
	// deadlines that the handler schedules for it, and nothing deletes the row,
	// so it always exists.
	if !exists {
		return xerrors.Deterministic(
			fmt.Errorf("process instance %q not found", instanceID),
		)
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
		return fmt.Errorf("unable to unmarshal deadline: %w", err)
	}

	scope := &deadlineScope{
		messageScope{
			CommandPacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			DeadlinePacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(inst.ID),
			),
			instanceID:           inst.ID,
			root:                 inst.Root,
			outboundMessageTypes: s.OutboundMessageTypes,
			engineLogger:         engineLogger,
			appLogger:            appLogger,
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
		return fmt.Errorf(
			"unable to handle deadline for process instance %q: %w",
			inst.ID,
			err,
		)
	}

	return writeEffects(
		ctx,
		tx,
		s.QueueRouter,
		inst,
		&scope.messageScope,
	)
}

// LogAttr returns the attribute that identifies the handler in log records.
func (s *DeadlineSink) LogAttr() slog.Attr {
	return xslog.HandlerIdentity(s.Identity, config.ProcessHandlerType)
}
