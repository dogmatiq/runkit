package process

import (
	"context"
	"database/sql"
	"log/slog"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
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

	// EngineLogger is the target for the engine's records about the sink and
	// the handler.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's records from the handler.
	AppLogger *slog.Logger
}

// Deliver delivers the deadline in the given envelope to the handler.
func (s *DeadlineSink) Deliver(
	ctx context.Context,
	tx *sql.Tx,
	d messagequeue.Delivery,
	env *envelopepb.Envelope,
) error {
	engineLogger := s.EngineLogger.With(
		xslog.Envelope("handled_message", env),
		slog.Uint64("failures", d.Failures),
	)

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

	instanceID := env.GetHeader().GetSource().GetInstanceId()

	instanceLogger := engineLogger.With(
		slog.Group("process_instance", slog.String("id", instanceID)),
	)

	root, err := newRoot(ctx, s.Handler, instanceLogger)
	if err != nil {
		return err
	}

	deadlineQueueID, ok, err := loadInstance(
		ctx,
		tx,
		s.QueueRouter,
		s.Identity.GetKey(),
		instanceID,
		root,
		instanceLogger,
	)
	if err != nil {
		return err
	}
	if !ok {
		// This _should_ be unreachable code because deadlines are deleted when
		// process instances are ended.
		instanceLogger.ErrorContext(ctx, "process instance has ended")
		return messagequeue.ErrFailed
	}

	scope := &deadlineScope{
		messageScope{
			instanceID: instanceID,
			root:       root,
			commandPacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(instanceID),
			),
			deadlinePacker: s.Packer.PackEffects(
				env,
				s.Identity,
				envelope.WithInstanceID(instanceID),
			),
			engineLogger: engineLogger,
			appLogger: s.AppLogger.With(
				xslog.Envelope("handled_message", env),
			),
			outboundMessageTypes: s.OutboundMessageTypes,
		},
		env.GetBody().GetScheduledFor().AsTime(),
	}

	if err := xerrors.Recover(
		func() error {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleDeadline(ctx, root, scope, deadline)
		},
	); err != nil {
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle deadline",
			xslog.Error(err),
		)

		return messagequeue.ErrFailed
	}

	if err := addCommandsToQueue(ctx, tx, s.QueueRouter, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		if err := endInstance(
			ctx,
			tx,
			s.Identity.GetKey(),
			instanceID,
		); err != nil {
			return err
		}

		return nil
	}

	if scope.mutated {
		if err := saveInstance(
			ctx,
			tx,
			s.Identity.GetKey(),
			instanceID,
			root,
			instanceLogger,
		); err != nil {
			return err
		}
	}

	return persistDeadlines(ctx, tx, deadlineQueueID, scope.deadlinePacker)
}
