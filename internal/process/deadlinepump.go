package process

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
	"github.com/dogmatiq/reference-engine/internal/messagepump"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// DeadlinePump is an engine component that periodically attempts to acquire
// pending deadlines for dispatch to a process message handler.
type DeadlinePump struct {
	DB                      *sql.DB
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Run runs the deadline pump until ctx is canceled.
func (p *DeadlinePump) Run(ctx context.Context) {
	messagepump.Run(
		ctx,
		p.DB,
		p.Logger,
		p.acquireNextDeadline,
		p.handleDelivery,
	)
}

func (p *DeadlinePump) acquireNextDeadline(ctx context.Context, tx *sql.Tx) (messagepump.Delivery, bool, error) {
	// The JOIN against process.instances is not used to read any data, but is
	// required to lock the instance row in the same statement that locks the
	// deadline row.
	//
	// Without this, the deadline pump would acquire the deadline lock here and
	// later acquire the instance lock inside loadInstance(), inverting the lock
	// ordering used by the event pump, which acquires the instance lock first
	// and then deletes deadlines in endInstance(). That inversion deadlocks
	// when both pumps target the same instance concurrently.
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			d.message_id,
			d.message_type_id,
			d.envelope,
			d.failures
		FROM process.deadlines AS d
		INNER JOIN process.instances AS i
			ON i.handler_key = d.handler_key
			AND i.instance_id = d.instance_id
		WHERE d.handler_key = $1
			AND d.deliver_at <= clock_timestamp()
		ORDER BY d.deliver_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		xsql.UUID(p.Identity.GetKey()),
	)

	del := messagepump.Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
	}

	if err := row.Scan(
		xsql.UUID(del.MessageID),
		xsql.UUID(del.MessageTypeID),
		&del.EnvelopeBytes,
		&del.FailureCount,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return messagepump.Delivery{}, false, nil
		}

		return messagepump.Delivery{}, false, fmt.Errorf("unable to scan pending deadline: %w", err)
	}

	return del, true, nil
}

func (p *DeadlinePump) handleDelivery(ctx context.Context, dc *messagepump.DeliveryContext) error {
	err := p.handleDeadline(ctx, dc)

	if errors.Is(err, errFailed) {
		err = p.failAndPostpone(ctx, dc)
	}

	return err
}

func (p *DeadlinePump) handleDeadline(ctx context.Context, dc *messagepump.DeliveryContext) error {
	var (
		deadlineEnvelope = &envelopepb.Envelope{}
		deadline         dogma.Deadline
	)

	if err := xmessage.Unpack(
		dc.EnvelopeBytes,
		deadlineEnvelope,
		&deadline,
	); err != nil {
		dc.Logger.ErrorContext(
			ctx,
			"unable to unmarshal deadline",
			xslog.Error(err),
		)
		return errFailed
	}

	logger := dc.Logger.With(
		xslog.Envelope("deadline", deadlineEnvelope),
	)

	instanceID := deadlineEnvelope.GetHeader().GetSource().GetInstanceId()
	root := p.Handler.New()

	ok, err := loadInstance(
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
		// This _should_ be unreachable code because deadlines are deleted when
		// process instances are ended.
		logger.ErrorContext(ctx, "process instance has ended")
		return errFailed
	}

	scope := &deadlineScope{
		messageScope{
			instanceID: instanceID,
			root:       root,
			commandPacker: p.Packer.PackEffects(
				deadlineEnvelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			deadlinePacker: p.Packer.PackEffects(
				deadlineEnvelope,
				p.Identity,
				envelopepb.WithInstanceID(instanceID),
			),
			logger: logger,
		},
		deadlineEnvelope.GetBody().GetScheduledFor().AsTime(),
	}

	if err := xerrors.ConvertPanicToError(
		func() error {
			return p.Handler.HandleDeadline(
				ctx,
				root,
				scope,
				deadline,
			)
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to handle deadline",
			xslog.Error(err),
		)

		return errFailed
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

		return nil
	}

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

	return p.deleteDeadline(ctx, dc)
}

func (*DeadlinePump) deleteDeadline(ctx context.Context, dc *messagepump.DeliveryContext) error {
	if err := xsql.ExecOne(
		ctx,
		dc.Tx,
		`DELETE FROM process.deadlines
		WHERE message_id = $1`,
		xsql.UUID(dc.MessageID),
	); err != nil {
		return fmt.Errorf("unable to delete deadline: %w", err)
	}

	return nil
}

// failAndPostpone increments the failure counter and postpones the deadline so
// that processing is retried after an exponential backoff period.
func (p *DeadlinePump) failAndPostpone(ctx context.Context, dc *messagepump.DeliveryContext) error {
	if _, err := dc.Tx.ExecContext(
		ctx,
		`UPDATE process.deadlines SET
			failures = failures + 1,
			deliver_at = clock_timestamp() + LEAST($2 * (2 ^ failures), $3)
		WHERE message_id = $1`,
		xsql.UUID(dc.MessageID),
		p.BackoffBase,
		p.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone deadline after failure: %w", err)
	}

	return nil
}
