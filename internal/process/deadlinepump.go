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
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// DeadlinePump is a [messagepump.Driver] that delivers pending deadlines to a
// process message handler.
type DeadlinePump struct {
	DB                   *sql.DB
	Handler              dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity             *identitypb.Identity
	Packer               *envelopepb.Packer
	DeadlineTypeIDs      *uuidpb.Set
	OutboundMessageTypes map[reflect.Type]struct{}
}

// AcquireDelivery attempts to acquire the next pending deadline for the
// handler.
func (p *DeadlinePump) AcquireDelivery(
	ctx context.Context,
	tx *sql.Tx,
) (messagepump.Delivery, bool, error) {
	// The JOIN against process.instances is not used to read any data, but is
	// required to lock the instance row in the same statement that locks the
	// deadline row.
	//
	// Without this, the deadline pump would acquire the deadline lock here and
	// later acquire the instance lock inside loadInstance(), inverting the lock
	// ordering used by the event pump, which acquires the instance lock first
	// and then deletes deadlines in endInstance(). That inversion deadlocks
	// when both pumps target the same instance concurrently.
	//
	// The "now" value used to gate the deadline's readiness is sourced from
	// Go's clock - not the database's - so that it agrees with the value
	// returned by [dogma.ProcessDeadlineScope.Now] when the handler runs. This
	// guarantees the deadline scope's notion of "now" is never earlier than its
	// scheduled time, without depending on clock synchronisation between the
	// engine and the database.
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
			AND d.deliver_at <= $2
			AND d.message_type_id = ANY($3)
		ORDER BY d.deliver_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		xsql.UUID(p.Identity.GetKey()),
		time.Now(),
		xsql.UUIDSeq(p.DeadlineTypeIDs.All()),
	)

	delivery := messagepump.Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
	}

	if err := row.Scan(
		xsql.UUID(delivery.MessageID),
		xsql.UUID(delivery.MessageTypeID),
		&delivery.EnvelopeBytes,
		&delivery.Failures,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return messagepump.Delivery{}, false, nil
		}

		return messagepump.Delivery{}, false, fmt.Errorf("unable to scan pending deadline: %w", err)
	}

	return delivery, true, nil
}

// PostponeDelivery reschedules the deadline for redelivery after delay.
func (*DeadlinePump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.deadlines SET
			failures = $2,
			deliver_at = clock_timestamp() + $3
		WHERE message_id = $1`,
		xsql.UUID(delivery.MessageID),
		delivery.Failures,
		delay,
	); err != nil {
		return fmt.Errorf("unable to postpone deadline: %w", err)
	}

	return nil
}

// HandleDelivery dispatches a deadline to the process handler.
func (p *DeadlinePump) HandleDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	envelope *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var deadline dogma.Deadline

	if err := xmessage.UnpackMessage(
		envelope,
		&deadline,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal deadline",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	instanceID := envelope.GetHeader().GetSource().GetInstanceId()

	instanceLogger := logger.With(
		slog.Group("process_instance", slog.String("id", instanceID)),
	)

	root, err := newRoot(ctx, p.Handler, instanceLogger)
	if err != nil {
		return err
	}

	ok, err := loadInstance(
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
		// This _should_ be unreachable code because deadlines are deleted when
		// process instances are ended.
		instanceLogger.ErrorContext(ctx, "process instance has ended")
		return messagepump.ErrFailed
	}

	instanceLogger = logger.With(
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
			slog.String("description", root.ProcessInstanceDescription(false)),
		),
	)

	scope := &deadlineScope{
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
		envelope.GetBody().GetScheduledFor().AsTime(),
	}

	if err := xerrors.Recover(
		func() error {
			return p.Handler.HandleDeadline(ctx, root, scope, deadline)
		},
	); err != nil {
		instanceLogger.ErrorContext(
			ctx,
			"unable to handle deadline",
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

		return nil
	}

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

	return p.deleteDeadline(ctx, tx, delivery.MessageID)
}

func (*DeadlinePump) deleteDeadline(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`DELETE FROM process.deadlines
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to delete deadline: %w", err)
	}

	return nil
}
