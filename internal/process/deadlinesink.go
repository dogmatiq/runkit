package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// DeadlineSink delivers pending deadlines to a process message handler.
// A delivery commits its effects and removal from the queue atomically.
type DeadlineSink struct {
	// DB stores pending deadlines and process state.
	DB *sql.DB

	// Handler is the process message handler that receives deadlines.
	Handler dogma.ProcessMessageHandler[dogma.ProcessRoot]

	// Identity is the identity of the process handler.
	Identity *identitypb.Identity

	// Packer packs commands and deadlines produced by the handler.
	Packer *envelope.Packer

	// DeadlineTypeIDs contains the deadline types routed to the handler.
	DeadlineTypeIDs *uuidcollection.Set

	// OutboundMessageTypes contains the types the handler may produce.
	OutboundMessageTypes map[reflect.Type]struct{}

	// HandleTimeout bounds each application handler call.
	HandleTimeout time.Duration

	// Concurrency limits simultaneous deliveries for this handler on this node.
	Concurrency int

	// BackoffBase and BackoffCap bound the retry delay after application failures.
	BackoffBase, BackoffCap time.Duration

	// Logger receives messages from the sink and application handler.
	Logger *slog.Logger
}

// Deliver attempts to deliver one due deadline for instanceID. A handler failure
// rolls back its effects before the deadline is postponed in another transaction.
// It returns nil if no deadline is due or an application failure is postponed;
// infrastructure errors are returned for the scheduler to retry.
func (s *DeadlineSink) Deliver(ctx context.Context, instanceID string) error {
	var (
		id       uuidpb.UUID
		failures uint64
		acquired bool
	)

	err := xsql.Transact(ctx, s.DB, func(ctx context.Context, tx *sql.Tx) error {
		if ok, err := s.lockInstance(ctx, tx, instanceID); err != nil || !ok {
			return err
		}

		var data []byte
		err := tx.QueryRowContext(
			ctx,
			`SELECT
				message_id,
				envelope,
				failures
			FROM process.deadlines
			WHERE handler_key = $1
			AND instance_id = $2
			AND deliver_at <= $3
			AND message_type_id = ANY($4)
			ORDER BY deliver_at, message_id
			LIMIT 1
			FOR UPDATE`,
			xsql.UUID(s.Identity.GetKey()),
			instanceID,
			time.Now(),
			xsql.UUIDSeq(s.DeadlineTypeIDs.All()),
		).Scan(xsql.UUID(&id), &data, &failures)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		acquired = true

		logger := s.Logger.With(
			slog.Group("delivery",
				xslog.UUID("id", &id),
				slog.Uint64("attempt", failures+1),
			),
		)
		env := &envelopepb.Envelope{}
		if err := env.UnmarshalBinary(data); err != nil {
			return fmt.Errorf("%w: unable to unmarshal deadline envelope: %w", errApplication, err)
		}
		if err := env.Validate(); err != nil {
			return fmt.Errorf("%w: invalid deadline envelope: %w", errApplication, err)
		}
		return s.dispatch(ctx, tx, &id, env, logger)
	})
	if err == nil || !acquired || !errors.Is(err, errApplication) {
		return err
	}

	s.Logger.ErrorContext(ctx, "unable to deliver deadline", xslog.Error(err))
	delay := s.backoff(failures + 1)
	return xsql.Transact(ctx, s.DB, func(ctx context.Context, tx *sql.Tx) error {
		if ok, err := s.lockInstance(ctx, tx, instanceID); err != nil || !ok {
			return err
		}

		// Another owner may have handled or postponed this deadline after the
		// failed attempt rolled back. Do not overwrite its progress.
		_, err := tx.ExecContext(
			ctx,
			`UPDATE process.deadlines SET
				failures = failures + 1,
				deliver_at = $4
			WHERE message_id = $1
			AND handler_key = $2
			AND failures = $3`,
			xsql.UUID(&id),
			xsql.UUID(s.Identity.GetKey()),
			failures,
			time.Now().Add(delay),
		)
		return err
	})
}

// lockInstance keeps instance locking ahead of deadline locking, as it is when
// an event ends an instance and deletes its pending deadlines.
func (s *DeadlineSink) lockInstance(
	ctx context.Context,
	tx *sql.Tx,
	instanceID string,
) (bool, error) {
	var id string
	err := tx.QueryRowContext(
		ctx,
		`SELECT instance_id
		FROM process.instances
		WHERE handler_key = $1
		AND instance_id = $2
		AND NOT ended
		FOR UPDATE`,
		xsql.UUID(s.Identity.GetKey()),
		instanceID,
	).Scan(&id)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// backoff preserves the capped, equal-jitter retry schedule used by deadline
// delivery before it moved out of the message pump.
func (s *DeadlineSink) backoff(n uint64) time.Duration {
	if s.BackoffBase <= 0 || s.BackoffCap <= s.BackoffBase {
		return s.BackoffBase
	}
	limit := s.BackoffBase << n
	if limit <= 0 || limit > s.BackoffCap {
		limit = s.BackoffCap
	}
	return s.BackoffBase + rand.N(limit-s.BackoffBase)
}

// dispatch delivers a deadline and applies its effects within tx.
func (s *DeadlineSink) dispatch(
	ctx context.Context,
	tx *sql.Tx,
	deadlineID *uuidpb.UUID,
	env *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var deadline dogma.Deadline

	if err := xmessage.UnpackMessage(
		env,
		&deadline,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal deadline",
			xslog.Error(err),
		)
		return fmt.Errorf("%w: %w", errApplication, err)
	}

	instanceID := env.GetHeader().GetSource().GetInstanceId()

	instanceLogger := logger.With(
		slog.Group("process_instance", slog.String("id", instanceID)),
	)

	root, err := newRoot(ctx, s.Handler, instanceLogger)
	if err != nil {
		return err
	}

	ok, err := loadInstance(
		ctx,
		tx,
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
		return fmt.Errorf("%w: process instance has ended", errApplication)
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
			logger:               instanceLogger,
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

		return fmt.Errorf("%w: %w", errApplication, err)
	}

	if err := addCommandsToQueue(ctx, tx, scope.commandPacker); err != nil {
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

	if err := persistDeadlines(ctx, tx, scope.deadlinePacker); err != nil {
		return err
	}

	return s.deleteDeadline(ctx, tx, deadlineID)
}

func (*DeadlineSink) deleteDeadline(
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
