package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// DeliverDeadline dispatches at most one due deadline for the given process
// instance.
//
// It returns nil if the instance has ended or has no due deadlines.
//
// If application code fails, its effects are rolled back and the deadline is
// postponed in a separate transaction. DeliverDeadline returns nil if the
// deadline is postponed successfully. Database errors are returned to the caller.
func (s *Sink) DeliverDeadline(ctx context.Context, instanceID string) error {
	var (
		id       uuidpb.UUID
		failures uint64
		acquired bool
	)

	err := xsql.Transact(
		ctx,
		s.DB,
		func(ctx context.Context, tx *sql.Tx) error {
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
				xsql.UUIDSeq(s.DeadlineTypes.All()),
			).Scan(xsql.UUID(&id), &data, &failures)
			if errors.Is(err, sql.ErrNoRows) {
				return nil
			}

			if err != nil {
				return err
			}

			acquired = true

			logger := s.Logger.With(
				slog.Group(
					"delivery",
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

			return s.dispatchDeadline(ctx, tx, &id, env, logger)
		},
	)
	if err == nil || !acquired || !errors.Is(err, errApplication) {
		return err
	}

	s.Logger.ErrorContext(
		ctx,
		"unable to deliver deadline",
		xslog.Error(err),
	)
	delay := s.deadlineBackoff(failures + 1)

	return xsql.Transact(
		ctx,
		s.DB,
		func(ctx context.Context, tx *sql.Tx) error {
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
		},
	)
}

// lockInstance locks the process instance with the given ID until tx completes.
//
// It returns false if the instance does not exist or has ended.
//
// The instance must be locked before its deadlines, matching the lock order
// used when handling an event that ends an instance.
func (s *Sink) lockInstance(
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

// deadlineBackoff returns the delay before retrying a deadline that has failed
// n times.
//
// The delay increases exponentially, with jitter, up to BackoffCap.
func (s *Sink) deadlineBackoff(n uint64) time.Duration {
	if s.BackoffBase <= 0 || s.BackoffCap <= s.BackoffBase {
		return s.BackoffBase
	}

	limit := s.BackoffBase << n

	if limit <= 0 || limit > s.BackoffCap {
		limit = s.BackoffCap
	}

	return s.BackoffBase + rand.N(limit-s.BackoffBase)
}

// dispatchDeadline delivers a deadline and applies its effects within tx.
func (s *Sink) dispatchDeadline(
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
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
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

	if err := s.applyEffects(ctx, tx, &scope.messageScope); err != nil {
		return err
	}

	if scope.ended {
		// Ending an instance removes all of its pending deadlines, including
		// the one being delivered.
		return nil
	}

	return s.deleteDeadline(ctx, tx, deadlineID)
}

// deleteDeadline removes the deadline with the given message ID from the queue.
func (*Sink) deleteDeadline(
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
