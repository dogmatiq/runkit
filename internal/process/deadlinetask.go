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
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

type deadlineTask struct {
	Tx                      *sql.Tx
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	MessageID               *uuidpb.UUID
	InstanceID              string
	InstanceStateBytes      []byte
	EnvelopeBytes           []byte
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Execute processes the task by handling its deadline and committing the
// transaction.
func (t *deadlineTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	err := t.handleDeadline(ctx)

	if errors.Is(err, errFailed) {
		err = t.failAndPostpone(ctx)
	}

	if err != nil {
		return err
	}

	if err := t.Tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// handleDeadline unpacks the deadline envelope and dispatches it to the
// handler.
func (t *deadlineTask) handleDeadline(ctx context.Context) error {
	var (
		deadlineEnvelope = &envelopepb.Envelope{}
		deadline         dogma.Deadline
	)

	if err := xmessage.Unpack(
		t.EnvelopeBytes,
		deadlineEnvelope,
		&deadline,
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to unmarshal deadline",
			xslog.Error(err),
		)
		return errFailed
	}

	t.Logger = t.Logger.With(
		xslog.Envelope("deadline", deadlineEnvelope),
	)

	root := t.Handler.New()

	if t.InstanceStateBytes != nil {
		if err := xerrors.ConvertPanicToError(
			func() error {
				return root.UnmarshalBinary(t.InstanceStateBytes)
			},
		); err != nil {
			t.Logger.ErrorContext(
				ctx,
				"unable to unmarshal process instance state",
				xslog.Error(err),
			)
			return errFailed
		}
	}

	scope := &deadlineScope{
		messageScope{
			instanceID: t.InstanceID,
			root:       root,
			commandPacker: t.Packer.PackEffects(
				deadlineEnvelope,
				t.Identity,
				envelopepb.WithInstanceID(t.InstanceID),
			),
			deadlinePacker: t.Packer.PackEffects(
				deadlineEnvelope,
				t.Identity,
				envelopepb.WithInstanceID(t.InstanceID),
			),
			logger: t.Logger,
		},
		deadlineEnvelope.GetBody().GetScheduledFor().AsTime(),
	}

	if err := xerrors.ConvertPanicToError(
		func() error {
			return t.Handler.HandleDeadline(
				ctx,
				root,
				scope,
				deadline,
			)
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to handle deadline",
			xslog.Error(err),
		)
		return errFailed
	}

	if err := addCommandsToQueue(ctx, t.Tx, scope.commandPacker); err != nil {
		return err
	}

	if scope.ended {
		if err := t.endInstance(ctx); err != nil {
			return err
		}
		return nil
	}

	if scope.mutated {
		if err := saveInstance(
			ctx,
			t.Tx,
			t.Identity.GetKey(),
			t.InstanceID,
			root,
			t.Logger,
		); err != nil {
			return err
		}
	}

	if err := persistDeadlines(ctx, t.Tx, scope.deadlinePacker); err != nil {
		return err
	}

	return t.deleteDeadline(ctx)
}

// endInstance marks the process instance as ended and clears its state.
func (t *deadlineTask) endInstance(ctx context.Context) error {
	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE process.instances SET
			ended = true,
			state = NULL
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(t.Identity.GetKey()),
		t.InstanceID,
	); err != nil {
		return fmt.Errorf("unable to end process instance: %w", err)
	}

	if _, err := t.Tx.ExecContext(
		ctx,
		`DELETE FROM process.deadlines
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(t.Identity.GetKey()),
		t.InstanceID,
	); err != nil {
		return fmt.Errorf("unable to delete deadlines for ended instance: %w", err)
	}

	return nil
}

// deleteDeadline removes the processed deadline from the queue.
func (t *deadlineTask) deleteDeadline(ctx context.Context) error {
	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`DELETE FROM process.deadlines
		WHERE message_id = $1`,
		xsql.UUID(t.MessageID),
	); err != nil {
		return fmt.Errorf("unable to delete deadline: %w", err)
	}

	return nil
}

// failAndPostpone increments the failure counter and postpones the deadline
// so that processing is retried after an exponential backoff period.
func (t *deadlineTask) failAndPostpone(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`UPDATE process.deadlines SET
			failures = failures + 1,
			deliver_at = clock_timestamp() + LEAST($2 * (2 ^ failures), $3)
		WHERE message_id = $1`,
		xsql.UUID(t.MessageID),
		t.BackoffBase,
		t.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone deadline after failure: %w", err)
	}

	return nil
}
