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

	scope := &messageScope{
		instanceID: t.InstanceID,
		root:       root,
		packer:     t.Packer.PackEffects(deadlineEnvelope, t.Identity, envelopepb.WithInstanceID(t.InstanceID)),
		time:       deadlineEnvelope.GetBody().GetScheduledFor().AsTime(),
		logger:     t.Logger,
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

	if scope.ended {
		if err := t.endInstance(ctx); err != nil {
			return err
		}
		return nil
	}

	if scope.mutated {
		if err := t.saveInstance(ctx, root); err != nil {
			return err
		}
	}

	if err := t.persistDeadlines(ctx, scope.packer); err != nil {
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

// persistDeadlines inserts any deadlines scheduled by the handler into the
// process.deadlines table.
func (t *deadlineTask) persistDeadlines(
	ctx context.Context,
	packer *envelopepb.EffectPacker,
) error {
	multi, ok := packer.Seal()
	if !ok {
		return nil
	}

	for env := range multi.All() {
		data, err := env.MarshalBinary()
		if err != nil {
			return fmt.Errorf("unable to marshal deadline envelope: %w", err)
		}

		if _, err := t.Tx.ExecContext(
			ctx,
			`INSERT INTO process.deadlines (
				message_id,
				handler_key,
				instance_id,
				envelope,
				deliver_at
			) VALUES ($1, $2, $3, $4, $5)`,
			xsql.UUID(env.GetBody().GetMessageId()),
			xsql.UUID(t.Identity.GetKey()),
			t.InstanceID,
			data,
			env.GetBody().GetScheduledFor().AsTime(),
		); err != nil {
			return fmt.Errorf("unable to persist deadline: %w", err)
		}
	}

	return nil
}

// saveInstance persists the process root state for the given instance.
func (t *deadlineTask) saveInstance(
	ctx context.Context,
	root dogma.ProcessRoot,
) error {
	var state []byte

	if err := xerrors.ConvertPanicToError(
		func() error {
			var err error
			state, err = root.MarshalBinary()
			return err
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to marshal process instance state",
			xslog.Error(err),
		)
		return errFailed
	}

	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE process.instances SET
			state = $1
		WHERE handler_key = $2
		AND instance_id = $3`,
		state,
		xsql.UUID(t.Identity.GetKey()),
		t.InstanceID,
	); err != nil {
		return fmt.Errorf("unable to save process instance: %w", err)
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
