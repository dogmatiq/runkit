package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

type commandTask struct {
	Tx                   *sql.Tx
	Handler              dogma.IntegrationMessageHandler
	Identity             *identitypb.Identity
	Concurrency          dogma.ConcurrencyPreference
	Packer               *envelopepb.Packer
	MessageID            *uuidpb.UUID
	EnvelopeBytes        []byte
	ParentLogger, Logger *slog.Logger
}

var errFailed = errors.New("unable to handle command")

// Execute processes the task by handling its command and committing the
// transaction.
func (t *commandTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	err := t.handleCommand(ctx)

	switch err {
	case nil:
		err = commandqueue.Remove(ctx, t.Tx, t.MessageID)
	case errFailed:
		err = commandqueue.DeferDueToFailure(ctx, t.Tx, t.MessageID)
	default:
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

// handleCommand processes the command in the given envelope.
func (t *commandTask) handleCommand(ctx context.Context) error {
	var (
		commandEnvelope    = &envelopepb.Envelope{}
		commandForHandling dogma.Command
	)

	if err := xmessage.Unpack(
		t.EnvelopeBytes,
		commandEnvelope,
		&commandForHandling,
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			slog.Group(
				"command",
				xslog.UUID("message_id", t.MessageID),
			),
			xslog.Error(err),
		)

		return errFailed
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	packer := t.Packer.PackEffects(
		commandEnvelope,
		t.Identity,
	)

	if t.Concurrency == dogma.MinimizeConcurrency {
		if err := t.acquireLock(ctx); err != nil {
			return err
		}
	}

	if err := xerrors.Recover(
		func() error {
			return t.Handler.HandleCommand(
				ctx,
				&scope{
					packer: packer,
					logger: t.Logger,
				},
				commandForHandling,
			)
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return errFailed
	}

	eventEnvelopes, ok := packer.Seal()
	if !ok {
		// No events were recorded.
		return nil
	}

	streamID, err := eventstream.Acquire(ctx, t.Tx)
	if err != nil {
		return err
	}

	_, err = eventstream.Append(
		ctx,
		t.Tx,
		streamID,
		eventEnvelopes,
	)

	return err
}

// acquireLock serializes command handling for the handler when it prefers
// minimized concurrency. It blocks until no other transaction holds the lock.
func (t *commandTask) acquireLock(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`INSERT INTO dogma.integration_handler_locks (
			handler_key
		)
		VALUES ($1)
		ON CONFLICT (handler_key) DO UPDATE SET
			handler_key = EXCLUDED.handler_key`,
		xsql.UUID(t.Identity.GetKey()),
	); err != nil {
		return fmt.Errorf("unable to acquire handler lock: %w", err)
	}

	return nil
}
