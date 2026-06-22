package integration

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"
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

type commandTask struct {
	Tx                   *sql.Tx
	Handler              dogma.IntegrationMessageHandler
	Identity             *identitypb.Identity
	Concurrency          dogma.ConcurrencyPreference
	Packer               *envelopepb.Packer
	MessageID            *uuidpb.UUID
	BackoffBase          time.Duration
	BackoffCap           time.Duration
	EnvelopeBytes        []byte
	ParentLogger, Logger *slog.Logger
}

var errFailed = errors.New("unable to handle command")

// Execute processes the task by handling its command and committing the
// transaction.
func (t *commandTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	err := t.handleCommand(ctx)

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
				&messageScope{
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

	if eventEnvelopes, ok := packer.Seal(); ok {
		return t.completeWithEvents(ctx, eventEnvelopes)
	}

	return t.completeWithoutEvents(ctx)
}

// acquireLock serializes command handling for the handler when it prefers
// minimized concurrency. It blocks until no other transaction holds the lock.
func (t *commandTask) acquireLock(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`INSERT INTO integration.handlers (
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

// completeWithoutEvents removes the command from the queue when no events were
// recorded during handling.
func (t *commandTask) completeWithoutEvents(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT integration.complete_without_events($1)`,
		xsql.UUID(t.MessageID),
	); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// completeWithEvents removes the command from the queue and appends events that
// were recorded during handling in a single database round-trip.
func (t *commandTask) completeWithEvents(
	ctx context.Context,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	var (
		query strings.Builder
		args  []any
	)

	args = append(
		args,
		xsql.UUID(t.MessageID),
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
	)

	query.WriteString(`SELECT integration.complete_with_events($1, $2, ARRAY[`)

	first := true
	for eventEnvelope := range eventEnvelopes.All() {
		if first {
			first = false
		} else {
			query.WriteString(", ")
		}

		n := len(args)
		fmt.Fprintf(
			&query,
			"ROW($%d, $%d, $%d)::eventstream.event",
			n+1, n+2, n+3,
		)

		args = append(
			args,
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(eventEnvelope),
		)
	}

	query.WriteString(`])`)

	if _, err := t.Tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

func (t *commandTask) failAndPostpone(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT commandqueue.fail_and_postpone($1, $2, $3)`,
		xsql.UUID(t.MessageID),
		t.BackoffBase,
		t.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone queued command after failure: %w", err)
	}

	return nil
}
