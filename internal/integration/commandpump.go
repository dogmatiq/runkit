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
	"github.com/dogmatiq/reference-engine/internal/concurrency"
	"github.com/dogmatiq/reference-engine/internal/messagepump"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// CommandPump is an engine component that periodically attempts to acquire
// pending commands for dispatch to an integration message handler of a specific
// type.
type CommandPump struct {
	DB                      *sql.DB
	Handler                 dogma.IntegrationMessageHandler
	Identity                *identitypb.Identity
	Concurrency             dogma.ConcurrencyPreference
	Packer                  *envelopepb.Packer
	CommandTypeIDs          []string
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (p *CommandPump) Run(ctx context.Context) {
	messagepump.Run(
		ctx,
		p.DB,
		p.Logger,
		p.acquireNextCommand,
		p.handleDelivery,
	)
}

func (p *CommandPump) acquireNextCommand(ctx context.Context, tx *sql.Tx) (messagepump.Delivery, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.message_id,
			c.message_type_id,
			c.envelope,
			c.failures
		FROM commandqueue.commands AS c
		WHERE message_type_id = ANY($1)
		AND deliver_at <= clock_timestamp()
		ORDER BY deliver_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		p.CommandTypeIDs,
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

		return messagepump.Delivery{}, false, fmt.Errorf("unable to scan pending command: %w", err)
	}

	return del, true, nil
}

// errFailed is a sentinel error that indicates a command could not be handled
// and should be postponed for re-delivery.
var errFailed = errors.New("unable to handle command")

func (p *CommandPump) handleDelivery(ctx context.Context, dc *messagepump.DeliveryContext) error {
	err := p.handleCommand(ctx, dc)

	if errors.Is(err, errFailed) {
		err = p.failAndPostpone(ctx, dc)
	}

	return err
}

func (p *CommandPump) handleCommand(ctx context.Context, dc *messagepump.DeliveryContext) error {
	var (
		commandEnvelope    = &envelopepb.Envelope{}
		commandForHandling dogma.Command
	)

	if err := xmessage.Unpack(
		dc.EnvelopeBytes,
		commandEnvelope,
		&commandForHandling,
	); err != nil {
		dc.Logger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			xslog.Error(err),
		)
		return errFailed
	}

	logger := dc.Logger.With(
		xslog.Envelope("command", commandEnvelope),
	)

	packer := p.Packer.PackEffects(
		commandEnvelope,
		p.Identity,
	)

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		dc.Tx,
		p.Identity.GetKey(),
		p.Concurrency,
		func() error {
			return xerrors.ConvertPanicToError(
				func() error {
					return p.Handler.HandleCommand(
						ctx,
						&commandScope{
							packer: packer,
							logger: logger,
						},
						commandForHandling,
					)
				},
			)
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return errFailed
	}

	if eventEnvelopes, ok := packer.Seal(); ok {
		return p.completeWithEvents(ctx, dc, eventEnvelopes)
	}

	return p.completeWithoutEvents(ctx, dc)
}

// completeWithEvents removes the command from the queue and appends events that
// were recorded during handling in a single database round-trip.
func (p *CommandPump) completeWithEvents(
	ctx context.Context,
	dc *messagepump.DeliveryContext,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	var (
		query strings.Builder
		args  []any
	)

	args = append(
		args,
		xsql.UUID(dc.MessageID),
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

	if _, err := dc.Tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// completeWithoutEvents removes the command from the queue when no events were
// recorded during handling.
func (p *CommandPump) completeWithoutEvents(ctx context.Context, dc *messagepump.DeliveryContext) error {
	if _, err := dc.Tx.ExecContext(
		ctx,
		`SELECT integration.complete_without_events($1)`,
		xsql.UUID(dc.MessageID),
	); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// failAndPostpone marks the command as failed and postpones it for re-delivery
// according to the configured backoff parameters.
func (p *CommandPump) failAndPostpone(ctx context.Context, dc *messagepump.DeliveryContext) error {
	if _, err := dc.Tx.ExecContext(
		ctx,
		`SELECT commandqueue.fail_and_postpone($1, $2, $3)`,
		xsql.UUID(dc.MessageID),
		p.BackoffBase,
		p.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone queued command: %w", err)
	}

	return nil
}
