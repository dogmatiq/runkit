package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/concurrency"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// CommandPump is a [messagepump.Driver] that delivers pending commands to an
// integration message handler of a specific type.
type CommandPump struct {
	DB                   *sql.DB
	Handler              dogma.IntegrationMessageHandler
	Identity             *identitypb.Identity
	Concurrency          dogma.ConcurrencyPreference
	Packer               *envelopepb.Packer
	CommandTypeIDs       *uuidpb.Set
	OutboundMessageTypes map[reflect.Type]struct{}
}

// AcquireDelivery attempts to acquire the next pending command for an
// integration handler of one of the configured types.
func (p *CommandPump) AcquireDelivery(ctx context.Context, tx *sql.Tx) (messagepump.Delivery, bool, error) {
	return messagepump.AcquireCommandDelivery(ctx, tx, p.CommandTypeIDs)
}

// PostponeDelivery reschedules the command for redelivery after delay.
func (p *CommandPump) PostponeDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	delay time.Duration,
) error {
	return messagepump.PostponeCommandDelivery(ctx, tx, delivery, delay)
}

// HandleDelivery dispatches a command to the integration handler.
func (p *CommandPump) HandleDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery messagepump.Delivery,
	envelope *envelopepb.Envelope,
	logger *slog.Logger,
) error {
	var commandForHandling dogma.Command

	if err := xmessage.UnpackMessage(
		envelope,
		&commandForHandling,
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			xslog.Error(err),
		)
		return messagepump.ErrFailed
	}

	packer := p.Packer.PackEffects(
		envelope,
		p.Identity,
	)

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		tx,
		p.Identity.GetKey(),
		p.Concurrency,
		func() error {
			return xerrors.Recover(
				func() error {
					return p.Handler.HandleCommand(
						ctx,
						&commandScope{
							packer:               packer,
							logger:               logger,
							outboundMessageTypes: p.OutboundMessageTypes,
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

		return messagepump.ErrFailed
	}

	if eventEnvelopes, ok := packer.Seal(); ok {
		return p.completeWithEvents(ctx, tx, delivery.MessageID, eventEnvelopes)
	}

	return p.completeWithoutEvents(ctx, tx, delivery.MessageID)
}

// completeWithEvents removes the command from the queue and appends events that
// were recorded during handling in a single database round-trip.
func (p *CommandPump) completeWithEvents(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	var (
		query strings.Builder
		args  []any
	)

	args = append(
		args,
		xsql.UUID(messageID),
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

	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}

// completeWithoutEvents removes the command from the queue when no events were
// recorded during handling.
func (p *CommandPump) completeWithoutEvents(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`SELECT integration.complete_without_events($1)`,
		xsql.UUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to complete command handling: %w", err)
	}

	return nil
}
