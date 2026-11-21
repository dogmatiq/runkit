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
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/identitypb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// CommandSink is a [messagequeue.Sink] that delivers commands to a
// [dogma.IntegrationMessageHandler].
type CommandSink struct {
	// Packer packs events produced by the handler, and OutboundMessageTypes
	// contains the types that the handler is allowed to produce.
	Packer               *envelope.Packer
	OutboundMessageTypes map[reflect.Type]struct{}

	// Handler is the integration message handler to which commands are delivered,
	// and Identity is its identity.
	Handler  dogma.IntegrationMessageHandler
	Identity *identitypb.Identity

	// HandleTimeout is the maximum time the handler is given to handle each
	// command.
	HandleTimeout time.Duration
}

// Deliver delivers the command in the given envelope to the handler.
func (s *CommandSink) Deliver(
	ctx context.Context,
	tx *sql.Tx,
	d messagequeue.Delivery,
	env *envelopepb.Envelope,
	engineLogger *slog.Logger,
	appLogger *slog.Logger,
) error {

	var commandForHandling dogma.Command

	if err := xmessage.UnpackMessage(
		env,
		&commandForHandling,
	); err != nil {
		return fmt.Errorf("unable to unmarshal command: %w", err)
	}

	packer := s.Packer.PackEffects(
		env,
		s.Identity,
	)

	scope := &commandScope{
		messageID:            env.GetBody().GetMessageId().AsStd(),
		packer:               packer,
		outboundMessageTypes: s.OutboundMessageTypes,
		engineLogger:         engineLogger,
		appLogger:            appLogger,
	}

	if err := s.handleCommand(ctx, scope, commandForHandling); err != nil {
		return fmt.Errorf("unable to handle command: %w", err)
	}

	if eventEnvelopes, ok := packer.Seal(); ok {
		return s.appendEvents(ctx, tx, eventEnvelopes)
	}

	return nil
}

// LogAttr returns the attribute that identifies the handler in log records.
func (s *CommandSink) LogAttr() slog.Attr {
	return xslog.HandlerIdentity(s.Identity, config.IntegrationHandlerType)
}

// handleCommand calls the handler's HandleCommand method.
func (s *CommandSink) handleCommand(
	ctx context.Context,
	scope *commandScope,
	command dogma.Command,
) error {
	return xerrors.Recover(
		func() error {
			ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
			defer cancel()

			return s.Handler.HandleCommand(
				ctx,
				scope,
				command,
			)
		},
	)
}

// appendEvents appends the events that the handler recorded to an event
// stream.
func (s *CommandSink) appendEvents(
	ctx context.Context,
	tx *sql.Tx,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	var (
		query strings.Builder
		args  []any
	)

	args = append(
		args,
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId().AsStd()),
	)

	query.WriteString(`SELECT runkit.append_events_to_any_stream($1, NULL, NULL, ARRAY[`)

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
			"ROW($%d, $%d, $%d)::runkit.event",
			n+1, n+2, n+3,
		)

		args = append(
			args,
			xsql.UUID(eventEnvelope.GetBody().GetMessageId().AsStd()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId().AsStd()),
			xsql.Envelope(eventEnvelope),
		)
	}

	query.WriteString(`])`)

	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to append events: %w", err)
	}

	return nil
}
