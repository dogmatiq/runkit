package integration

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
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
	// Handler is the integration message handler to which commands are
	// delivered.
	Handler dogma.IntegrationMessageHandler

	// Identity is the identity of the integration handler.
	Identity *identitypb.Identity

	// Packer packs events produced by the handler.
	Packer *envelope.Packer

	// OutboundMessageTypes contains the types the handler is allowed to produce.
	OutboundMessageTypes map[reflect.Type]struct{}

	// Concurrency is the handler's concurrency preference.
	//
	// If it's [dogma.MinimizeConcurrency], the sink makes one call at a time
	// to the handler's HandleCommand method. While the handler is handling one
	// command, [CommandSink.Deliver] returns [messagequeue.ErrBusy] for any
	// other command, without calling the handler.
	Concurrency dogma.ConcurrencyPreference

	// HandleTimeout is the maximum time the handler is given to handle each
	// command.
	HandleTimeout time.Duration

	// handlerLock is held during each call to the handler's HandleCommand
	// method when Concurrency is [dogma.MinimizeConcurrency].
	handlerLock sync.Mutex
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
		engineLogger.ErrorContext(
			ctx,
			"unable to unmarshal command",
			xslog.Error(err),
		)
		return messagequeue.ErrFailed
	}

	packer := s.Packer.PackEffects(
		env,
		s.Identity,
	)

	scope := &commandScope{
		packer:               packer,
		engineLogger:         engineLogger,
		appLogger:            appLogger,
		outboundMessageTypes: s.OutboundMessageTypes,
	}

	called, err := s.handleCommand(ctx, scope, commandForHandling)

	// The handler is handling another command. Waiting for it would hold this
	// attempt's transaction and connection open for up to the handle timeout,
	// so the sink returns ErrBusy and the consumer postpones the delivery.
	if !called {
		return messagequeue.ErrBusy
	}

	if err != nil {
		engineLogger.ErrorContext(
			ctx,
			"unable to handle command",
			xslog.Error(err),
		)

		return messagequeue.ErrFailed
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
//
// If Concurrency is [dogma.MinimizeConcurrency] and another call holds
// handlerLock, it returns false without calling the handler.
func (s *CommandSink) handleCommand(
	ctx context.Context,
	scope *commandScope,
	command dogma.Command,
) (bool, error) {
	if s.Concurrency == dogma.MinimizeConcurrency {
		if !s.handlerLock.TryLock() {
			return false, nil
		}
		defer s.handlerLock.Unlock()
	}

	return true, xerrors.Recover(
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
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
	)

	query.WriteString(`SELECT eventstream.append_any($1, NULL, NULL, ARRAY[`)

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
		return fmt.Errorf("unable to append events: %w", err)
	}

	return nil
}
