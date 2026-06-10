package dogmaengine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/reference-engine/internal/aggregate"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
	"golang.org/x/sync/errgroup"
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
type Engine struct {
	// DB is the database connection to use.
	DB *sql.DB

	// App is the Dogma application to run.
	App dogma.Application

	// Logger is the structured logger used by the engine.
	//
	// If it is nil, [slog.Default] is used.
	Logger *slog.Logger

	// ready is a latch that is set when the engine is ready to accept commands
	// for execution. It is used to block ExecuteCommand() from proceeding until
	// the engine is ready.
	ready xsync.Latch

	// appConfig is the application configuration derived from e.App.
	appConfig *config.Application

	// envelopePacker is used to pack messages into envelopes for persistence.
	envelopePacker *envelopepb.Packer

	// inboundCommandTypes is the set of command types that the engine accepts for
	// execution.
	inboundCommandTypes map[reflect.Type]struct{}
}

// Run starts the engine and blocks until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	e.appConfig = runtimeconfig.FromApplication(e.App)

	if err := config.Validate(e.appConfig, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	e.setupCommandTypes()

	e.envelopePacker = &envelopepb.Packer{
		Application: e.appConfig.Identity(),
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, handlerConfig := range e.appConfig.Handlers() {
		c := e.newControllerForHandler(handlerConfig)
		g.Go(func() error {
			return c.Run(ctx)
		})
	}

	e.ready.Set()

	return g.Wait()
}

// ExecuteCommand submits a [Command] for execution.
//
// It returns once the engine has taken ownership of the command. By
// default, it doesn't wait for handling to finish.
//
// See [dogma.CommandExecutor] for more details.
func (e *Engine) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	_ ...dogma.ExecuteCommandOption,
) error {
	if err := e.ready.WaitContext(ctx); err != nil {
		return err
	}

	commandEnvelope := e.envelopePacker.PackCommand(command)

	contexthook.Invoke(ctx, contexthook.ExecuteCommand{
		CommandEnvelope: commandEnvelope,
	})

	if err := command.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     commandEnvelope,
		},
	); err != nil {
		return err
	}

	if _, ok := e.inboundCommandTypes[reflect.TypeOf(command)]; !ok {
		return fmt.Errorf("no route found for command type: %T", command)
	}

	return xsql.Transact(
		ctx,
		e.DB,
		func(ctx context.Context, tx *sql.Tx) error {
			if err := commandqueue.Add(
				ctx,
				tx,
				commandEnvelope,
			); err != nil {
				return err
			}

			e.Logger.InfoContext(
				ctx,
				"enqueued command for execution",
				xslog.Envelope("command", commandEnvelope),
			)

			return nil
		},
	)
}

// setupCommandTypes builds a set of command types that the engine accepts for
// execution.
func (e *Engine) setupCommandTypes() {
	inboundCommandRoutes := e.appConfig.
		RouteSet().
		Filter(
			config.FilterByMessageDirection(config.InboundDirection),
			config.FilterByMessageKind(message.CommandKind),
		).
		Routes()

	e.inboundCommandTypes = map[reflect.Type]struct{}{}

	for route := range inboundCommandRoutes {
		typ := route.MessageType.Get().ReflectType()
		e.inboundCommandTypes[typ] = struct{}{}
	}
}

// controller is the interface implemented by all message handler controllers.
type controller interface {
	Run(context.Context) error
}

// newControllerForHandler creates a controller for the given handler.
func (e *Engine) newControllerForHandler(handlerConfig config.Handler) controller {
	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		return &aggregate.Controller{
			DB:              e.DB,
			Handler:         handlerConfig.Interface(),
			HandlerIdentity: handlerConfig.Identity(),
			EnvelopePacker:  e.envelopePacker,
			CommandTypeIDs:  e.collectInboundMessageTypeIDs(handlerConfig),
			Logger:          e.newLoggerForHandler(handlerConfig),
		}
	default:
		panic(fmt.Sprintf("unsupported handler type: %T", handlerConfig))
	}
}

// newLoggerForHandler creates a logger for the given handler with the
// appropriate fields.
func (e *Engine) newLoggerForHandler(handlerConfig config.Handler) *slog.Logger {
	return e.Logger.With(
		xslog.Identity(
			"handler",
			handlerConfig.Identity(),
			slog.String("type", handlerConfig.HandlerType().String()),
		),
	)
}

// collectInboundMessageTypeIDs returns the message type IDs of all inbound
// messages routed to the given handler. It is represented as a slice of UUID
// strings for direct use in SQL queries.
func (*Engine) collectInboundMessageTypeIDs(handlerConfig config.Handler) []string {
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	var inboundMessageTypeIDs []string

	for route := range inboundRoutes {
		inboundMessageTypeIDs = append(inboundMessageTypeIDs, route.MessageTypeID.Get())
	}

	return inboundMessageTypeIDs
}
