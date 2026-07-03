package dogmaengine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"reflect"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

const (
	// DefaultProjectionCompactInterval is the default minimum time between
	// projection compaction attempts.
	DefaultProjectionCompactInterval = 6 * time.Hour
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

	// ProjectionCompactInterval is the minimum time between projection
	// compaction attempts. If non-positive [DefaultProjectionCompactInterval]
	// is used.
	ProjectionCompactInterval time.Duration

	// ready is a latch that is set when the engine is ready to accept commands
	// for execution. It is used to block ExecuteCommand() from proceeding until
	// the engine is ready.
	ready xsync.Latch

	// appConfig is the application configuration derived from e.App.
	appConfig *config.Application

	// packer is used to pack messages into envelopes for persistence.
	packer *envelopepb.Packer

	// commandTypes is the set of command types that the engine accepts for
	// execution.
	commandTypes map[reflect.Type]struct{}
}

// Run starts the engine and blocks until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	e.appConfig = runtimeconfig.FromApplication(e.App)

	if err := config.Validate(e.appConfig, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	e.setupCommandTypes()

	for _, handlerConfig := range e.appConfig.Handlers() {
		if err := e.initializeHandler(ctx, handlerConfig); err != nil {
			return fmt.Errorf(
				"unable to initialize handler %s: %w",
				handlerConfig.Identity(),
				err,
			)
		}
	}

	e.packer = &envelopepb.Packer{
		Application: e.appConfig.Identity(),
	}

	var runGroup sync.WaitGroup

	for _, handlerConfig := range e.appConfig.Handlers() {
		if handlerConfig.IsDisabled() {
			continue
		}

		for _, c := range e.newComponentsForHandler(handlerConfig) {
			runGroup.Go(func() {
				c.Run(ctx)

				if ctx.Err() == nil {
					panic(fmt.Sprintf(
						"%T component for handler %s stopped before context was canceled",
						c,
						handlerConfig.Identity(),
					))
				}
			})
		}
	}

	e.ready.Set()
	runGroup.Wait()
	return ctx.Err()
}

// Ready returns a channel that is closed when the engine is ready to accept
// commands for execution.
//
// It is exposed as an operational signal. For example, to signal readiness to a
// load balancer. It is not an error to attempt command execution before the
// engine is ready.
func (e *Engine) Ready() <-chan struct{} {
	return e.ready.Chan()
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

	e.commandTypes = map[reflect.Type]struct{}{}

	for route := range inboundCommandRoutes {
		typ := route.MessageType.Get().ReflectType()
		e.commandTypes[typ] = struct{}{}
	}
}

// initializeHandler initializes the engine's state for a handler.
//
// Even though they are not executed at runtime, it is called with
// configurations for disabled handlers.
func (e *Engine) initializeHandler(ctx context.Context, handlerConfig config.Handler) error {
	switch handlerConfig := handlerConfig.(type) {
	case *config.Projection:
		return projection.InitializeHandler(ctx, e.DB, handlerConfig)
	case *config.Process:
		return process.InitializeHandler(ctx, e.DB, handlerConfig)
	default:
		return nil
	}
}

// newComponentsForHandler creates engine components for the given handler.
func (e *Engine) newComponentsForHandler(handlerConfig config.Handler) []component {
	const (
		backoffBase  = 10 * time.Millisecond
		backoffCap   = 5 * time.Minute
		pollInterval = 25 * time.Millisecond
	)

	logger := e.newLoggerForHandler(handlerConfig)
	workers := e.workerCountForHandler(handlerConfig)

	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		return []component{
			&messagepump.MessagePump{
				Driver: &aggregate.CommandPump{
					DB:                   e.DB,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					CommandTypeIDs:       e.inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.DB,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger,
			},
		}

	case *config.Integration:
		return []component{
			&messagepump.MessagePump{
				Driver: &integration.CommandPump{
					DB:                   e.DB,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Concurrency:          handlerConfig.ConcurrencyPreference(),
					Packer:               e.packer,
					CommandTypeIDs:       e.inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.DB,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger,
			},
		}

	case *config.Process:
		return []component{
			&messagepump.MessagePump{
				Driver: &process.EventPump{
					DB:                   e.DB,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					EventTypeIDs:         e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
					Logger:               logger,
				},
				DB:           e.DB,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger,
			},
			&messagepump.MessagePump{
				Driver: &process.DeadlinePump{
					DB:                   e.DB,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					DeadlineTypeIDs:      e.inboundMessageTypeIDsForHandler(handlerConfig, message.DeadlineKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.DB,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger,
			},
		}

	case *config.Projection:
		compactInterval := e.ProjectionCompactInterval
		if compactInterval <= 0 {
			compactInterval = DefaultProjectionCompactInterval
		}

		return []component{
			&messagepump.MessagePump{
				Driver: &projection.EventPump{
					DB:           e.DB,
					Handler:      handlerConfig.Interface(),
					Identity:     handlerConfig.Identity(),
					Concurrency:  handlerConfig.ConcurrencyPreference(),
					EventTypeIDs: e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
					Logger:       logger,
				},
				DB:           e.DB,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger,
			},
			&projection.Compactor{
				DB:       e.DB,
				Handler:  handlerConfig.Interface(),
				Identity: handlerConfig.Identity(),
				Interval: compactInterval,
				Logger:   logger,
			},
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

// workerCountForHandler returns the number of workers goroutines to run for the
// given handler's message pump.
func (e *Engine) workerCountForHandler(handlerConfig config.Handler) int {
	switch handlerConfig := handlerConfig.(type) {
	case *config.Projection:
	case *config.Integration:
		if handlerConfig.ConcurrencyPreference() == dogma.MinimizeConcurrency {
			return 1
		}
	}

	return 10 * runtime.GOMAXPROCS(0)
}

// inboundMessageTypeIDsForHandler returns the message type IDs of all inbound
// messages routed to the given handler. It is represented as a slice of UUID
// strings for direct use in SQL queries.
func (*Engine) inboundMessageTypeIDsForHandler(
	handlerConfig config.Handler,
	messageKind message.Kind,
) *uuidpb.Set {
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageKind(messageKind)).
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	messageTypeIDs := &uuidpb.Set{}

	for route := range inboundRoutes {
		messageTypeID := uuidpb.MustParse(route.MessageTypeID.Get())
		messageTypeIDs.Add(messageTypeID)
	}

	return messageTypeIDs
}

// outboundMessageTypesForHandler returns the reflect.Types of all outbound
// messages of the given kind routed from the given handler.
func (*Engine) outboundMessageTypesForHandler(handlerConfig config.Handler) map[reflect.Type]struct{} {
	outboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageDirection(config.OutboundDirection)).
		Routes()

	types := map[reflect.Type]struct{}{}

	for route := range outboundRoutes {
		types[route.MessageType.Get().ReflectType()] = struct{}{}
	}

	return types
}
