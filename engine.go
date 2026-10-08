package runkit

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/grpc/messaginggrpc"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/notification"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	// DefaultProjectionCompactInterval is the default minimum time between
	// projection compaction attempts.
	DefaultProjectionCompactInterval = 6 * time.Hour

	// DefaultListenAddr is the default address for the engine's gRPC server.
	DefaultListenAddr = ":50555"
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
type Engine struct {
	// app is the Dogma application to run.
	app dogma.Application

	// db is the database connection to use.
	db *sql.DB

	// nodeID is the unique identifier for this engine instance.
	nodeID *uuidpb.UUID

	// compactInterval is the minimum time between projection
	// compaction attempts. If non-positive [DefaultProjectionCompactInterval]
	// is used.
	compactInterval time.Duration

	// Addr is the address to listen on for gRPC requests from other engines.
	// If it is empty, [DefaultListenAddr] is used.
	listenAddress string

	// logger is the structured logger used by the engine.
	//
	// If it is nil, [slog.Default] is used.
	logger *slog.Logger

	// ready is a latch that is set when the engine is ready to accept commands
	// for execution. It is used to block ExecuteCommand() from proceeding until
	// the engine is ready.
	ready xsync.Latch

	// listener is the TCP listener for gRPC requests from other engines.
	listener net.Listener

	// appConfig is the application configuration derived from e.App.
	appConfig *config.Application

	// notifications listens for inter-node notifications.
	notifications *notification.Listener

	// membership observes changes to the set of live nodes in the cluster.
	membership *cluster.MembershipObserver

	// eventStreams observes changes to the set of available event streams.
	eventStreams *eventstream.Observer

	// packer is used to pack messages into envelopes for persistence.
	packer *envelopepb.Packer

	// commandTypes is the set of command types that the engine accepts for
	// execution.
	commandTypes map[reflect.Type]struct{}
}

// New returns a new engine that runs the given application.
func New(
	app dogma.Application,
	db *sql.DB,
	options ...EngineOption,
) *Engine {
	e := &Engine{
		app:             app,
		db:              db,
		nodeID:          uuidpb.Generate(),
		compactInterval: DefaultProjectionCompactInterval,
		listenAddress:   DefaultListenAddr,
		logger:          slog.Default(),
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// Run starts the engine and blocks until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	e.logger = e.logger.With(
		xslog.UUID("node_id", e.nodeID),
	)

	e.appConfig = runtimeconfig.FromApplication(e.app)

	if err := config.Validate(e.appConfig, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	e.packer = &envelopepb.Packer{
		Application: e.appConfig.Identity(),
	}

	e.setupCommandTypes()

	// Start the gRPC listener before anything else; if we're unable to listen
	// on the configured address, we want to fail fast.
	//
	// We also want to do this synchronously so that e.listener is populated
	// before the e.ready latch is set.
	var err error
	e.listener, err = net.Listen("tcp", e.listenAddress)
	if err != nil {
		return fmt.Errorf("unable to listen on %s: %w", e.listenAddress, err)
	}
	defer e.listener.Close()

	// Next attempt to initialize handler state in the database.
	for _, handlerConfig := range e.appConfig.Handlers() {
		if err := e.initializeHandler(ctx, handlerConfig); err != nil {
			return fmt.Errorf(
				"unable to initialize handler %s: %w",
				handlerConfig.Identity(),
				err,
			)
		}
	}

	// Start an error group that will run all engine components until ctx is
	// canceled.
	group, ctx := errgroup.WithContext(ctx)

	// runComponent is a helper that runs c within group, and returns an error
	// if c.Run() returns before ctx is canceled.
	//
	// This is largely to catch programming errors in tests, as no component
	// should be returning before the engine is shut down; each is responsible
	// for recovering from its own errors.
	runComponent := func(c component) {
		group.Go(func() error {
			c.Run(ctx)

			if ctx.Err() == nil {
				return fmt.Errorf("%T component stopped before context was canceled", c)
			}

			return ctx.Err()
		})
	}

	e.notifications = &notification.Listener{
		DB:     e.db,
		Logger: e.logger.With(slog.String("component", "notification-listener")),
	}

	e.membership = &cluster.MembershipObserver{
		Notifications: e.notifications,
		Logger:        e.logger.With(slog.String("component", "cluster.membership-observer")),
	}

	heartbeater := &cluster.Heartbeater{
		NodeID: e.nodeID,
		DB:     e.db,
		Logger: e.logger.With(slog.String("component", "cluster.heartbeater")),
	}

	e.eventStreams = &eventstream.Observer{
		DB:            e.db,
		Notifications: e.notifications,
		Logger:        e.logger.With(slog.String("component", "eventstream.observer")),
	}

	runComponent(e.notifications)
	runComponent(e.membership)
	runComponent(heartbeater)
	runComponent(e.eventStreams)

	// Create and run components that manage all (non-disabled) handlers within
	// the application.
	for _, handlerConfig := range e.appConfig.Handlers() {
		if !handlerConfig.IsDisabled() {
			for _, c := range e.newComponentsForHandler(handlerConfig) {
				runComponent(c)
			}
		}
	}

	group.Go(func() error {
		server := grpc.NewServer()

		messaginggrpc.RegisterEventStreamConsumerAPIServer(
			server,
			&eventstream.ConsumerAPI{
				DB:           e.db,
				EventStreams: e.eventStreams,
				Logger: e.logger.With(
					slog.String("component", "eventstream.consumer-api"),
				),
			},
		)

		context.AfterFunc(ctx, server.Stop)

		e.logger.DebugContext(
			ctx,
			"listening for gRPC requests",
			slog.String("addr", e.listener.Addr().String()),
		)

		err := server.Serve(e.listener)

		if ctx.Err() == nil {
			return fmt.Errorf("gRPC server stopped before context was canceled: %w", err)
		}

		return ctx.Err()
	})

	e.ready.Set()
	group.Wait()
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

// ListenAddr returns the address that the engine is listening on for gRPC
// requests from other engines.
func (e *Engine) ListenAddr(ctx context.Context) (net.Addr, error) {
	if err := e.ready.WaitContext(ctx); err != nil {
		return nil, err
	}
	return e.listener.Addr(), nil
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
		return projection.InitializeHandler(ctx, e.db, handlerConfig)
	case *config.Process:
		return process.InitializeHandler(ctx, e.db, handlerConfig)
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
					DB:                   e.db,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					CommandTypeIDs:       e.inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.db,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger.With(slog.String("component", "aggregate.command-pump")),
			},
		}

	case *config.Integration:
		return []component{
			&messagepump.MessagePump{
				Driver: &integration.CommandPump{
					DB:                   e.db,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Concurrency:          handlerConfig.ConcurrencyPreference(),
					Packer:               e.packer,
					CommandTypeIDs:       e.inboundMessageTypeIDsForHandler(handlerConfig, message.CommandKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.db,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger.With(slog.String("component", "integration.command-pump")),
			},
		}

	case *config.Process:
		eventPumpLogger := logger.With(slog.String("component", "process.event-pump"))

		return []component{
			&messagepump.MessagePump{
				Driver: &process.EventPump{
					DB:                   e.db,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					EventTypeIDs:         e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
					Logger:               eventPumpLogger,
				},
				DB:           e.db,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       eventPumpLogger,
			},
			&messagepump.MessagePump{
				Driver: &process.DeadlinePump{
					DB:                   e.db,
					Handler:              handlerConfig.Interface(),
					Identity:             handlerConfig.Identity(),
					Packer:               e.packer,
					DeadlineTypeIDs:      e.inboundMessageTypeIDsForHandler(handlerConfig, message.DeadlineKind),
					OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				},
				DB:           e.db,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       logger.With(slog.String("component", "process.deadline-pump")),
			},
		}

	case *config.Projection:
		eventPumpLogger := logger.With(slog.String("component", "projection.event-pump"))

		return []component{
			&messagepump.MessagePump{
				Driver: &projection.EventPump{
					DB:           e.db,
					Handler:      handlerConfig.Interface(),
					Identity:     handlerConfig.Identity(),
					Concurrency:  handlerConfig.ConcurrencyPreference(),
					EventTypeIDs: e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
					Logger:       eventPumpLogger,
				},
				DB:           e.db,
				Workers:      workers,
				PollInterval: pollInterval,
				BackoffBase:  backoffBase,
				BackoffCap:   backoffCap,
				Logger:       eventPumpLogger,
			},
			&projection.Compactor{
				DB:       e.db,
				Handler:  handlerConfig.Interface(),
				Identity: handlerConfig.Identity(),
				Interval: e.compactInterval,
				Logger:   logger.With(slog.String("component", "projection.compactor")),
			},
		}

	default:
		panic(fmt.Sprintf("unsupported handler type: %T", handlerConfig))
	}
}

// newLoggerForHandler creates a logger for the given handler with the
// appropriate fields.
func (e *Engine) newLoggerForHandler(handlerConfig config.Handler) *slog.Logger {
	return e.logger.With(
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

// component is an interface for subsystems that run within the engine and
// manage their own lifecycle.
type component interface {
	// Run executes the component until ctx is canceled.
	Run(context.Context)
}
