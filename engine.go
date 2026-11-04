package runkit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"runtime"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/aggregate"
	"github.com/dogmatiq/runkit/internal/application"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/grpcserver"
	"github.com/dogmatiq/runkit/internal/integration"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/process"
	"github.com/dogmatiq/runkit/internal/projection"
	"github.com/dogmatiq/runkit/internal/remoteengine"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
	// DefaultProjectionCompactInterval is the default minimum time between
	// successful projection compactions.
	DefaultProjectionCompactInterval = 6 * time.Hour

	// projectionCompactTimeout is the maximum time a projection is given to
	// compact its data in a single attempt.
	projectionCompactTimeout = 10 * time.Minute

	// applicationBindTimeout is the maximum time given to bind the database to
	// the application when the engine starts.
	applicationBindTimeout = 15 * time.Second

	// heartbeatInterval is the time between the heartbeats that each engine
	// sends to the others in the cluster.
	heartbeatInterval = 1 * time.Second

	// heartbeatTimeout is the amount of time an engine can go without sending
	// a heartbeat before the others consider it offline.
	heartbeatTimeout = 5 * heartbeatInterval

	// eventStreamCacheSize is the number of recent events cached per event
	// stream for delivery to consumers that are keeping up with the stream.
	eventStreamCacheSize = 100

	// eventStreamSegmentSize is the maximum number of events in one segment
	// read from the database.
	eventStreamSegmentSize = 100

	// eventStreamBufferSize is the number of batches of events buffered for
	// each subscription to an event stream. It absorbs short bursts of appends;
	// a subscription that falls further behind is served historical events
	// instead.
	eventStreamBufferSize = 4
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
//
// It must be created with [New]; the zero value is not usable.
type Engine struct {
	// app is the Dogma application to run.
	app dogma.Application

	// db is the database connection to use.
	db *sql.DB

	// nodeID is the unique identifier for this engine instance.
	nodeID *uuidpb.UUID

	// compactInterval is the minimum time between successful projection
	// compactions.
	compactInterval time.Duration

	// listenAddress is the address to listen on for gRPC requests from other
	// engines. If it is empty, the engine does not listen for gRPC requests.
	listenAddress string

	// grpcOptions are the options used to create the engine's gRPC server.
	grpcOptions []grpc.ServerOption

	// remoteEngineDiscoverers are the discoverers that the engine uses to find
	// remote engines.
	remoteEngineDiscoverers []RemoteEngineDiscoverer

	// logger is the structured logger used by the engine.
	logger *slog.Logger

	// ready is a latch that is set when the engine is ready to accept commands
	// for execution. It is used to block ExecuteCommand() from proceeding until
	// the engine is ready.
	ready xsync.Latch

	// stopped is a latch that is set when Run() returns.
	stopped xsync.Latch

	// listenAddr is the address the engine is listening on for gRPC requests
	// from other engines. If the engine listens for gRPC requests, it is
	// populated before the ready latch is set; otherwise, it is nil.
	listenAddr net.Addr

	// appConfig is the application configuration derived from e.app.
	appConfig *config.Application

	// signals sends and receives signals on behalf of this node.
	signals *signal.Bus

	// node represents this engine's node in the cluster.
	node *cluster.Node

	// eventStreams is the catalog from which event streams are consumed.
	eventStreams *eventstream.Catalog

	// packer is used to pack messages into envelopes for persistence.
	packer *envelope.Packer

	// commandTypes is the set of command types that the engine accepts for
	// execution.
	commandTypes map[reflect.Type]struct{}

	// group runs the engine's components. If any of them fails, the others are
	// stopped.
	group *errgroup.Group
}

// New returns a new engine that runs the given application.
//
// db must use the [pgx] driver, for example by opening it with
// sql.Open("pgx", dsn) or [stdlib.OpenDB]. It panics if db is nil or uses any
// other driver.
//
// [pgx]: https://pkg.go.dev/github.com/jackc/pgx/v5/stdlib
func New(
	app dogma.Application,
	db *sql.DB,
	options ...EngineOption,
) *Engine {
	if db == nil {
		panic("database must not be nil")
	}

	if _, ok := db.Driver().(*stdlib.Driver); !ok {
		panic(fmt.Sprintf(
			"database must use the pgx driver (github.com/jackc/pgx/v5/stdlib), got %T",
			db.Driver(),
		))
	}

	e := &Engine{
		app:             app,
		db:              db,
		nodeID:          uuidpb.Generate(),
		compactInterval: DefaultProjectionCompactInterval,
		logger:          slog.Default(),
	}

	for _, option := range options {
		option(e)
	}

	return e
}

// Run starts the engine and blocks until ctx is canceled or the engine is
// unable to continue.
//
// It must be called at most once.
func (e *Engine) Run(ctx context.Context) error {
	defer e.stopped.Set()

	e.logger = e.logger.With(
		xslog.UUID("node_id", e.nodeID),
	)

	e.appConfig = runtimeconfig.FromApplication(e.app)

	if err := config.Validate(e.appConfig, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	bindCtx, cancelBind := context.WithTimeout(ctx, applicationBindTimeout)
	defer cancelBind()

	if err := application.Bind(
		bindCtx,
		e.db,
		e.appConfig.Identity(),
	); err != nil {
		return fmt.Errorf(
			"unable to bind database to application %s: %w",
			e.appConfig.Identity(),
			err,
		)
	}

	e.packer = &envelope.Packer{
		Application: e.appConfig.Identity(),
	}

	e.setupCommandTypes()

	e.signals = &signal.Bus{
		DB:     e.db,
		Logger: e.logger.With(slog.String("component", "signal.bus")),
	}

	e.node = &cluster.Node{
		ID:       e.nodeID,
		Signals:  e.signals,
		Interval: heartbeatInterval,
		Timeout:  heartbeatTimeout,
		Logger:   e.logger.With(slog.String("component", "cluster.node")),
	}

	contexthook.Invoke(ctx, contexthook.Node{
		Node: e.node,
	})

	e.eventStreams = &eventstream.Catalog{
		CacheSize: eventStreamCacheSize,
		Logger:    e.logger.With(slog.String("component", "eventstream.catalog")),
	}

	localEventStreams := &source.Local{
		DB:          e.db,
		Signals:     e.signals,
		Logger:      e.logger.With(slog.String("component", "eventstream.source.local")),
		SegmentSize: eventStreamSegmentSize,
	}

	var server *grpcserver.Server

	if e.listenAddress != "" {
		server = &grpcserver.Server{
			Address: e.listenAddress,
			EventStreamConsumerAPI: &eventstream.ConsumerAPI{
				DB:                   e.db,
				EventStreams:         e.eventStreams.Legacy(),
				RecordableEventTypes: e.recordableEventTypes(),
				BufferSize:           eventStreamBufferSize,
				Logger: e.logger.With(
					slog.String("component", "eventstream.consumer-api"),
				),
			},
			Options: e.grpcOptions,
			Logger:  e.logger.With(slog.String("component", "grpc.server")),
		}
	}

	// Initialize handler state in the database.
	for _, handlerConfig := range e.appConfig.Handlers() {
		if err := e.initializeHandler(ctx, handlerConfig); err != nil {
			return fmt.Errorf(
				"unable to initialize handler %s: %w",
				handlerConfig.Identity(),
				err,
			)
		}
	}

	e.group, ctx = errgroup.WithContext(ctx)

	e.group.Go(func() error {
		return e.signals.Run(ctx)
	})

	e.group.Go(func() error {
		return e.node.Run(ctx)
	})

	e.group.Go(func() error {
		return e.eventStreams.Watch(ctx, localEventStreams, nil)
	})

	for _, d := range e.remoteEngineDiscoverers {
		connector := &remoteengine.Connector{
			Discover: d,
			Logger: e.logger.With(
				slog.String("component", "remoteengine.connector"),
			),
		}

		e.group.Go(func() error {
			return connector.Run(ctx)
		})
	}

	for _, handlerConfig := range e.appConfig.Handlers() {
		if !handlerConfig.IsDisabled() {
			e.runHandler(ctx, handlerConfig)
		}
	}

	if server == nil {
		e.ready.Set()
	} else {
		e.group.Go(func() error {
			return server.Run(ctx)
		})

		// The engine is not ready until it is listening for gRPC requests. If
		// the server failed to listen, ctx is canceled and the failure is
		// reported by the group.
		if addr, err := server.ListenAddr(ctx); err == nil {
			e.listenAddr = addr
			e.ready.Set()
		}
	}

	return e.group.Wait()
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
//
// It returns an error immediately if the engine was created without
// [WithListenAddress]. Otherwise, it blocks until the engine is ready. It
// returns an error if ctx is canceled before then, or if the engine has
// stopped.
func (e *Engine) ListenAddr(ctx context.Context) (net.Addr, error) {
	if e.listenAddress == "" {
		return nil, errors.New("engine does not listen for gRPC requests")
	}

	if err := e.waitUntilReady(ctx); err != nil {
		return nil, err
	}
	return e.listenAddr, nil
}

// waitUntilReady blocks until the engine is ready or ctx is canceled.
//
// It returns an error if the engine has stopped, even if it was ready
// beforehand.
func (e *Engine) waitUntilReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ready.Chan():
	case <-e.stopped.Chan():
	}

	// Once the engine has stopped, both latches are set, and select chooses
	// between ready cases at random. Checking stopped afterwards, rather than
	// before the select, ensures a stopped engine is always reported, even if
	// it stops while waiting.
	if e.stopped.IsSet() {
		return errors.New("engine is not running")
	}

	return nil
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

// runHandler runs the components that manage the given handler.
func (e *Engine) runHandler(ctx context.Context, handlerConfig config.Handler) {
	const (
		backoffBase  = 10 * time.Millisecond
		backoffCap   = 5 * time.Minute
		pollInterval = 25 * time.Millisecond
	)

	logger := e.newLoggerForHandler(handlerConfig)
	workers := e.workerCountForHandler(handlerConfig)

	switch handlerConfig := handlerConfig.(type) {
	case *config.Aggregate:
		pump := &messagepump.MessagePump{
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
		}

		e.group.Go(func() error {
			return pump.Run(ctx)
		})

	case *config.Integration:
		pump := &messagepump.MessagePump{
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
		}

		e.group.Go(func() error {
			return pump.Run(ctx)
		})

	case *config.Process:
		eventPumpLogger := logger.With(slog.String("component", "process.event-pump"))

		eventPump := &messagepump.MessagePump{
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
		}

		deadlinePump := &messagepump.MessagePump{
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
		}

		e.group.Go(func() error {
			return eventPump.Run(ctx)
		})

		e.group.Go(func() error {
			return deadlinePump.Run(ctx)
		})

	case *config.Projection:
		consumer := &projection.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams.Legacy(),
			Handler:      handlerConfig.Interface(),
			Identity:     handlerConfig.Identity(),
			Concurrency:  handlerConfig.ConcurrencyPreference(),
			EventTypeIDs: e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
			BufferSize:   eventStreamBufferSize,
			Logger:       logger.With(slog.String("component", "projection.consumer")),
		}

		compactor := &projection.Compactor{
			DB:       e.db,
			Handler:  handlerConfig.Interface(),
			Identity: handlerConfig.Identity(),
			Interval: e.compactInterval,
			Timeout:  projectionCompactTimeout,
			Logger:   logger.With(slog.String("component", "projection.compactor")),
		}

		e.group.Go(func() error {
			return consumer.Run(ctx)
		})

		e.group.Go(func() error {
			return compactor.Run(ctx)
		})

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
) *uuidcollection.Set {
	inboundRoutes := handlerConfig.
		RouteSet().
		Filter(config.FilterByMessageKind(messageKind)).
		Filter(config.FilterByMessageDirection(config.InboundDirection)).
		Routes()

	messageTypeIDs := &uuidcollection.Set{}

	for route := range inboundRoutes {
		messageTypeID := uuidpb.MustParse(route.MessageTypeID.Get())
		messageTypeIDs.Add(messageTypeID)
	}

	return messageTypeIDs
}

// recordableEventTypes returns the message type IDs of all event types that
// may be recorded by any (non-disabled) handler within the application.
func (e *Engine) recordableEventTypes() *uuidcollection.Set {
	outboundEventRoutes := e.appConfig.
		RouteSet().
		Filter(config.FilterByMessageKind(message.EventKind)).
		Filter(config.FilterByMessageDirection(config.OutboundDirection)).
		Routes()

	messageTypeIDs := &uuidcollection.Set{}

	for route, handlerConfig := range outboundEventRoutes {
		if !handlerConfig.IsDisabled() {
			messageTypeID := uuidpb.MustParse(route.MessageTypeID.Get())
			messageTypeIDs.Add(messageTypeID)
		}
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
