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

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/uuidcollection"
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
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
)

const (
	// messageBackoffBase and messageBackoffCap bound message delivery retries.
	messageBackoffBase = 10 * time.Millisecond
	messageBackoffCap  = 5 * time.Minute

	// messagePollInterval is the polling interval for command queues.
	messagePollInterval = 25 * time.Millisecond

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

	// eventStreamBufferSize is the number of segments that [source.Local]
	// buffers for each call to [source.Local.Watch]. A call that falls further
	// behind ends with [source.ErrConsumerTooSlow].
	eventStreamBufferSize = 100

	// eventStreamMaxSegmentBytes is the maximum combined size, in bytes, of the
	// events in each segment that the gRPC server sends to a remote engine. If
	// a single event is larger than this, it is sent on its own. It is well
	// under the 4 MiB message size that gRPC clients accept by default.
	eventStreamMaxSegmentBytes = 2 << 20
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

	// compactTimeout is the maximum time a projection is given to compact its
	// data in a single attempt.
	compactTimeout time.Duration

	// handleTimeout is the maximum time an application message handler is
	// given to handle a single message.
	handleTimeout time.Duration

	// routeTimeout is the maximum time an application message handler is
	// given to decide where a message is delivered, or where consumption of an
	// event stream resumes.
	routeTimeout time.Duration

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
	ready cosyne.Latch

	// stopped is a latch that is set when Run() returns.
	stopped cosyne.Latch

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
		compactTimeout:  DefaultProjectionCompactTimeout,
		handleTimeout:   DefaultHandleTimeout,
		routeTimeout:    DefaultRouteTimeout,
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
		BufferSize:  eventStreamBufferSize,
	}

	var server *grpcserver.Server

	if e.listenAddress != "" {
		server = &grpcserver.Server{
			Address: e.listenAddress,
			EventStreamConsumerAPI: &source.Server{
				// The server provides only the application's own streams, so
				// that it never relays events recorded by a remote engine's
				// application.
				EventStreams:    localEventStreams,
				MaxSegmentBytes: eventStreamMaxSegmentBytes,
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

	eventTypeIDs := e.consumedEventTypeIDs()

	// The application's own streams are watched only if the catalog consumes
	// them for projections or processes, or the gRPC server serves them remotely.
	if !eventTypeIDs.IsEmpty() || server != nil {
		e.group.Go(func() error {
			return localEventStreams.Run(ctx)
		})
	}

	// Watch local streams if any enabled projection or process consumes events.
	if !eventTypeIDs.IsEmpty() {
		e.group.Go(func() error {
			return e.eventStreams.Watch(ctx, localEventStreams, eventTypeIDs)
		})
	}

	for _, d := range e.remoteEngineDiscoverers {
		connector := &remoteengine.Connector{
			Discover:     d,
			EventStreams: e.eventStreams,
			EventTypeIDs: eventTypeIDs,
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

	e.runDeadlineScheduler(ctx)

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
	default:
		return nil
	}
}

// runHandler runs the components that manage the given handler.
func (e *Engine) runHandler(ctx context.Context, handlerConfig config.Handler) {
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
			PollInterval: messagePollInterval,
			BackoffBase:  messageBackoffBase,
			BackoffCap:   messageBackoffCap,
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
				HandleTimeout:        e.handleTimeout,
			},
			DB:           e.db,
			Workers:      workers,
			PollInterval: messagePollInterval,
			BackoffBase:  messageBackoffBase,
			BackoffCap:   messageBackoffCap,
			Logger:       logger.With(slog.String("component", "integration.command-pump")),
		}

		e.group.Go(func() error {
			return pump.Run(ctx)
		})

	case *config.Process:
		consumerLogger := logger.With(slog.String("component", "process.consumer"))
		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &process.Sink{
				DB:                   e.db,
				Handler:              handlerConfig.Interface(),
				Identity:             handlerConfig.Identity(),
				Packer:               e.packer,
				EventTypes:           e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				OutboundMessageTypes: e.outboundMessageTypesForHandler(handlerConfig),
				HandleTimeout:        e.handleTimeout,
				RouteTimeout:         e.routeTimeout,
				Logger:               consumerLogger,
			},
			Logger: consumerLogger,
		}

		e.group.Go(func() error {
			return consumer.Run(ctx)
		})

	case *config.Projection:
		var handlerLock *semaphore.Weighted
		if handlerConfig.ConcurrencyPreference() == dogma.MinimizeConcurrency {
			handlerLock = semaphore.NewWeighted(1)
		}

		consumerLogger := logger.With(slog.String("component", "projection.consumer"))
		consumer := &eventstream.Consumer{
			Node:         e.node,
			EventStreams: e.eventStreams,
			Sink: &projection.Sink{
				Handler:       handlerConfig.Interface(),
				Identity:      handlerConfig.Identity(),
				Concurrency:   handlerConfig.ConcurrencyPreference(),
				EventTypes:    e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind),
				HandlerLock:   handlerLock,
				HandleTimeout: e.handleTimeout,
				RouteTimeout:  e.routeTimeout,
				Logger:        consumerLogger,
			},
			Logger: consumerLogger,
		}

		compactor := &projection.Compactor{
			DB:       e.db,
			Handler:  handlerConfig.Interface(),
			Identity: handlerConfig.Identity(),
			Interval: e.compactInterval,
			Timeout:  e.compactTimeout,
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

// runDeadlineScheduler starts one scheduler for all enabled process handlers.
func (e *Engine) runDeadlineScheduler(ctx context.Context) {
	scheduler := &process.DeadlineScheduler{
		Node:    e.node,
		Signals: e.signals,
		Logger:  e.logger.With(slog.String("component", "process.deadline-scheduler")),
	}

	for _, handlerConfig := range e.appConfig.Handlers() {
		processConfig, ok := handlerConfig.(*config.Process)
		if !ok || processConfig.IsDisabled() {
			continue
		}
		types := e.inboundMessageTypeIDsForHandler(processConfig, message.DeadlineKind)
		if types.IsEmpty() {
			continue
		}
		scheduler.Sinks = append(scheduler.Sinks, &process.DeadlineSink{
			DB:                   e.db,
			Handler:              processConfig.Interface(),
			Identity:             processConfig.Identity(),
			Packer:               e.packer,
			DeadlineTypeIDs:      types,
			OutboundMessageTypes: e.outboundMessageTypesForHandler(processConfig),
			HandleTimeout:        e.handleTimeout,
			Concurrency:          e.workerCountForHandler(processConfig),
			BackoffBase:          messageBackoffBase,
			BackoffCap:           messageBackoffCap,
			Logger:               e.newLoggerForHandler(processConfig).With(slog.String("component", "process.deadline-sink")),
		})
	}

	if len(scheduler.Sinks) != 0 {
		e.group.Go(func() error {
			return scheduler.Run(ctx)
		})
	}
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

// consumedEventTypeIDs returns the message type IDs of every event type that
// is routed to a non-disabled handler within the application.
func (e *Engine) consumedEventTypeIDs() *uuidcollection.Set {
	messageTypeIDs := uuidcollection.NewSet()

	for _, handlerConfig := range e.appConfig.Handlers() {
		if handlerConfig.IsDisabled() {
			continue
		}

		eventTypeIDs := e.inboundMessageTypeIDsForHandler(handlerConfig, message.EventKind)

		for id := range eventTypeIDs.All() {
			messageTypeIDs.Add(id)
		}
	}

	return messageTypeIDs
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
