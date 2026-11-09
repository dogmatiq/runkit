package runkit

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"reflect"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/application"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/eventstream/source"
	"github.com/dogmatiq/runkit/internal/grpcserver"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/remoteengine"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/jackc/pgx/v5/stdlib"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
)

const (
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
	// behind ends with [source.ErrNotContiguous].
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

	// engineLogger is the target for the engine's own log records.
	engineLogger *slog.Logger

	// appLogger is the target for the application's log records.
	appLogger *slog.Logger

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

	// queues keeps track of which queues have messages.
	queues *messagequeue.Catalog

	// queueRouter chooses the queue for each new message.
	queueRouter *messagequeue.Router

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
		engineLogger:    slog.Default(),
		appLogger:       slog.Default(),
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

	e.appConfig = runtimeconfig.FromApplication(e.app)

	if err := config.Validate(e.appConfig, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	e.engineLogger = e.engineLogger.With(
		xslog.UUID("node_id", e.nodeID),
		xslog.Identity("application", e.appConfig.Identity()),
		slog.String("layer", "engine"),
	)

	e.appLogger = e.appLogger.With(
		xslog.UUID("node_id", e.nodeID),
		xslog.Identity("application", e.appConfig.Identity()),
		slog.String("layer", "application"),
	)

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

	e.commandTypes = executableCommandTypes(e.appConfig)

	e.queueRouter = &messagequeue.Router{
		NodeID:     e.nodeID,
		Aggregates: aggregatesForQueueRouter(e.appConfig),
	}

	e.signals = &signal.Bus{
		DB:           e.db,
		EngineLogger: e.engineLogger,
	}

	e.node = &cluster.Node{
		ID:                e.nodeID,
		DB:                e.db,
		Signals:           e.signals,
		HeartbeatInterval: heartbeatInterval,
		HeartbeatTimeout:  heartbeatTimeout,
		EngineLogger:      e.engineLogger,
	}

	contexthook.Invoke(ctx, contexthook.Node{
		Node: e.node,
	})

	e.queues = &messagequeue.Catalog{
		DB:           e.db,
		Signals:      e.signals,
		EngineLogger: e.engineLogger,
	}

	e.eventStreams = &eventstream.Catalog{
		CacheSize:    eventStreamCacheSize,
		EngineLogger: e.engineLogger,
	}

	localEventStreams := &source.Local{
		DB:           e.db,
		Signals:      e.signals,
		EngineLogger: e.engineLogger,
		SegmentSize:  eventStreamSegmentSize,
		BufferSize:   eventStreamBufferSize,
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
			Options:      e.grpcOptions,
			EngineLogger: e.engineLogger,
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
		return e.queues.Run(ctx)
	})

	eventTypeIDs := consumedEventTypeIDs(e.appConfig)

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
			EngineLogger: e.engineLogger,
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

		e.engineLogger.InfoContext(ctx, "engine started")
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

			e.engineLogger.InfoContext(
				ctx,
				"engine started",
				slog.String("grpc_listen_address", addr.String()),
			)
		}
	}

	err := e.group.Wait()

	// A canceled context is how the engine is asked to stop, so it isn't
	// reported as the reason it stopped.
	var attrs []slog.Attr
	if !errors.Is(err, ctx.Err()) {
		attrs = append(attrs, xslog.Error(err))
	}

	// The context may already be canceled, but the record must still be
	// written.
	e.engineLogger.LogAttrs(
		context.WithoutCancel(ctx),
		slog.LevelInfo,
		"engine stopped",
		attrs...,
	)

	return err
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
