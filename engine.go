package runkit

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/persistencekit"
	"github.com/dogmatiq/runkit/internal/heartbeat"
	"github.com/dogmatiq/runkit/internal/network"
	"golang.org/x/sync/errgroup"
)

// Engine runs a single Dogma [dogma.Application].
type Engine struct {
	// Deprecated: goal is to make this transient within New() and remove it
	cfg engineConfig

	nodeID  *uuidpb.UUID
	packer  *envelopepb.Packer
	routes  uuidpb.Map[commandSink]
	started atomic.Bool
	ready   xsync.Latch
}

// New returns an [Engine] that runs the given [dogma.Application].
//
// It panics if app is nil. It returns an error if the engine configuration is
// incomplete, such as when a site identity or persistence provider has not been
// specified.
//
// By default, New reads the following environment variables to fill in any
// configuration not provided by explicit options:
//
//   - DOGMA_SITE_NAME (see [WithSiteIdentity])
//   - DOGMA_SITE_KEY (see [WithSiteIdentity])
//   - DOGMA_NODE_ID (see [WithNodeID])
//   - DOGMA_PERSISTENCE_URL (see [WithPersistence])
//   - DOGMA_LISTEN_ADDRESS (see [WithListenAddress])
//   - DOGMA_ADVERTISE_ADDRESS (see [WithAdvertiseAddress])
//
// Explicit options always take precedence over environment variables. Use
// [WithoutEnvironment] to disable environment variable reading entirely.
func New(app dogma.Application, opts ...Option) (*Engine, error) {
	cfg, err := newEngineConfig(app, opts...)
	if err != nil {
		return nil, err
	}

	e := &Engine{
		cfg: cfg,

		nodeID: cfg.NodeID,
		packer: &envelopepb.Packer{
			Site:        cfg.Site,
			Application: cfg.App.Identity(),
		},
	}

	setupCommandRoutes(cfg, e)

	return e, nil
}

func setupCommandRoutes(cfg engineConfig, e *Engine) {
	routes := cfg.App.
		RouteSet().
		Filter(config.FilterByRouteType(config.HandlesCommandRouteType)).
		Routes()

	for r, h := range routes {
		if h.IsDisabled() {
			continue
		}

		typeID := uuidpb.MustParse(r.MessageTypeID.Get())

		config.SwitchByHandlerTypeOf(
			h,
			func(*config.Aggregate) { e.routes.Set(typeID, aggregateCommandSink{}) },
			func(*config.Process) {},
			func(*config.Integration) { e.routes.Set(typeID, integrationCommandSink{}) },
			func(*config.Projection) {},
		)
	}
}

// Run starts the engine and blocks until ctx is canceled or a fatal error
// occurs.
//
// It panics if called more than once on the same engine.
func (e *Engine) Run(ctx context.Context) (err error) {
	if e.cfg.App == nil {
		panic("runkit: engine is not properly initialized")
	}

	if !e.started.CompareAndSwap(false, true) {
		panic("runkit: engine has already been started")
	}

	driver, err := e.cfg.OpenPersistenceDriver(ctx)
	if err != nil {
		return err
	}
	defer driver.Close()

	wg, ctx := errgroup.WithContext(ctx)

	if e.cfg.ListenAddr != "" {
		listener, advertiseAddrs, err := network.Listen(e.cfg.ListenAddr, e.cfg.AdvertiseAddr)
		if err != nil {
			return err
		}

		defer context.AfterFunc(ctx, func() {
			listener.Close()
		})()

		wg.Go(func() error {
			return e.heartbeat(ctx, driver, advertiseAddrs)
		})

		wg.Go(func() error {
			return e.serve(listener)
		})
	}

	// TODO: remove once there's real goroutines in the group; until then
	// this prevents wg.Wait() from returning immediately.
	wg.Go(func() error {
		<-ctx.Done()
		return ctx.Err()
	})

	e.ready.Set()

	return wg.Wait()
}

func (*Engine) serve(listener net.Listener) error {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return fmt.Errorf("unable to accept connection: %w", err)
		}

		conn.Close()
	}
}

func (e *Engine) heartbeat(ctx context.Context, driver persistencekit.Driver, advertiseAddrs []string) error {
	w := &heartbeat.Writer{
		NodeID:         e.nodeID,
		KVStore:        driver.KVStore(),
		AdvertiseAddrs: advertiseAddrs,
	}

	return w.Run(ctx)
}

// ExecuteCommand routes a command to the appropriate handler within the
// application. See [dogma.CommandExecutor] for detailed semantics.
//
// It blocks until [Engine.Run] is called or ctx is canceled.
func (e *Engine) ExecuteCommand(
	ctx context.Context,
	cmd dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	// TODO: Add Latch.WaitContext()
	if !e.ready.IsSet() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-e.ready.Chan():
		}
	}

	var packOpts []envelopepb.PackCommandOption
	var observers []dogma.EventObserverOption

	for _, opt := range options {
		switch opt := opt.(type) {
		case dogma.IdempotencyKeyOption:
			packOpts = append(packOpts, envelopepb.WithIdempotencyKey(opt.Key()))
		case dogma.EventObserverOption:
			observers = append(observers, opt)
		}
	}

	env := e.packer.PackCommand(cmd, packOpts...)

	sink, ok := e.routes.Get(env.GetBody().GetMessage().GetTypeId())
	if !ok {
		panic(fmt.Sprintf("runkit: no handler registered for %T commands", cmd))
	}

	return sink.ExecuteCommand(ctx, &executionContext{
		Envelope:  env,
		Observers: observers,
	})
}
