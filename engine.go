package runkit

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/heartbeat"
	"golang.org/x/sync/errgroup"
)

// defaultPort is the default TCP port for the engine listener.
// TODO: replace with an assigned IANA port before shipping.
const defaultPort = 0

// Engine runs one or more Dogma applications.
type Engine struct {
	site          *identitypb.Identity
	nodeID        *uuidpb.UUID
	bindAddr      string
	advertiseAddr string
	persistence   PersistenceProvider
	apps          []dogma.Application // TODO(agent): pick one of slice or map, the set is likely to contain one or 2 elements
	appsByKey     map[string]struct{}
	executors     map[dogma.Application]*executor
	running       atomic.Bool
}

// New returns an [Engine] configured by the given options.
func New(opts ...Option) *Engine {
	e := &Engine{
		appsByKey: map[string]struct{}{},
		executors: map[dogma.Application]*executor{},
	}

	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Run starts the engine and blocks until ctx is canceled or a fatal error
// occurs.
//
// It panics if called more than once on the same engine.
func (e *Engine) Run(ctx context.Context) error {
	if !e.running.CompareAndSwap(false, true) {
		panic("runkit: Run() has already been called")
	}

	if e.site == nil {
		panic("runkit: a site identity is required, use WithSite() or FromEnvironment()")
	}

	if e.persistence == nil {
		panic("runkit: a persistence provider is required, use WithPersistence()")
	}

	if e.nodeID == nil {
		e.nodeID = uuidpb.Generate()
	}

	bindAddr := e.bindAddr
	if bindAddr == "" {
		bindAddr = fmt.Sprintf("0.0.0.0:%d", defaultPort)
	}

	configuredAdvertiseAddr := e.advertiseAddr

	kvStore, err := e.persistence.KVStore(ctx)
	if err != nil {
		return err
	}

	g, gctx := errgroup.WithContext(ctx)

	l := &stubListener{bindAddr: bindAddr, advertiseAddr: configuredAdvertiseAddr}
	addrCh := make(chan string, 1)
	g.Go(func() error {
		return l.ListenAndServe(gctx, func(addr string) { addrCh <- addr })
	})

	var advertiseAddr string
	select {
	case advertiseAddr = <-addrCh:
	case <-gctx.Done():
		if err := g.Wait(); ctx.Err() == nil {
			return err
		}
		return nil
	}

	w := &heartbeat.Writer{
		NodeID:        e.nodeID,
		KVStore:       kvStore,
		AdvertiseAddr: advertiseAddr,
	}
	g.Go(func() error { return w.Run(gctx) })

	for _, ex := range e.executors {
		ex.future.Store(noopExecutor{})
	}

	if err := g.Wait(); ctx.Err() == nil {
		return err
	}
	return nil
}

// noopExecutor is a [dogma.CommandExecutor] that discards all commands. It is
// stored in an [executor]'s future by the Phase 1 Run() stub and replaced with
// real routing logic in Phase 10.
type noopExecutor struct{}

func (noopExecutor) ExecuteCommand(
	context.Context,
	dogma.Command,
	...dogma.ExecuteCommandOption,
) error {
	return nil
}

// ExecutorFor returns a [dogma.CommandExecutor] for app.
//
// Commands executed before [Engine.Run] is called block until the engine
// starts.
//
// It panics if app was not registered with [WithApplication].
func (e *Engine) ExecutorFor(app dogma.Application) dogma.CommandExecutor {
	ex, ok := e.executors[app]
	if !ok {
		panic("runkit: application is not registered with this engine")
	}

	return ex
}
