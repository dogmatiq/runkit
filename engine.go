package runkit

import (
	"cmp"
	"context"
	"sync/atomic"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/persistencekit/kv"
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
	listenAddr    string
	advertiseAddr string
	persistence   PersistenceProvider
	apps          []dogma.Application // TODO(agent): pick one of slice or map, the set is likely to contain one or 2 elements
	appsByKey     map[string]struct{}
	executors     map[dogma.Application]*executor
	running       atomic.Bool
	kvStore       kv.BinaryStore
	wg            *errgroup.Group
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

	if e.persistence == nil {
		panic("runkit: a persistence provider is required, use WithPersistence()")
	}

	if e.advertiseAddr != "" && e.listenAddr == "" {
		panic("runkit: WithAdvertiseAddress requires WithListenAddress or DOGMA_LISTEN_ADDRESS")
	}

	kvStore, err := e.persistence.NewKVStore(ctx)
	if err != nil {
		return err
	}
	e.kvStore = kvStore

	e.wg, ctx = errgroup.WithContext(ctx)

	if e.listenAddr != "" {
		e.startListener(ctx)
	} else {
		// TODO: remove once startExecutors() adds real goroutines to the group;
		// until then this prevents wg.Wait() from returning immediately.
		e.wg.Go(func() error {
			<-ctx.Done()
			return ctx.Err()
		})
	}
	e.startExecutors()

	return e.wg.Wait()
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

func (e *Engine) startListener(ctx context.Context) {
	e.wg.Go(func() error {
		listenAddr := cmp.Or(e.listenAddr, defaultListenAddr)
		lis := &stubListener{listenAddr: listenAddr}

		addr, err := lis.Listen()
		if err != nil {
			return err
		}

		addrs, err := resolveAdvertiseAddrs(addr, listenAddr, e.advertiseAddr)
		if err != nil {
			return err
		}

		// Start the heartbeat writer now that we know which addresses to
		// advertise. errgroup.Go is safe to call from within a running goroutine.
		e.startHeartbeatWriter(ctx, addrs)

		return lis.Serve(ctx)
	})
}

func (e *Engine) startHeartbeatWriter(ctx context.Context, advertiseAddrs []string) {
	e.wg.Go(func() error {
		w := &heartbeat.Writer{
			NodeID:         e.nodeID,
			KVStore:        e.kvStore,
			AdvertiseAddrs: advertiseAddrs,
		}
		return w.Run(ctx)
	})
}

func (e *Engine) startExecutors() {
	for _, ex := range e.executors {
		ex.future.Store(noopExecutor{})
	}
}
