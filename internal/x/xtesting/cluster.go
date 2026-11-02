package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"slices"
	"sync"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/spruce"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// Cluster is a set of nodes that run the same application against the same
// database, which together form a single logical engine.
//
// It implements [dogma.CommandExecutor] by executing each command on one of
// its running nodes, choosing each node in turn.
type Cluster struct {
	t       testing.TB
	db      *sql.DB
	app     dogma.Application
	options []runkit.EngineOption
	logger  *slog.Logger

	ctx     context.Context
	stop    context.CancelFunc
	running sync.WaitGroup
	failed  xsync.Latch

	m       sync.Mutex
	members []*clusterMember
	started int
	next    int
}

var _ dogma.CommandExecutor = (*Cluster)(nil)

// clusterMember is a running node in a [Cluster].
type clusterMember struct {
	// Engine is the engine that runs on the node.
	Engine *runkit.Engine

	// Node is the node's membership of the cluster, which is available once it
	// has joined.
	Node xsync.Future[*cluster.Node]

	// Stop stops the node's engine.
	Stop context.CancelFunc

	// Stopped is closed when the node's engine has stopped.
	Stopped chan struct{}
}

// StartCluster runs app against db on n nodes, and waits for them to converge
// into a cluster.
//
// Each node's engine listens for gRPC requests on a random port of the
// loopback interface. The options are applied after the defaults, so they may
// override them.
//
// The nodes run until they are stopped by [Cluster.StopNode] or
// [Cluster.Stop], or the test ends. If any node stops before then, the test
// fails.
func StartCluster(
	t testing.TB,
	db *sql.DB,
	app dogma.Application,
	n int,
	options ...runkit.EngineOption,
) *Cluster {
	t.Helper()

	ctx, stop := context.WithCancel(t.Context())

	c := &Cluster{
		t:       t,
		db:      db,
		app:     app,
		options: options,
		logger:  spruce.NewTestLogger(t),
		ctx:     ctx,
		stop:    stop,
	}
	t.Cleanup(c.Stop)

	for range n {
		c.start()
	}

	c.converge(t)

	return c
}

// Nodes returns the engines running on the cluster's current nodes, in the
// order in which they were started.
func (c *Cluster) Nodes() []*runkit.Engine {
	c.m.Lock()
	defer c.m.Unlock()

	engines := make([]*runkit.Engine, 0, len(c.members))
	for _, m := range c.members {
		engines = append(engines, m.Engine)
	}

	return engines
}

// ExecuteCommand executes a command on one of the cluster's running nodes,
// choosing each node in turn.
func (c *Cluster) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	c.m.Lock()

	if len(c.members) == 0 {
		c.m.Unlock()
		return errors.New("the cluster has no running nodes")
	}

	m := c.members[c.next%len(c.members)]
	c.next++

	c.m.Unlock()

	return m.Engine.ExecuteCommand(ctx, command, options...)
}

// AddNode starts another node in the cluster, and waits for every node to see
// all of the others. It returns the new node's engine.
func (c *Cluster) AddNode(t testing.TB) *runkit.Engine {
	t.Helper()

	e := c.start()
	c.converge(t)

	return e
}

// StopNode stops the node that runs e, and waits for e to return.
//
// The other nodes continue to run. They see that the node has left the cluster
// only once it has missed its heartbeats.
func (c *Cluster) StopNode(e *runkit.Engine) {
	c.m.Lock()

	idx := slices.IndexFunc(
		c.members,
		func(m *clusterMember) bool {
			return m.Engine == e
		},
	)
	if idx == -1 {
		c.m.Unlock()
		panic("engine is not running on a node of the cluster")
	}

	m := c.members[idx]
	c.members = slices.Delete(c.members, idx, idx+1)

	c.m.Unlock()

	m.Stop()
	<-m.Stopped
}

// start starts a new node in the cluster, and returns its engine.
func (c *Cluster) start() *runkit.Engine {
	c.m.Lock()
	defer c.m.Unlock()

	idx := c.started
	c.started++

	ctx, stop := context.WithCancel(c.ctx)

	m := &clusterMember{
		Engine: runkit.New(
			c.app,
			c.db,
			append(
				[]runkit.EngineOption{
					runkit.WithProjectionCompactInterval(ProjectionCompactInterval),
					runkit.WithListenAddress("127.0.0.1:0"),
					runkit.WithLogger(c.logger.With("node", idx)),
				},
				c.options...,
			)...,
		),
		Stop:    stop,
		Stopped: make(chan struct{}),
	}

	c.members = append(c.members, m)

	c.running.Go(func() {
		defer close(m.Stopped)

		ctx := contexthook.With(
			ctx,
			func(n contexthook.Node) {
				m.Node.Store(n.Node)
			},
		)

		if err := m.Engine.Run(ctx); !errors.Is(err, context.Canceled) {
			c.t.Errorf("node %d stopped unexpectedly: %v", idx, err)
			c.failed.Set()
		}
	})

	return m.Engine
}

// converge waits for every running node to be ready and to see all of the
// others.
func (c *Cluster) converge(t testing.TB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	c.m.Lock()
	members := slices.Clone(c.members)
	c.m.Unlock()

	peerCount := len(members) - 1

	for _, m := range members {
		select {
		case <-c.failed.Chan():
			return
		case <-ctx.Done():
			t.Fatal("timeout waiting for nodes to become ready")
		case <-m.Engine.Ready():
		}

		node, err := m.Node.WaitContext(ctx)
		if err != nil {
			t.Fatal("timeout waiting for nodes to join the cluster")
		}

		for {
			peers, changed := node.Peers()
			if peers.Len() == peerCount {
				break
			}

			select {
			case <-c.failed.Chan():
				return
			case <-ctx.Done():
				t.Fatalf("timeout waiting for nodes to see each other: %d of %d peers", peers.Len(), peerCount)
			case <-changed:
			}
		}
	}
}

// Dial returns an insecure gRPC client connection that reaches any of the
// cluster's running nodes, as a single connection to a remote engine would. It
// does not reach nodes that are added afterwards. The connection is
// closed when the test ends.
func (c *Cluster) Dial(t testing.TB) *grpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var addresses []resolver.Address

	for _, e := range c.Nodes() {
		addr, err := e.ListenAddr(ctx)
		if err != nil {
			t.Fatalf("unable to get the node's listen address: %v", err)
		}

		addresses = append(
			addresses,
			resolver.Address{
				Addr: addr.String(),
			},
		)
	}

	r := manual.NewBuilderWithScheme("xtesting")
	r.InitialState(
		resolver.State{
			Addresses: addresses,
		},
	)

	conn, err := grpc.NewClient(
		r.Scheme()+":///cluster",
		grpc.WithResolvers(r),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig": [{"round_robin": {}}]}`),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("unable to create gRPC client: %v", err)
	}
	t.Cleanup(func() { conn.Close() })

	return conn
}

// Stop stops every node and waits for their engines to return.
func (c *Cluster) Stop() {
	c.stop()
	c.running.Wait()
}

// Failed returns a channel that is closed if any node stops other than by
// [Cluster.StopNode] or [Cluster.Stop].
func (c *Cluster) Failed() <-chan struct{} {
	return c.failed.Chan()
}
