package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/spruce"
)

// Cluster is a set of nodes that run the same application against the same
// database, which together form a single logical engine.
//
// It implements [dogma.CommandExecutor] by executing each command on one of
// its running nodes, chosen at random.
//
// It must only be used from the test's goroutine.
type Cluster struct {
	t       testing.TB
	db      *sql.DB
	app     dogma.Application
	options []runkit.EngineOption
	logger  *slog.Logger

	ctx    context.Context
	stop   context.CancelFunc
	failed xsync.Latch

	members map[*runkit.Engine]*clusterMember
	started int
}

// clusterMember is a running node in a [Cluster].
type clusterMember struct {
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
		members: map[*runkit.Engine]*clusterMember{},
	}
	t.Cleanup(c.Stop)

	for range n {
		c.start()
	}

	c.converge(t)

	return c
}

// Nodes returns the engines running on the cluster's current nodes, in no
// particular order.
func (c *Cluster) Nodes() []*runkit.Engine {
	return slices.Collect(maps.Keys(c.members))
}

// ExecuteCommand executes a command on one of the cluster's running nodes,
// chosen at random.
func (c *Cluster) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	nodes := c.Nodes()
	if len(nodes) == 0 {
		return errors.New("the cluster has no running nodes")
	}

	idx := rand.N(len(nodes))
	return nodes[idx].ExecuteCommand(ctx, command, options...)
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
	m, ok := c.members[e]
	if !ok {
		panic("engine is not running on a node of the cluster")
	}

	delete(c.members, e)

	m.Stop()
	<-m.Stopped
}

// Stop stops every node and waits for their engines to return.
func (c *Cluster) Stop() {
	c.stop()

	for _, m := range c.members {
		<-m.Stopped
	}
}

// Failed returns a channel that is closed if any node stops other than by
// [Cluster.StopNode] or [Cluster.Stop].
func (c *Cluster) Failed() <-chan struct{} {
	return c.failed.Chan()
}

// start starts a new node in the cluster, and returns its engine.
func (c *Cluster) start() *runkit.Engine {
	idx := c.started
	c.started++

	e := runkit.New(
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
	)

	ctx, stop := context.WithCancel(c.ctx)

	m := &clusterMember{
		Stop:    stop,
		Stopped: make(chan struct{}),
	}

	c.members[e] = m

	go func() {
		defer close(m.Stopped)

		ctx := contexthook.With(
			ctx,
			func(n contexthook.Node) {
				m.Node.Store(n.Node)
			},
		)

		if err := e.Run(ctx); !errors.Is(err, context.Canceled) {
			c.t.Errorf("node %d stopped unexpectedly: %v", idx, err)
			c.failed.Set()
		}
	}()

	return e
}

// converge waits for every running node to be ready and to see exactly the
// other running nodes as its peers.
func (c *Cluster) converge(t testing.TB) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var (
		nodes   []*cluster.Node
		nodeIDs = uuidcollection.NewSet()
	)

	for e, m := range c.members {
		select {
		case <-c.failed.Chan():
			return
		case <-ctx.Done():
			t.Fatal("timeout waiting for nodes to become ready")
		case <-e.Ready():
		}

		node, err := m.Node.WaitContext(ctx)
		if err != nil {
			t.Fatal("timeout waiting for nodes to join the cluster")
		}

		nodes = append(nodes, node)
		nodeIDs.Add(node.ID)
	}

	for _, node := range nodes {
		want := nodeIDs.Clone()
		want.Delete(node.ID)

		for {
			membership, changed := node.Membership().Load()
			peers := membership.Peers()

			if want.IsEqual(peers) {
				break
			}

			select {
			case <-c.failed.Chan():
				return
			case <-ctx.Done():
				t.Fatalf("timeout waiting for nodes to see each other: node %s sees peers %s, want %s", node.ID, peers, want)
			case <-changed:
			}
		}
	}
}
