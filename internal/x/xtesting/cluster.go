package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/spruce"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
)

// Cluster is a set of engines that run the same application against the same
// database.
type Cluster struct {
	// Engines are the engines in the cluster.
	Engines []*runkit.Engine

	stop    context.CancelFunc
	group   *errgroup.Group
	stopped xsync.Latch
}

// StartCluster runs n engines for app against db, and waits for them to
// converge into a cluster.
//
// Each engine listens for gRPC requests on a random port of the loopback
// interface. The options are applied after the defaults, so they may override
// them.
//
// The engines run until [Cluster.Stop] is called or the test ends. If any
// engine stops before then, the test fails.
func StartCluster(
	t testing.TB,
	db *sql.DB,
	app dogma.Application,
	n int,
	options ...runkit.EngineOption,
) *Cluster {
	t.Helper()

	ctx, stop := context.WithCancel(t.Context())
	group, ctx := errgroup.WithContext(ctx)

	c := &Cluster{
		stop:  stop,
		group: group,
	}
	t.Cleanup(c.Stop)

	logger := spruce.NewTestLogger(t)
	nodes := make(chan contexthook.Node, n)

	for idx := range n {
		e := runkit.New(
			app,
			db,
			append(
				[]runkit.EngineOption{
					runkit.WithProjectionCompactInterval(ProjectionCompactInterval),
					runkit.WithListenAddress("127.0.0.1:0"),
					runkit.WithLogger(logger.With("engine", idx)),
				},
				options...,
			)...,
		)

		c.Engines = append(c.Engines, e)

		group.Go(func() error {
			defer c.stopped.Set()

			ctx := contexthook.With(
				ctx,
				func(n contexthook.Node) {
					nodes <- n
				},
			)

			err := e.Run(ctx)

			if !errors.Is(err, context.Canceled) {
				t.Errorf("engine %d stopped unexpectedly: %v", idx, err)
			}

			return err
		})
	}

	for _, e := range c.Engines {
		select {
		case <-ctx.Done():
		case <-e.Ready():
		}
	}

	// Wait for every engine to see all of the others, so that the test starts
	// from a cluster that has converged.
	convergeContext, cancelConverge := context.WithTimeout(ctx, WaitTimeout)
	defer cancelConverge()

	for range n {
		var node contexthook.Node

		select {
		case <-ctx.Done():
		case <-convergeContext.Done():
			t.Fatal("timeout waiting for engines to join the cluster")
		case node = <-nodes:
		}

		for node.Node != nil {
			peers, changed := node.Node.Peers()
			if peers.Len() == n-1 {
				break
			}

			select {
			case <-ctx.Done():
				node.Node = nil
			case <-convergeContext.Done():
				t.Fatalf("timeout waiting for engines to see each other: %d of %d peers", peers.Len(), n-1)
			case <-changed:
			}
		}
	}

	return c
}

// Dial returns an insecure gRPC client connection that reaches any of the
// engines in the cluster, as a single connection to a remote engine would. The
// connection is closed when the test ends.
func (c *Cluster) Dial(t testing.TB) *grpc.ClientConn {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var addresses []resolver.Address

	for _, e := range c.Engines {
		addr, err := e.ListenAddr(ctx)
		if err != nil {
			t.Fatalf("unable to get the engine's listen address: %v", err)
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

// Stop stops the engines and waits for them to return.
func (c *Cluster) Stop() {
	c.stop()
	c.group.Wait()
}

// Stopped returns a channel that is closed when any of the engines stops.
func (c *Cluster) Stopped() <-chan struct{} {
	return c.stopped.Chan()
}
