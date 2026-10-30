package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// heartbeatBufferSize is the number of heartbeat signals that can be buffered
// before any are dropped.
const heartbeatBufferSize = 64

// MembershipObserver listens for heartbeat signals from other nodes in the
// cluster to build a view of the live nodes.
//
// Heartbeat delivery is lossy. A node is only removed from the view after the
// heartbeat timeout, which spans several heartbeats, so dropped heartbeats are
// tolerated.
type MembershipObserver struct {
	// Signals is the listener used to receive heartbeat signals from other
	// nodes.
	Signals *signal.Listener

	// HeartbeatInterval is the time between the heartbeats sent by each node.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before it is considered offline.
	HeartbeatTimeout time.Duration

	// Logger is the target for log messages produced by the observer.
	Logger *slog.Logger

	live       xsync.Observable[*uuidpb.Set]
	heartbeats uuidpb.Map[time.Time]
}

// LiveNodeIDs returns the IDs of the nodes that are currently live in the
// cluster, and a channel that is closed when the set changes.
//
// The returned set must not be modified.
func (o *MembershipObserver) LiveNodeIDs() (*uuidpb.Set, <-chan struct{}) {
	return o.live.Load()
}

// Run listens for heartbeat signals and updates the set of live nodes until
// ctx is canceled.
//
// It must be called at most once.
func (o *MembershipObserver) Run(ctx context.Context) error {
	if o.HeartbeatInterval <= 0 {
		panic("heartbeat interval must be positive")
	}

	if o.HeartbeatTimeout <= 0 {
		panic("heartbeat timeout must be positive")
	}

	heartbeats := make(chan string, heartbeatBufferSize)
	unsubscribe := o.Signals.Subscribe(heartbeatTopic, heartbeats)
	defer unsubscribe()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case nodeID := <-heartbeats:
			// A signal on the heartbeat topic itself carries no node ID, and so
			// is not a heartbeat.
			if nodeID != "" {
				o.update(ctx, nodeID)
			}
		case <-time.After(o.HeartbeatInterval):
			o.update(ctx)
		}
	}
}

// update updates the set of live nodes based on the received heartbeat
// signals and the heartbeat timeout.
//
// nodeIDs is a variadic list of node IDs that have sent heartbeat signals.
func (o *MembershipObserver) update(ctx context.Context, nodeIDs ...string) {
	now := time.Now()
	changed := false

	for _, s := range nodeIDs {
		nodeID, err := uuidpb.Parse(s)
		if err != nil {
			o.Logger.WarnContext(
				ctx,
				"ignored invalid node ID in heartbeat signal",
				slog.String("node_id", s),
				xslog.Error(err),
			)
			continue
		}

		exists := o.heartbeats.Has(nodeID)
		o.heartbeats.Set(nodeID, now)

		if !exists {
			changed = true

			o.Logger.DebugContext(
				ctx,
				"node added to cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	for nodeID, lastBeatAt := range o.heartbeats.All() {
		if now.Sub(lastBeatAt) > o.HeartbeatTimeout {
			o.heartbeats.Delete(nodeID)
			changed = true

			o.Logger.DebugContext(
				ctx,
				"node removed from cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	if !changed {
		return
	}

	// Consumers may still hold the previous set, so a new one is published
	// rather than modifying it.
	live := uuidpb.NewSet()

	for nodeID := range o.heartbeats.Keys() {
		live.Add(nodeID)
	}

	o.live.Store(live)
}
