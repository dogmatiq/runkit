package cluster

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

const (
	// heartbeatTopic is the signal topic beneath which each node sends its
	// heartbeats, on a topic named after its node ID.
	heartbeatTopic = "heartbeat"

	// heartbeatBufferSize is the number of heartbeat signals that can be
	// buffered before any are dropped.
	heartbeatBufferSize = 64
)

// Node represents this engine's node in the cluster. It sends heartbeat signals
// on behalf of this node, and receives them from every node in the cluster to
// build a view of the live nodes in the cluster.
//
// Heartbeat delivery is lossy. A node is only removed from the view after the
// heartbeat timeout, which spans several heartbeats, so dropped heartbeats are
// tolerated.
type Node struct {
	// ID is the ID of this node.
	ID *uuidpb.UUID

	// Signals is the bus used to send and receive heartbeat signals.
	Signals *signal.Bus

	// Interval is the time between the heartbeats sent by each node.
	Interval time.Duration

	// Timeout is the amount of time a node can go without sending a heartbeat
	// before it is considered offline.
	Timeout time.Duration

	// Logger is the target for log messages produced by the node.
	Logger *slog.Logger

	membership xsync.Observable[*Membership]
	heartbeats uuidcollection.Map[time.Time]
}

// Membership returns an observable of this node's view of the live nodes in
// the cluster.
//
// The membership is nil until this node first hears a heartbeat, and has no
// live nodes once this node has stopped.
func (n *Node) Membership() xsync.ObservableR[*Membership] {
	return &n.membership
}

// Run sends and receives heartbeats, and publishes the membership as it
// changes, until ctx is canceled.
//
// It must be called at most once.
func (n *Node) Run(ctx context.Context) error {
	if n.Interval <= 0 {
		panic("heartbeat interval must be positive")
	}

	if n.Timeout <= n.Interval {
		panic("heartbeat timeout must be greater than the heartbeat interval")
	}

	// Once this node has stopped, it is no longer responsible for any
	// workload.
	defer n.membership.Store(&Membership{})

	heartbeats, stop := n.Signals.Watch(ctx, heartbeatTopic, heartbeatBufferSize)
	defer stop()

	ticker := time.NewTicker(n.Interval)
	defer ticker.Stop()

	n.sendHeartbeat(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case nodeID, ok := <-heartbeats:
			if !ok {
				return ctx.Err()
			}

			// A signal on the heartbeat topic itself carries no node ID, and
			// so is not a heartbeat.
			if nodeID == "" {
				continue
			}

			changed, joined := n.recordHeartbeat(ctx, nodeID)
			if changed {
				n.publishMembershipChange()
			}

			// A node that has just joined does not know about this one yet,
			// so reply with a heartbeat straight away.
			if joined {
				n.sendHeartbeat(ctx)
			}

		case <-ticker.C:
			n.sendHeartbeat(ctx)

			if n.expireNodes(ctx) {
				n.publishMembershipChange()
			}
		}
	}
}

// sendHeartbeat sends this node's heartbeat.
//
// A heartbeat that can not be sent is retried on the next tick, so that once
// the database recovers this node is silent for no longer than one interval.
func (n *Node) sendHeartbeat(ctx context.Context) {
	topic := heartbeatTopic + "." + n.ID.AsString()

	if err := n.Signals.Send(ctx, topic); err != nil && ctx.Err() == nil {
		n.Logger.WarnContext(
			ctx,
			"unable to send heartbeat",
			xslog.Error(err),
		)
	}
}

// recordHeartbeat records a heartbeat from the node with the given ID.
//
// changed is true if the node was not already live. joined is true if, in
// addition, it is a node other than this one.
func (n *Node) recordHeartbeat(ctx context.Context, id string) (changed, joined bool) {
	nodeID, err := uuidpb.Parse(id)
	if err != nil {
		n.Logger.WarnContext(
			ctx,
			"ignored invalid node ID in heartbeat signal",
			slog.String("node_id", id),
			xslog.Error(err),
		)
		return false, false
	}

	exists := n.heartbeats.Has(nodeID)
	n.heartbeats.Set(nodeID, time.Now())

	if exists {
		return false, false
	}

	n.Logger.DebugContext(
		ctx,
		"node added to cluster",
		xslog.UUID("node_id", nodeID),
	)

	return true, !nodeID.Equal(n.ID)
}

// expireNodes removes the nodes that have not sent a heartbeat within the
// heartbeat timeout. It returns true if any node was removed.
func (n *Node) expireNodes(ctx context.Context) (changed bool) {
	now := time.Now()

	for nodeID, lastBeatAt := range n.heartbeats.All() {
		if now.Sub(lastBeatAt) <= n.Timeout {
			continue
		}

		n.heartbeats.Delete(nodeID)
		changed = true

		n.Logger.DebugContext(
			ctx,
			"node removed from cluster",
			xslog.UUID("node_id", nodeID),
		)
	}

	return changed
}

// publishMembershipChange publishes the membership after the set of live
// nodes has changed.
func (n *Node) publishMembershipChange() {
	m := &Membership{
		nodeID: n.ID,
		peers:  uuidcollection.NewSet(),
	}

	for nodeID := range n.heartbeats.Keys() {
		if nodeID.Equal(n.ID) {
			m.isLive = true
		} else {
			m.peers.Add(nodeID)
		}
	}

	n.membership.Store(m)
}
