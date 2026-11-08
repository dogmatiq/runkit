package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
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

	// EngineLogger is the target for log messages produced by the node.
	EngineLogger *slog.Logger

	// membership is this node's view of the live nodes in the cluster.
	membership cosyne.Observable[*Membership]

	// heartbeats maps the ID of each live node to the time its most recent
	// heartbeat was received.
	heartbeats uuidcollection.Map[time.Time]

	// lastSendErr explains why the most recent attempt to send this node's
	// heartbeat failed, or is nil if it was sent.
	lastSendErr error

	// heardOwnHeartbeatAt is the time this node last heard its own heartbeat,
	// or zero if it never has.
	heardOwnHeartbeatAt time.Time
}

// Membership returns an observable of this node's view of the live nodes in
// the cluster.
//
// The membership is nil until this node first hears a heartbeat, and has no
// live nodes once this node has stopped.
func (n *Node) Membership() cosyne.ObservableR[*Membership] {
	return &n.membership
}

// isSelf returns true if the given ID is this node's ID.
func (n *Node) isSelf(id *uuidpb.UUID) bool {
	return id.Equal(n.ID)
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

			if n.handleHeartbeat(ctx, nodeID) {
				n.publishMembershipChange()
			}

		case <-ticker.C:
			n.sendHeartbeat(ctx)

			changed, ok := n.drainHeartbeats(ctx, heartbeats)
			if !ok {
				return ctx.Err()
			}

			if n.expireNodes(ctx) {
				changed = true
			}

			if changed {
				n.publishMembershipChange()
			}
		}
	}
}

// handleHeartbeat handles a signal received on the heartbeat topic. It returns
// true if the set of live nodes changed.
func (n *Node) handleHeartbeat(ctx context.Context, nodeID string) bool {
	// A signal on the heartbeat topic itself carries no node ID, and so is not
	// a heartbeat.
	if nodeID == "" {
		return false
	}

	changed, peerJoined := n.recordHeartbeat(ctx, nodeID)

	// A peer that has just joined does not know about this node yet, so reply
	// with a heartbeat straight away.
	if peerJoined {
		n.sendHeartbeat(ctx)
	}

	return changed
}

// drainHeartbeats handles the heartbeats already waiting in the given channel,
// without waiting for more.
//
// Heartbeats keep arriving while this node is held up, such as while
// sendHeartbeat waits on a slow database, and each is recorded at the time it
// is read. They are read before expiring nodes, so that a hold-up longer than
// the heartbeat timeout does not remove nodes whose heartbeats are waiting.
//
// This is not covered by tests. A node removed this way is added again by the
// next heartbeat read, which is too soon for a test to observe reliably through
// the membership.
//
// changed is true if the set of live nodes changed. ok is false if the channel
// is closed.
func (n *Node) drainHeartbeats(ctx context.Context, heartbeats <-chan string) (changed, ok bool) {
	for {
		select {
		case nodeID, open := <-heartbeats:
			if !open {
				return changed, false
			}

			if n.handleHeartbeat(ctx, nodeID) {
				changed = true
			}

		default:
			return changed, true
		}
	}
}

// sendHeartbeat sends this node's heartbeat.
//
// A heartbeat that can not be sent is retried on the next tick, so that once
// the database recovers this node is silent for no longer than one interval.
func (n *Node) sendHeartbeat(ctx context.Context) {
	topic := heartbeatTopic + "." + n.ID.AsString()

	err := n.Signals.Send(ctx, topic)
	if err == nil {
		n.lastSendErr = nil
		return
	}

	// A failed send is logged only at DEBUG. If sends keep failing, this node
	// stops hearing its own heartbeat, and expireNodes logs that at ERROR with
	// this error, which says what failed.
	n.lastSendErr = fmt.Errorf("unable to send heartbeat: %w", err)

	if ctx.Err() == nil {
		n.EngineLogger.DebugContext(
			ctx,
			"unable to send heartbeat",
			xslog.Error(err),
		)
	}
}

// recordHeartbeat records a heartbeat from the node with the given ID.
//
// changed is true if the node was not already live. peerJoined is true if, in
// addition, it is a node other than this one.
func (n *Node) recordHeartbeat(ctx context.Context, id string) (changed, peerJoined bool) {
	nodeID, err := uuidpb.Parse(id)
	if err != nil {
		n.EngineLogger.WarnContext(
			ctx,
			"ignored invalid node ID in heartbeat signal",
			slog.String("node_id", id),
			xslog.Error(err),
		)
		return false, false
	}

	now := time.Now()
	exists := n.heartbeats.Has(nodeID)
	n.heartbeats.Set(nodeID, now)

	if !n.isSelf(nodeID) {
		if exists {
			return false, false
		}

		n.EngineLogger.DebugContext(
			ctx,
			"node added to cluster",
			xslog.UUID("node_id", nodeID),
		)

		return true, true
	}

	previous := n.heardOwnHeartbeatAt
	n.heardOwnHeartbeatAt = now

	if exists {
		return false, false
	}

	if previous.IsZero() {
		n.EngineLogger.DebugContext(
			ctx,
			"local node joined cluster",
		)
	} else {
		n.EngineLogger.InfoContext(
			ctx,
			"local node rejoined cluster",
			slog.Time("last_heartbeat_at", previous),
		)
	}

	return true, false
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

		if n.isSelf(nodeID) {
			err := n.lastSendErr
			if err == nil {
				err = errors.New("no heartbeat received from the local node")
			}

			n.EngineLogger.ErrorContext(
				ctx,
				"local node removed from cluster",
				slog.Duration("timeout", n.Timeout),
				slog.Time("last_heartbeat_at", lastBeatAt),
				xslog.Error(err),
			)
		} else {
			n.EngineLogger.DebugContext(
				ctx,
				"node removed from cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
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
		if n.isSelf(nodeID) {
			m.isLive = true
		} else {
			m.peers.Add(nodeID)
		}
	}

	n.membership.Store(m)
}
