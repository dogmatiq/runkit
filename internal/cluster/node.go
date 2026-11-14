package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
)

const (
	// heartbeatTopic is the signal topic of every heartbeat. A heartbeat's
	// subtopic is the ID of the node that sent it.
	heartbeatTopic = "heartbeat"

	// heartbeatBufferSize is the number of heartbeat signals that can be
	// buffered before any are dropped.
	heartbeatBufferSize = 64
)

// Node represents this engine's node in the cluster.
//
// It keeps track of which nodes are live, and delivers cluster-wide signals.
// See [Node.LiveNodes] and [Node.AttachSignal].
type Node struct {
	// DB is the database whose signals the node listens for.
	DB *sql.DB

	// Signals is the bus used to send and receive heartbeat signals.
	Signals *signal.Bus

	// ID is the ID of this node, which must be unique within the cluster.
	ID *uuidpb.UUID

	// HeartbeatInterval is the time between the heartbeats that this node
	// sends.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before this node removes it from its live nodes.
	//
	// It must be longer than [Node.HeartbeatInterval]. Heartbeats can be
	// dropped, so it should span several intervals.
	HeartbeatTimeout time.Duration

	// EngineFailurePolicy is how the engine responds to failures to listen for signals.
	EngineFailurePolicy failure.Policy

	// EngineLogger is the target for log messages produced by the node.
	EngineLogger *slog.Logger

	// liveNodes is the set of IDs of the nodes that this node sees as live.
	liveNodes cosyne.Observable[*uuidcollection.Set]

	// signalAttachmentsM guards signalAttachments.
	signalAttachmentsM sync.Mutex

	// signalAttachments maps each cluster-wide signal to the attachments that
	// [Node.AttachSignal] made for it.
	signalAttachments map[string]map[*signalAttachment]struct{}

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

// LiveNodes returns an observable set containing the ID of each live node.
//
// A node is live while its heartbeats keep arriving within
// [Node.HeartbeatTimeout]. That includes this node, which is live only while
// its own heartbeats arrive.
//
// The set is empty until the first heartbeat arrives, and once this node has
// stopped. It must not be modified.
func (n *Node) LiveNodes() cosyne.ObservableR[*uuidcollection.Set] {
	return &n.liveNodes
}

// OwnsWorkload returns true if, among the given live nodes, this node owns the
// workload with the given key.
func (n *Node) OwnsWorkload(key *uuidpb.UUID, liveNodes *uuidcollection.Set) bool {
	return ownsWorkload(n.ID, key, liveNodes)
}

// isSelf returns true if the given ID is this node's ID.
func (n *Node) isSelf(id *uuidpb.UUID) bool {
	return id.Equal(n.ID)
}

// Run listens for signals, sends and receives heartbeats, and publishes the set
// of live nodes as it changes, until ctx is canceled.
//
// It must be called at most once.
func (n *Node) Run(ctx context.Context) error {
	if n.HeartbeatInterval <= 0 {
		panic("heartbeat interval must be positive")
	}

	if n.HeartbeatTimeout <= n.HeartbeatInterval {
		panic("heartbeat timeout must be greater than the heartbeat interval")
	}

	// After this node has stopped, it doesn't own any workloads.
	defer n.liveNodes.Store(nil)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(
		func() error {
			return supervisor.Run(
				ctx,
				"listen for signals",
				n.EngineFailurePolicy,
				n.EngineLogger,
				n.listen,
			)
		},
	)

	g.Go(
		func() error {
			return n.exchangeHeartbeats(ctx)
		},
	)

	return g.Wait()
}

// exchangeHeartbeats sends this node's heartbeats, receives every node's
// heartbeats, and publishes the set of live nodes as it changes, until ctx is
// canceled.
func (n *Node) exchangeHeartbeats(ctx context.Context) error {
	heartbeats, stop := n.Signals.Watch(ctx, heartbeatTopic, heartbeatBufferSize)
	defer stop()

	ticker := time.NewTicker(n.HeartbeatInterval)
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
				n.publishLiveNodes()
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
				n.publishLiveNodes()
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
// [Node.LiveNodes].
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

	// A failed send is logged only as a DEBUG record. If sends keep failing,
	// this node stops hearing its own heartbeat, and expireNodes logs that as
	// an ERROR with this error, which says what failed.
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
		if now.Sub(lastBeatAt) <= n.HeartbeatTimeout {
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
				slog.Duration("heartbeat_timeout", n.HeartbeatTimeout),
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

// publishLiveNodes publishes the set of live nodes after it has changed.
func (n *Node) publishLiveNodes() {
	liveNodes := uuidcollection.NewSet()

	for nodeID := range n.heartbeats.Keys() {
		liveNodes.Add(nodeID)
	}

	n.liveNodes.Store(liveNodes)
}
