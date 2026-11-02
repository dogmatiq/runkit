package cluster

import (
	"context"
	"database/sql"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/zeebo/xxh3"
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

	// DB is the database connection used to send heartbeat signals.
	DB *sql.DB

	// Signals is the listener used to receive heartbeat signals.
	Signals *signal.Listener

	// Interval is the time between the heartbeats sent by each node.
	Interval time.Duration

	// Timeout is the amount of time a node can go without sending a heartbeat
	// before it is considered offline.
	Timeout time.Duration

	// Logger is the target for log messages produced by the node.
	Logger *slog.Logger

	peers xsync.Observable[*uuidpb.Set]

	once       sync.Once
	observe    chan *observer
	unobserve  chan *observer
	done       xsync.Latch
	heartbeats uuidpb.Map[time.Time]
	observers  uuidpb.Map[map[*observer]struct{}]
}

// observer is a caller's interest in whether this node is responsible for a
// workload, created by [Node.ObserveResponsibility].
type observer struct {
	Workload    *uuidpb.UUID
	Responsible xsync.Observable[bool]
}

// Peers returns the IDs of the other live nodes in the cluster, and a channel
// that is closed when that set changes.
func (n *Node) Peers() (*uuidpb.Set, <-chan struct{}) {
	peers, changed := n.peers.Load()
	return peers.Clone(), changed
}

// ObserveResponsibility observes whether this node is responsible for the
// given rendezvous workload among the live nodes in the cluster.
//
// responsible returns true while this node is responsible for the workload, and
// a channel that is closed when that changes. This node is responsible for no
// workloads while it can not hear its own heartbeats.
//
// unobserve stops observing the workload, after which responsible no longer
// changes.
//
// It blocks until [Node.Run] has been called. If the node has stopped, this
// node is never responsible for the workload.
func (n *Node) ObserveResponsibility(workload *uuidpb.UUID) (
	responsible func() (bool, <-chan struct{}),
	unobserve func(),
) {
	n.init()

	o := &observer{
		Workload: workload,
	}

	select {
	case <-n.done.Chan():
		return o.Responsible.Load, func() {}
	case n.observe <- o:
	}

	var once sync.Once

	return o.Responsible.Load, func() {
		once.Do(func() {
			select {
			case <-n.done.Chan():
			case n.unobserve <- o:
			}
		})
	}
}

func (n *Node) init() {
	n.once.Do(func() {
		n.observe = make(chan *observer)
		n.unobserve = make(chan *observer)
	})
}

// Run sends and receives heartbeats, and reports whether this node is
// responsible for each observed workload, until ctx is canceled.
//
// It must be called at most once.
func (n *Node) Run(ctx context.Context) error {
	if n.Interval <= 0 {
		panic("heartbeat interval must be positive")
	}

	if n.Timeout <= n.Interval {
		panic("heartbeat timeout must be greater than the heartbeat interval")
	}

	n.init()

	defer func() {
		n.done.Set()

		// Once this node has stopped, it is no longer responsible for any
		// workload.
		for observers := range n.observers.Values() {
			for o := range observers {
				xsync.ChangeObservable(&o.Responsible, false)
			}
		}
	}()

	heartbeats := make(chan string, heartbeatBufferSize)
	unsubscribe := n.Signals.Subscribe(heartbeatTopic, heartbeats)
	defer unsubscribe()

	ticker := time.NewTicker(n.Interval)
	defer ticker.Stop()

	n.sendHeartbeat(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case o := <-n.observe:
			n.addObserver(o)

		case o := <-n.unobserve:
			n.removeObserver(o)

		case nodeID := <-heartbeats:
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

	if err := signal.Notify(ctx, n.DB, topic); err != nil && ctx.Err() == nil {
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

// publishMembershipChange updates whether this node is responsible for each
// observed workload after the set of live nodes has changed, and publishes the
// new set of peers if that has changed too.
func (n *Node) publishMembershipChange() {
	for workload, observers := range n.observers.All() {
		responsible := n.isResponsibleFor(workload)

		for o := range observers {
			xsync.ChangeObservable(&o.Responsible, responsible)
		}
	}

	// [Node.Peers] may be cloning the published set on another goroutine, so a
	// new set is built rather than modifying it.
	peers := uuidpb.NewSet()

	for peerID := range n.heartbeats.Keys() {
		if peerID.Equal(n.ID) {
			continue
		}

		peers.Add(peerID)
	}

	if prev, _ := n.peers.Load(); !peers.IsEqual(prev) {
		n.peers.Store(peers)
	}
}

// addObserver adds o to the observers of its workload, and sets whether this
// node is responsible for it.
func (n *Node) addObserver(o *observer) {
	observers, ok := n.observers.Get(o.Workload)
	if !ok {
		observers = map[*observer]struct{}{}
		n.observers.Set(o.Workload, observers)
	}

	observers[o] = struct{}{}

	xsync.ChangeObservable(
		&o.Responsible,
		n.isResponsibleFor(o.Workload),
	)
}

// removeObserver removes o from the observers of its workload.
func (n *Node) removeObserver(o *observer) {
	observers, ok := n.observers.Get(o.Workload)
	if !ok {
		return
	}

	delete(observers, o)

	if len(observers) == 0 {
		n.observers.Delete(o.Workload)
	}
}

// isResponsibleFor returns true if this node is responsible for the given
// workload among the live nodes.
//
// It uses rendezvous hashing: each live node is scored by hashing the workload
// with the node's ID, and the node with the highest score is responsible. Ties
// are broken by the lower ID, so every node reaches the same answer regardless
// of the order in which it holds the live nodes.
//
// This node is responsible for nothing while it can not hear its own
// heartbeats.
func (n *Node) isResponsibleFor(workload *uuidpb.UUID) bool {
	if !n.heartbeats.Has(n.ID) {
		return false
	}

	// The first half of buf holds the workload, and the second half holds the
	// ID of the node being scored.
	var buf [32]byte
	uuidpb.CopyBytes(workload, buf[:16])

	uuidpb.CopyBytes(n.ID, buf[16:])
	ownScore := xxh3.Hash(buf[:])

	for peerID := range n.heartbeats.Keys() {
		if peerID.Equal(n.ID) {
			continue
		}

		uuidpb.CopyBytes(peerID, buf[16:])
		peerScore := xxh3.Hash(buf[:])

		if peerScore > ownScore {
			return false
		}

		if peerScore == ownScore {
			// Break ties by the lower ID, so that every node reaches the same
			// answer regardless of the order in which it holds the live nodes.
			if peerID.Compare(n.ID) < 0 {
				return false
			}
		}
	}

	return true
}
