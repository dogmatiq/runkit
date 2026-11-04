package cluster

import (
	"context"
	"log/slog"
	"sync"
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

	once       sync.Once
	observe    chan *observer
	unobserve  chan *observer
	done       xsync.Latch
	heartbeats uuidcollection.Map[time.Time]
	observers  uuidcollection.Map[map[*observer]struct{}]
}

// observer is a caller's interest in whether this node is responsible for a
// workload, created by [Node.ObserveResponsibility].
type observer struct {
	Workload    *uuidpb.UUID
	Responsible xsync.Observable[bool]
}

// Membership returns an observable of this node's view of the live nodes in
// the cluster.
//
// The membership is nil until this node first hears a heartbeat, and has no
// live nodes once this node has stopped.
func (n *Node) Membership() xsync.ObservableR[*Membership] {
	return &n.membership
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
		n.membership.Store(&Membership{})

		for observers := range n.observers.Values() {
			for o := range observers {
				xsync.ChangeObservable(&o.Responsible, false)
			}
		}
	}()

	heartbeats := n.Signals.Watch(ctx, heartbeatTopic, heartbeatBufferSize)

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
// nodes has changed, and updates whether this node is responsible for each
// observed workload.
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

	for workload, observers := range n.observers.All() {
		responsible := m.LocalNodeIsResponsibleFor(workload)

		for o := range observers {
			xsync.ChangeObservable(&o.Responsible, responsible)
		}
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

	m, _ := n.membership.Load()

	xsync.ChangeObservable(
		&o.Responsible,
		m.LocalNodeIsResponsibleFor(o.Workload),
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
