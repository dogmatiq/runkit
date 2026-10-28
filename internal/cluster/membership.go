package cluster

import (
	"context"
	"log/slog"
	"sync"
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
	// If it is zero, [DefaultHeartbeatInterval] is used.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before it is considered offline. If it is zero,
	// [DefaultHeartbeatTimeout] is used.
	HeartbeatTimeout time.Duration

	// Logger is the target for log messages produced by the observer.
	Logger *slog.Logger

	done       xsync.Latch
	heartbeats uuidpb.Map[time.Time]

	once        sync.Once
	sub, unsub  chan *membershipSubscriber
	subscribers map[*membershipSubscriber]struct{}
}

// MembershipChange represents a change to the set of live nodes in the
// cluster.
type MembershipChange struct {
	// Added is the set of nodes that were added to the cluster.
	Added *uuidpb.Set

	// Removed is the set of nodes that were removed from the cluster.
	Removed *uuidpb.Set

	// Live is the set of nodes that are currently live in the cluster, after
	// the changes have been applied.
	Live *uuidpb.Set
}

// membershipSubscriber encapsulates the state of a single subscriber to
// membership changes.
type membershipSubscriber struct {
	Changes      chan<- MembershipChange
	Unsubscribed xsync.Latch
}

// Run listens for heartbeat signals and updates the set of live nodes until
// ctx is canceled.
func (o *MembershipObserver) Run(ctx context.Context) error {
	o.init()
	defer o.done.Set()

	heartbeats := make(chan string, heartbeatBufferSize)
	unsubscribe := o.Signals.Subscribe(heartbeatTopic, heartbeats)
	defer unsubscribe()

	interval := o.HeartbeatInterval
	if interval == 0 {
		interval = DefaultHeartbeatInterval
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case sub := <-o.sub:
			o.subscribe(ctx, sub)
		case sub := <-o.unsub:
			delete(o.subscribers, sub)
		case nodeID := <-heartbeats:
			// A signal on the heartbeat topic itself carries no node ID, and so
			// is not a heartbeat.
			if nodeID != "" {
				o.update(ctx, nodeID)
			}
		case <-time.After(interval):
			o.update(ctx)
		}
	}
}

// Subscribe registers a channel to receive notifications about changes to the
// set of live nodes in the cluster.
func (o *MembershipObserver) Subscribe(ch chan<- MembershipChange) func() {
	o.init()

	sub := &membershipSubscriber{
		Changes: ch,
	}

	select {
	case <-o.done.Chan():
	case o.sub <- sub:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			sub.Unsubscribed.Set()

			select {
			case <-o.done.Chan():
			case o.unsub <- sub:
			}
		})
	}
}

func (o *MembershipObserver) init() {
	o.once.Do(func() {
		o.sub = make(chan *membershipSubscriber)
		o.unsub = make(chan *membershipSubscriber)
		o.subscribers = map[*membershipSubscriber]struct{}{}
	})
}

// subscribe registers a new subscriber and sends it the current set of live
// nodes.
func (o *MembershipObserver) subscribe(ctx context.Context, sub *membershipSubscriber) {
	o.subscribers[sub] = struct{}{}

	if o.heartbeats.IsEmpty() {
		return
	}

	live := &uuidpb.Set{}

	for nodeID := range o.heartbeats.Keys() {
		live.Add(nodeID)
	}

	o.unicast(ctx, sub, MembershipChange{
		Added: live,
		Live:  live,
	})
}

// update updates the set of live nodes based on the received heartbeat
// signals and the heartbeat timeout.
//
// nodeIDs is a variadic list of node IDs that have sent heartbeat signals.
func (o *MembershipObserver) update(ctx context.Context, nodeIDs ...string) {
	timeout := o.HeartbeatTimeout
	if timeout == 0 {
		timeout = DefaultHeartbeatTimeout
	}

	now := time.Now()
	var added, removed, live uuidpb.Set

	for _, s := range nodeIDs {
		nodeID, err := uuidpb.Parse(s)
		if err != nil {
			o.Logger.WarnContext(
				ctx,
				"ignored invalid node ID in heartbeat signal",
				slog.String("node_id", s),
				slog.String("error", err.Error()),
			)
			continue
		}

		exists := o.heartbeats.Has(nodeID)
		o.heartbeats.Set(nodeID, now)

		if !exists {
			added.Add(nodeID)

			o.Logger.DebugContext(
				ctx,
				"node added to cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	for nodeID, lastBeatAt := range o.heartbeats.All() {
		if now.Sub(lastBeatAt) > timeout {
			o.heartbeats.Delete(nodeID)
			removed.Add(nodeID)

			o.Logger.DebugContext(
				ctx,
				"node removed from cluster",
				xslog.UUID("node_id", nodeID),
			)
		} else {
			live.Add(nodeID)
		}
	}

	if added.IsEmpty() && removed.IsEmpty() {
		return
	}

	o.broadcast(ctx, MembershipChange{
		Added:   &added,
		Removed: &removed,
		Live:    &live,
	})
}

// unicast sends a change notification to a single subscriber channel.
func (o *MembershipObserver) unicast(ctx context.Context, sub *membershipSubscriber, change MembershipChange) {
	select {
	case <-ctx.Done():
	case <-sub.Unsubscribed.Chan():
	case sub.Changes <- change:
	}
}

// broadcast sends a change notification to all subscriber channels.
func (o *MembershipObserver) broadcast(ctx context.Context, change MembershipChange) {
	var group sync.WaitGroup

	for sub := range o.subscribers {
		group.Go(func() {
			o.unicast(ctx, sub, change)
		})
	}

	group.Wait()
}
