package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// MembershipObserver emits notifications about changes to the set of live nodes
// within the cluster.
type MembershipObserver struct {
	Notifications *NotificationListener
	Logger        *slog.Logger

	once          sync.Once
	done          chan struct{}
	sub, unsub    chan chan<- MembershipChange
	subscriptions map[chan<- MembershipChange]int
	nodes         *uuidpb.Map[time.Time]
}

// MembershipChange represents a modification to the set of live nodes in the
// cluster.
type MembershipChange struct {
	// Added is the set of nodes that were added to the cluster.
	Added *uuidpb.Set

	// Removed is the set of nodes that were removed from the cluster.
	Removed *uuidpb.Set

	// Live is the set of nodes that are currently live in the cluster.
	Live *uuidpb.Set
}

// Run polls the heartbeat table until ctx is canceled, sending notifications of
// each change to the set of live nodes.
func (o *MembershipObserver) Run(ctx context.Context) {
	o.init()
	defer close(o.done)

	heartbeats := make(chan string)
	stop := o.Notifications.Subscribe("heartbeat", heartbeats)
	defer stop()

	o.nodes = &uuidpb.Map[time.Time]{}

	for {
		select {
		case <-ctx.Done():
			return
		case ch := <-o.sub:
			o.subscribe(ctx, ch)
		case ch := <-o.unsub:
			o.unsubscribe(ctx, ch)
		case nodeID := <-heartbeats:
			o.update(ctx, nodeID)
		case <-time.After(HeartbeatInterval):
			o.update(ctx)
		}
	}
}

// Subscribe registers ch to receive notifications about changes to the set of
// live nodes.
func (o *MembershipObserver) Subscribe(ch chan<- MembershipChange) func() {
	o.init()

	select {
	case o.sub <- ch:
	case <-o.done:
	}

	var once sync.Once

	return func() {
		once.Do(func() {
			select {
			case <-o.done:
			case o.unsub <- ch:
			}
		})
	}
}

func (o *MembershipObserver) init() {
	o.once.Do(func() {
		o.done = make(chan struct{})
		o.sub = make(chan chan<- MembershipChange)
		o.unsub = make(chan chan<- MembershipChange)
		o.subscriptions = map[chan<- MembershipChange]int{}
		o.nodes = &uuidpb.Map[time.Time]{}
	})
}

func (o *MembershipObserver) subscribe(ctx context.Context, ch chan<- MembershipChange) {
	o.subscriptions[ch]++

	o.Logger.DebugContext(
		ctx,
		"membership subscription added",
		slog.Any("channel", ch),
	)

	if o.subscriptions[ch] == 1 && o.nodes.Len() != 0 {
		live := &uuidpb.Set{}

		for nodeID := range o.nodes.Keys() {
			live.Add(nodeID)
		}

		o.unicast(ctx, ch, MembershipChange{
			Added: live,
			Live:  live,
		})
	}
}

func (o *MembershipObserver) unsubscribe(ctx context.Context, ch chan<- MembershipChange) {
	o.subscriptions[ch]--

	o.Logger.DebugContext(
		ctx,
		"membership subscription removed",
		slog.Any("channel", ch),
	)

	if o.subscriptions[ch] == 0 {
		delete(o.subscriptions, ch)
	}
}

func (o *MembershipObserver) update(ctx context.Context, nodeIDs ...string) {
	now := time.Now()
	added := &uuidpb.Set{}
	removed := &uuidpb.Set{}
	live := &uuidpb.Set{}

	for _, s := range nodeIDs {
		nodeID, err := uuidpb.Parse(s)
		if err != nil {
			o.Logger.WarnContext(
				ctx,
				"ignored invalid node ID in heartbeat notification",
				slog.String("node_id", s),
				slog.String("error", err.Error()),
			)
			continue
		}

		exists := o.nodes.Has(nodeID)
		o.nodes.Set(nodeID, now)

		if !exists {
			added.Add(nodeID)

			o.Logger.InfoContext(
				ctx,
				"node added to cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	for nodeID, lastHeartbeat := range o.nodes.All() {
		if now.Sub(lastHeartbeat) > HeartbeatTimeout {
			o.nodes.Delete(nodeID)
			removed.Add(nodeID)

			o.Logger.InfoContext(
				ctx,
				"node removed from cluster",
				xslog.UUID("node_id", nodeID),
			)
		} else {
			live.Add(nodeID)
		}
	}

	if added.Len()+removed.Len() == 0 {
		return
	}

	o.broadcast(ctx, MembershipChange{
		Added:   added,
		Removed: removed,
		Live:    live,
	})
}

// unicast sends a change notification to a single subscriber channel.
func (o *MembershipObserver) unicast(ctx context.Context, ch chan<- MembershipChange, change MembershipChange) {
	select {
	case <-ctx.Done():
	case ch <- change:
	}
}

// broadcast sends a change notification to all subscriber channels.
func (o *MembershipObserver) broadcast(ctx context.Context, change MembershipChange) {
	var group sync.WaitGroup

	for ch := range o.subscriptions {
		group.Go(func() {
			o.unicast(ctx, ch, change)
		})
	}

	group.Wait()
}
