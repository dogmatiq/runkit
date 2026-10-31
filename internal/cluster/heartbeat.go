package cluster

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/enginekit/x/xsync"
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

// Heartbeat sends heartbeat signals on behalf of this node, and receives them
// from every node in the cluster to build a view of the live nodes.
//
// Heartbeat delivery is lossy. A node is only removed from the view after the
// heartbeat timeout, which spans several heartbeats, so dropped heartbeats are
// tolerated.
type Heartbeat struct {
	// NodeID is the ID of this node.
	NodeID *uuidpb.UUID

	// DB is the database connection used to send heartbeat signals.
	DB *sql.DB

	// Signals is the listener used to receive heartbeat signals.
	Signals *signal.Listener

	// Interval is the time between the heartbeats sent by each node.
	Interval time.Duration

	// Timeout is the amount of time a node can go without sending a heartbeat
	// before it is considered offline.
	Timeout time.Duration

	// Logger is the target for log messages produced by the heartbeat.
	Logger *slog.Logger

	live   xsync.Observable[*uuidpb.Set]
	beatAt uuidpb.Map[time.Time]
}

// Membership returns the IDs of the nodes that are currently live in the
// cluster, and a channel that is closed when the set changes.
//
// This node is absent from the set until it has received its own heartbeat, and
// whenever it can not hear heartbeats.
//
// The returned set must not be modified.
func (h *Heartbeat) Membership() (*uuidpb.Set, <-chan struct{}) {
	return h.live.Load()
}

// Run sends and receives heartbeats until ctx is canceled.
//
// It must be called at most once.
func (h *Heartbeat) Run(ctx context.Context) error {
	if h.Interval <= 0 {
		panic("heartbeat interval must be positive")
	}

	if h.Timeout <= 0 {
		panic("heartbeat timeout must be positive")
	}

	heartbeats := make(chan string, heartbeatBufferSize)
	unsubscribe := h.Signals.Subscribe(heartbeatTopic, heartbeats)
	defer unsubscribe()

	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()

	h.send(ctx)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case nodeID := <-heartbeats:
			// A signal on the heartbeat topic itself carries no node ID, and
			// so is not a heartbeat.
			if nodeID == "" {
				continue
			}

			// A node that has just joined does not know about this one yet,
			// so reply with a heartbeat straight away.
			if h.update(ctx, nodeID) {
				h.send(ctx)
			}

		case <-ticker.C:
			h.send(ctx)
			h.update(ctx)
		}
	}
}

// send sends this node's heartbeat.
//
// A heartbeat that can not be sent is retried on the next tick, so that once
// the database recovers this node is silent for no longer than one interval.
func (h *Heartbeat) send(ctx context.Context) {
	topic := heartbeatTopic + "." + h.NodeID.AsString()

	if err := signal.Notify(ctx, h.DB, topic); err != nil && ctx.Err() == nil {
		h.Logger.WarnContext(
			ctx,
			"unable to send heartbeat",
			xslog.Error(err),
		)
	}
}

// update updates the set of live nodes based on the received heartbeat
// signals and the heartbeat timeout.
//
// nodeIDs is a variadic list of node IDs that have sent heartbeat signals.
//
// It returns true if a node other than this one has joined the cluster.
func (h *Heartbeat) update(ctx context.Context, nodeIDs ...string) (joined bool) {
	now := time.Now()
	changed := false

	for _, s := range nodeIDs {
		nodeID, err := uuidpb.Parse(s)
		if err != nil {
			h.Logger.WarnContext(
				ctx,
				"ignored invalid node ID in heartbeat signal",
				slog.String("node_id", s),
				xslog.Error(err),
			)
			continue
		}

		exists := h.beatAt.Has(nodeID)
		h.beatAt.Set(nodeID, now)

		if !exists {
			changed = true
			joined = joined || !nodeID.Equal(h.NodeID)

			h.Logger.DebugContext(
				ctx,
				"node added to cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	for nodeID, beatAt := range h.beatAt.All() {
		if now.Sub(beatAt) > h.Timeout {
			h.beatAt.Delete(nodeID)
			changed = true

			h.Logger.DebugContext(
				ctx,
				"node removed from cluster",
				xslog.UUID("node_id", nodeID),
			)
		}
	}

	if !changed {
		return joined
	}

	// Consumers may still hold the previous set, so a new one is published
	// rather than modifying it.
	live := uuidpb.NewSet()

	for nodeID := range h.beatAt.Keys() {
		live.Add(nodeID)
	}

	h.live.Store(live)

	return joined
}
