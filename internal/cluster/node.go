package cluster

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"uuid"

	"github.com/dogmatiq/cosyne"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/cluster/internal/heartbeatpb"
	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/durationpb"
)

// heartbeatBufferSize is the number of heartbeats that can be buffered before
// any are dropped.
const heartbeatBufferSize = 64

// Node represents this engine's node in the cluster.
//
// It keeps track of which nodes are live, and delivers cluster-wide signals.
// See [Node.LiveNodes] and [Node.AttachSignal].
type Node struct {
	// DB is the database whose signals and heartbeats the node sends and
	// listens for.
	DB *sql.DB

	// ID is the ID of this node, which must be unique within the cluster.
	ID uuid.UUID

	// HeartbeatInterval is the time between the heartbeats that this node
	// sends.
	HeartbeatInterval time.Duration

	// HeartbeatTimeout is how long each heartbeat that this node sends keeps
	// this node live. A peer that receives the heartbeat counts that time from
	// the moment of receipt, and removes this node from its live nodes if no
	// newer heartbeat from this node arrives within that time.
	//
	// This node sends the timeout in each heartbeat, as its expiry.
	//
	// HeartbeatTimeout must be longer than [Node.HeartbeatInterval]. Heartbeats
	// can be dropped, so the timeout should span several intervals.
	HeartbeatTimeout time.Duration

	// EngineFailurePolicy is how the engine responds to failures to listen for signals.
	EngineFailurePolicy failure.Policy

	// EngineLogger is the target for log messages produced by the node.
	EngineLogger *slog.Logger

	// liveNodes is the set of IDs of the nodes that this node sees as live.
	liveNodes cosyne.Observable[*uuidset.SetR]

	// signalAttachmentsM guards signalAttachments.
	signalAttachmentsM sync.Mutex

	// signalAttachments maps each cluster-wide signal to the attachments that
	// [Node.AttachSignal] made for it.
	signalAttachments map[string]map[*signalAttachment]struct{}

	// heartbeats maps the ID of each live node to the time at which the most
	// recent heartbeat from that node expires. It is set when the node starts
	// exchanging heartbeats.
	heartbeats map[uuid.UUID]time.Time

	// heartbeatPayloads receives the payload of each notification on the
	// heartbeat channel. [Node.listen] sends to it, and
	// [Node.exchangeHeartbeats] receives from it. It is set when [Node.Run]
	// starts.
	heartbeatPayloads chan string

	// listening receives a value each time [Node.listen] has started listening
	// for both signals and heartbeats. It is set when [Node.Run] starts.
	listening chan struct{}

	// lastSendErr explains why the most recent attempt to send this node's
	// heartbeat failed, or is nil if it was sent.
	lastSendErr error

	// heardOwnHeartbeatAt is the time this node last heard its own heartbeat,
	// or zero if it never has.
	heardOwnHeartbeatAt time.Time
}

// LiveNodes returns an observable set containing the ID of each live node.
//
// A node is live until the expiry in its most recent heartbeat has passed. That
// includes this node, which is live only while its own heartbeats arrive.
//
// The set is empty until the first heartbeat arrives, and once this node has
// stopped.
func (n *Node) LiveNodes() cosyne.ObservableR[*uuidset.SetR] {
	return &n.liveNodes
}

// OwnsWorkload returns true if, among the given live nodes, this node owns the
// workload with the given key.
func (n *Node) OwnsWorkload(key uuid.UUID, liveNodes *uuidset.SetR) bool {
	return ownsWorkload(n.ID, key, liveNodes)
}

// isSelf returns true if the given ID is this node's ID.
func (n *Node) isSelf(id uuid.UUID) bool {
	return id == n.ID
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

	n.heartbeatPayloads = make(chan string, heartbeatBufferSize)
	n.listening = make(chan struct{}, 1)

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
	n.heartbeats = map[uuid.UUID]time.Time{}

	ticker := time.NewTicker(n.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-n.listening:
			// This node never receives the heartbeats that its peers sent
			// before it started listening, so it doesn't yet know which peers
			// are live. It sends a heartbeat now, and both this node and its
			// peers receive it. A peer that hasn't seen this node before
			// replies at once, and the reply tells this node that the peer is
			// live.
			n.sendHeartbeat(ctx)

		case payload := <-n.heartbeatPayloads:
			if n.handleHeartbeat(ctx, payload) {
				n.publishLiveNodes()
			}

		case <-ticker.C:
			n.sendHeartbeat(ctx)

			changed := n.drainHeartbeats(ctx)

			if n.expireNodes(ctx) {
				changed = true
			}

			if changed {
				n.publishLiveNodes()
			}
		}
	}
}

// handleHeartbeat handles the payload of a notification received on the
// heartbeat channel. It returns true if the set of live nodes changed.
func (n *Node) handleHeartbeat(ctx context.Context, payload string) bool {
	h, err := decodeHeartbeat(payload)
	if err != nil {
		n.EngineLogger.WarnContext(
			ctx,
			"ignored invalid heartbeat",
			xslog.Error(err),
		)
		return false
	}

	changed, peerJoined := n.recordHeartbeat(ctx, h)

	// A peer that has just joined does not know about this node yet, so reply
	// with a heartbeat straight away.
	if peerJoined {
		n.sendHeartbeat(ctx)
	}

	return changed
}

// drainHeartbeats handles the heartbeats already waiting to be handled, without
// waiting for more.
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
// It returns true if the set of live nodes changed.
func (n *Node) drainHeartbeats(ctx context.Context) (changed bool) {
	for {
		select {
		case payload := <-n.heartbeatPayloads:
			if n.handleHeartbeat(ctx, payload) {
				changed = true
			}

		default:
			return changed
		}
	}
}

// sendHeartbeat sends this node's heartbeat.
//
// A heartbeat that can not be sent is retried on the next tick, so that once
// the database recovers this node is silent for no longer than one interval,
// plus the time that the failed send took.
func (n *Node) sendHeartbeat(ctx context.Context) {
	err := n.notifyHeartbeat(ctx)
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

// notifyHeartbeat sends this node's heartbeat as a PostgreSQL notification.
func (n *Node) notifyHeartbeat(ctx context.Context) error {
	payload, err := encodeHeartbeat(
		heartbeatpb.NewHeartbeatBuilder().
			WithNodeId(uuidpb.FromStd(n.ID)).
			WithExpiresAfter(durationpb.New(n.HeartbeatTimeout)).
			Build(),
	)
	if err != nil {
		return err
	}

	// The send goes through the connection pool, so it can block for as long as
	// the network to the database lets it. A heartbeat that takes longer than
	// the timeout to send arrives after the other nodes have already removed
	// this node, so the send gives up then.
	//
	// The deadline is the timeout, not the interval, because the send may have
	// to open a connection first. Opening one can take longer than an interval,
	// and the pool discards a connection when the deadline ends its opening, so
	// a deadline shorter than that would stop any heartbeat from being sent.
	ctx, cancel := context.WithTimeout(ctx, n.HeartbeatTimeout)
	defer cancel()

	_, err = n.DB.ExecContext(
		ctx,
		`SELECT pg_notify($1, $2)`,
		heartbeatChannel,
		payload,
	)

	return err
}

// recordHeartbeat records a valid heartbeat.
//
// changed is true if the sender was not already live. peerJoined is true if, in
// addition, it is a node other than this one.
func (n *Node) recordHeartbeat(ctx context.Context, h *heartbeatpb.Heartbeat) (changed, peerJoined bool) {
	nodeID := h.GetNodeId().AsStd()

	// The expiry is counted from now, not from the time at which the sender sent
	// the heartbeat, because the sender's clock may differ from this node's.
	now := time.Now()
	_, exists := n.heartbeats[nodeID]
	n.heartbeats[nodeID] = now.Add(h.GetExpiresAfter().AsDuration())

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

// expireNodes removes the nodes whose most recent heartbeat has expired. It
// returns true if any node was removed.
func (n *Node) expireNodes(ctx context.Context) (changed bool) {
	now := time.Now()

	for nodeID, expiresAt := range n.heartbeats {
		if !now.After(expiresAt) {
			continue
		}

		delete(n.heartbeats, nodeID)
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
				slog.Time("last_heartbeat_at", n.heardOwnHeartbeatAt),
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
	liveNodes := &uuidset.Set{}

	for nodeID := range n.heartbeats {
		liveNodes.Add(nodeID)
	}

	n.liveNodes.Store(liveNodes.R())
}
