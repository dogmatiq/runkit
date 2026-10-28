package cluster

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
)

const (
	// HeartbeatInterval is the time between heartbeats.
	HeartbeatInterval = 100 * time.Millisecond

	// HeartbeatTimeout is the amount of time a node can go without sending a
	// heartbeat before it is considered offline.
	HeartbeatTimeout = 3 * HeartbeatInterval

	// heartbeatTopic is the signal topic beneath which each node sends its
	// heartbeats, on a topic named after its node ID.
	heartbeatTopic = "heartbeat"
)

// Heartbeater periodically sends heartbeat signals to other nodes within the
// cluster.
type Heartbeater struct {
	// NodeID is the UUID of this node.
	NodeID *uuidpb.UUID

	// DB is the database connection used to send heartbeat signals.
	DB *sql.DB

	// Logger is the target for log messages produced by the heartbeater.
	Logger *slog.Logger
}

// Run sends a heartbeat signal immediately and on every interval tick until
// ctx is canceled.
func (h *Heartbeater) Run(ctx context.Context) {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	topic := heartbeatTopic + "." + h.NodeID.AsString()

	for {
		if err := signal.Notify(ctx, h.DB, topic); err != nil {
			if ctx.Err() != nil {
				return
			}

			h.Logger.ErrorContext(
				ctx,
				"unable to send heartbeat signal",
				slog.String("error", err.Error()),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
