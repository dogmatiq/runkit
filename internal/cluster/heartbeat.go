package cluster

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/signal"
	"github.com/dogmatiq/runkit/internal/supervisor"
)

// heartbeatTopic is the signal topic beneath which each node sends its
// heartbeats, on a topic named after its node ID.
const heartbeatTopic = "heartbeat"

// Heartbeater periodically sends heartbeat signals to other nodes within the
// cluster.
type Heartbeater struct {
	// NodeID is the UUID of this node.
	NodeID *uuidpb.UUID

	// DB is the database connection used to send heartbeat signals.
	DB *sql.DB

	// Interval is the time between heartbeats.
	Interval time.Duration

	// Logger is the target for log messages produced by the heartbeater.
	Logger *slog.Logger
}

// Run sends a heartbeat signal immediately and on every interval tick until
// ctx is canceled.
//
// It must be called at most once.
func (h *Heartbeater) Run(ctx context.Context) error {
	if h.Interval <= 0 {
		panic("heartbeat interval must be positive")
	}

	return supervisor.Run(ctx, h.Logger, h.run)
}

// run sends heartbeats until ctx is canceled or a signal can not be sent.
func (h *Heartbeater) run(ctx context.Context) error {
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()

	topic := heartbeatTopic + "." + h.NodeID.AsString()

	for {
		if err := signal.Notify(ctx, h.DB, topic); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
