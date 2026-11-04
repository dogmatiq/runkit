package remoteengine

import (
	"context"
	"errors"
	"log/slog"

	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"google.golang.org/grpc"
)

// Connector runs a function that discovers remote engines, and connects the
// engine to each remote engine while it is available.
type Connector struct {
	// Discover adds and removes remote engines as they become available and
	// unavailable, as described by
	// [github.com/dogmatiq/runkit.RemoteEngineDiscoverer].
	Discover func(
		ctx context.Context,
		add func(conn grpc.ClientConnInterface) (remove func()),
	) error

	// Logger is the target for log messages produced by the connector and its
	// connections to remote engines.
	Logger *slog.Logger
}

// Run discovers remote engines until ctx is canceled.
func (c *Connector) Run(ctx context.Context) error {
	return supervisor.Run(ctx, c.Logger, c.discover)
}

// discover calls Discover until it returns.
//
// Discover is expected to run until ctx is canceled, so returning before then
// is a failure, even without an error.
//
// Nothing is done with the remote engines that Discover adds yet.
func (c *Connector) discover(ctx context.Context) error {
	add := func(grpc.ClientConnInterface) func() {
		return func() {}
	}

	err := xerrors.Recover(func() error {
		return c.Discover(ctx, add)
	})

	if err == nil && ctx.Err() == nil {
		return errors.New("discoverer stopped before the context was canceled")
	}

	return err
}
