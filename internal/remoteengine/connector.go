package remoteengine

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/supervisor"
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

	// EventStreams is the catalog to which the event streams offered by each
	// available remote engine are added.
	EventStreams *eventstream.CompositeCatalog

	// Logger is the target for log messages produced by the connector and its
	// connections to remote engines.
	Logger *slog.Logger
}

// Run discovers remote engines until ctx is canceled.
func (c *Connector) Run(ctx context.Context) error {
	return supervisor.Run(ctx, c.Logger, c.discover)
}

// discover calls Discover, connecting to each remote engine that it adds until
// the engine is removed or Discover returns.
//
// Discover is expected to run until ctx is canceled, so returning before then
// is a failure, even without an error.
func (c *Connector) discover(ctx context.Context) error {
	var connections sync.WaitGroup
	defer connections.Wait()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	err := c.Discover(
		ctx,
		func(conn grpc.ClientConnInterface) func() {
			if ctx.Err() != nil {
				return func() {}
			}

			ctx, cancel := context.WithCancel(ctx)

			logger := c.Logger
			if t, ok := conn.(interface{ Target() string }); ok {
				logger = logger.With(
					slog.String("target", t.Target()),
				)
			}

			catalog := &eventstream.RemoteCatalog{
				Client: messaginggrpc.NewEventStreamConsumerAPIClient(conn),
				Logger: logger,
			}

			removeCatalog := c.EventStreams.Add(catalog)
			stopped := make(chan struct{})

			connections.Go(func() {
				defer close(stopped)
				defer removeCatalog()

				catalog.Run(ctx)
			})

			return func() {
				removeCatalog()
				cancel()
				<-stopped
			}
		},
	)

	if err == nil && ctx.Err() == nil {
		return errors.New("discoverer stopped before the context was canceled")
	}

	return err
}
