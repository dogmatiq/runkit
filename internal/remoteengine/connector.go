package remoteengine

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/protobuf/messaginggrpc"
	"github.com/dogmatiq/runkit/internal/eventstream"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
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

	// EventStreamRecheckInterval is how long the connector waits before asking
	// a remote engine again after it reports having no events or event streams
	// to offer.
	EventStreamRecheckInterval time.Duration

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

	add := func(conn grpc.ClientConnInterface) func() {
		if ctx.Err() != nil {
			return func() {}
		}

		logger := c.Logger
		if t, ok := conn.(interface{ Target() string }); ok {
			logger = logger.With(
				slog.String("target", t.Target()),
			)
		}

		catalog := &eventstream.RemoteCatalog{
			Client:          messaginggrpc.NewEventStreamConsumerAPIClient(conn),
			RecheckInterval: c.EventStreamRecheckInterval,
			Logger:          logger,
		}

		removeCatalog := c.EventStreams.Add(catalog)

		// The catalog is removed before it is stopped, so that its streams are
		// no longer offered by the time its subscriptions are lost. Its context
		// is therefore canceled only after removal, rather than along with ctx.
		catalogCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))

		remove := func() {
			removeCatalog()
			cancel()
		}

		deregister := context.AfterFunc(ctx, remove)
		stopped := make(chan struct{})

		connections.Go(func() {
			defer close(stopped)
			defer removeCatalog()

			if err := catalog.Run(catalogCtx); catalogCtx.Err() == nil {
				logger.ErrorContext(
					catalogCtx,
					"stopped consuming events from remote engine",
					xslog.Error(err),
				)
			}
		})

		return func() {
			deregister()
			remove()
			<-stopped
		}
	}

	err := xerrors.Recover(func() error {
		return c.Discover(ctx, add)
	})

	if err == nil && ctx.Err() == nil {
		return errors.New("discoverer stopped before the context was canceled")
	}

	return err
}
