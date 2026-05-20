package integration

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"golang.org/x/sync/errgroup"
)

const (
	// pollInterval is the frequency at which workers poll for new commands.
	pollInterval = 25 * time.Millisecond

	// maxWorkers is the maximum number of worker goroutines per handler when
	// using MaximizeConcurrency.
	maxWorkers = 50
)

// Controller processes commands routed to a single integration handler.
type Controller struct {
	// Config is the integration handler's configuration.
	Config *config.Integration

	// DB is the database connection that the controller and its workers use.
	DB *sql.DB

	// Packer is used for packing the events that the handler records into
	// envelopes.
	Packer *envelopepb.Packer

	// Logger is the target for log messages from both the engine and the
	// application.
	Logger *slog.Logger
}

// Run runs the controller until ctx is canceled or an unrecoverable error
// occurs.
func (c *Controller) Run(ctx context.Context) error {
	n := maxWorkers
	if c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency {
		n = 1
	}

	g, ctx := errgroup.WithContext(ctx)

	for range n {
		g.Go(func() error {
			w := &worker{
				Config: c.Config,
				DB:     c.DB,
				Packer: c.Packer,
				Logger: c.Logger,
				Lock:   c.Config.ConcurrencyPreference() == dogma.MinimizeConcurrency,
			}

			err := w.Run(ctx)

			if errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}

			return err
		})
	}

	return g.Wait()
}
