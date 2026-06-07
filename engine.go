package dogmaengine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/reference-engine/internal/aggregate"
	"github.com/dogmatiq/reference-engine/internal/database"
	"golang.org/x/sync/errgroup"
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
type Engine struct {
	// DB is the database connection to use.
	DB *sql.DB

	// App is the Dogma application to run.
	App dogma.Application

	// Logger is the structured logger used by the engine.
	//
	// If it is nil, [slog.Default] is used.
	Logger *slog.Logger

	ready  xsync.Latch
	app    *config.Application
	packer *envelopepb.Packer
}

// Run starts the engine and blocks until ctx is canceled.
func (e *Engine) Run(ctx context.Context) error {
	e.app = runtimeconfig.FromApplication(e.App)

	if err := config.Validate(e.app, config.ForExecution()); err != nil {
		return fmt.Errorf("invalid application configuration: %w", err)
	}

	e.packer = &envelopepb.Packer{
		Application: e.app.Identity(),
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, handler := range e.app.Handlers() {
		c := e.buildHandlerController(handler)
		g.Go(func() error {
			return c.Run(ctx)
		})
	}

	e.ready.Set()

	return g.Wait()
}

// ExecuteCommand submits a [Command] for execution.
//
// It returns once the engine has taken ownership of the command. By
// default, it doesn't wait for handling to finish.
//
// See [dogma.CommandExecutor] for more details.
func (e *Engine) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	_ ...dogma.ExecuteCommandOption,
) error {
	if err := e.ready.WaitContext(ctx); err != nil {
		return err
	}

	commandEnvelope := e.packer.PackCommand(command)

	if _, err := e.DB.ExecContext(
		ctx,
		`INSERT INTO dogma.pending_commands (
			envelope
		) VALUES ($1)`,
		database.Envelope(commandEnvelope),
	); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	return nil
}

// controller is the interface implemented by all message handler controllers.
type controller interface {
	Run(context.Context) error
}

// buildHandlerController creates a controller for the given handler
// configuration.
func (e *Engine) buildHandlerController(handler config.Handler) controller {
	switch handler := handler.(type) {
	case *config.Aggregate:
		return &aggregate.Controller{
			DB:      e.DB,
			Handler: handler.Interface(),
		}
	default:
		panic(fmt.Sprintf("unsupported handler type: %T", handler))
	}
}
