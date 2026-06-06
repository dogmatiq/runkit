package engine

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
	"github.com/dogmatiq/reference-engine/internal/integration"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql
	"golang.org/x/sync/errgroup"
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
type Engine struct {
	// App is the Dogma application to run.
	App dogma.Application

	// DSN is the PostgreSQL connection string (DSN or URL).
	DSN string

	// Logger is the structured logger used by the engine.
	//
	// If it is nil, [slog.Default] is used.
	Logger *slog.Logger

	ready  xsync.Latch
	db     *sql.DB
	app    *config.Application
	packer *envelopepb.Packer
}

// Run starts the engine and blocks until ctx is canceled or an unrecoverable
// error occurs. It applies the schema, then runs one goroutine per handler.
//
// Multiple replicas may call Run concurrently against the same database; they
// share work via advisory locks and row-level locking.
func (e *Engine) Run(ctx context.Context) error {
	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}

	var err error
	e.db, err = sql.Open("pgx", e.DSN)
	if err != nil {
		return fmt.Errorf("unable to open database: %w", err)
	}
	defer e.db.Close()

	if err := database.ApplySchema(ctx, e.db); err != nil {
		return fmt.Errorf("unable to apply schema: %w", err)
	}

	e.app = runtimeconfig.FromApplication(e.App)
	e.packer = &envelopepb.Packer{Application: e.app.Identity()}

	g, ctx := errgroup.WithContext(ctx)

	for _, handler := range e.app.Handlers() {
		if !handler.IsDisabled() {
			g.Go(func() error {
				c := e.makeController(handler)
				return c.Run(ctx)
			})
		}
	}

	e.ready.Set()

	return g.Wait()
}

type controller interface {
	Run(context.Context) error
}

func (e *Engine) makeController(handler config.Handler) controller {
	switch handler := handler.(type) {
	case *config.Aggregate:
		return &aggregate.Controller{
			Config: handler,
			DB:     e.db,
			Packer: e.packer,
			Logger: e.Logger,
		}
	case *config.Integration:
		return &integration.Controller{
			Config: handler,
			DB:      e.db,
			Packer:  e.packer,
			Logger:  e.Logger,
		}
	default:
		panic("not implemented")
	}
}
