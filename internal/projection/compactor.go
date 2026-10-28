package projection

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Compactor periodically compacts a projection by invoking
// [dogma.ProjectionMessageHandler].Compact().
type Compactor struct {
	// DB is the database connection used to coordinate compaction.
	DB *sql.DB

	// Handler is the projection to compact.
	Handler dogma.ProjectionMessageHandler

	// Identity is the identity of the handler.
	Identity *identitypb.Identity

	// Interval is the minimum time between compaction attempts.
	Interval time.Duration

	// Logger is the target for log messages produced by the compactor and by
	// the handler during compaction.
	Logger *slog.Logger
}

// Run attempts compaction immediately and on every interval until ctx is
// canceled.
//
// It must be called at most once.
func (c *Compactor) Run(ctx context.Context) {
	supervisor.Run(ctx, c.Logger, c.run)
}

// run attempts compaction until ctx is canceled or an attempt fails.
func (c *Compactor) run(ctx context.Context) error {
	for {
		if err := c.tryCompact(ctx); err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(c.Interval):
		}
	}
}

func (c *Compactor) tryCompact(ctx context.Context) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin compaction transaction: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		`SELECT handler_key
		FROM projection.handlers
		WHERE handler_key = $1
			AND clock_timestamp() - last_compacted_at >= $2
		FOR UPDATE SKIP LOCKED`,
		xsql.UUID(c.Identity.GetKey()),
		c.Interval,
	)

	var handlerKey uuidpb.UUID
	if err := row.Scan(xsql.UUID(&handlerKey)); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}
		return fmt.Errorf("unable to acquire compaction lock: %w", err)
	}

	if err := xerrors.Recover(
		func() error {
			return c.Handler.Compact(
				ctx,
				&compactScope{c.Logger},
			)
		},
	); err != nil {
		return err
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE projection.handlers
		SET last_compacted_at = clock_timestamp()
		WHERE handler_key = $1`,
		xsql.UUID(c.Identity.GetKey()),
	); err != nil {
		return fmt.Errorf("unable to update compaction timestamp: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// compactScope implements [dogma.ProjectionCompactScope].
type compactScope struct {
	logger *slog.Logger
}

func (s *compactScope) Now() time.Time {
	return time.Now()
}

func (s *compactScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}
