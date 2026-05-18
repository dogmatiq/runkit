package projection

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/routes"
	"google.golang.org/protobuf/proto"
)

const (
	// pollInterval is the cadence at which the controller polls for new
	// events when idle, or when another replica holds the singleton lock.
	pollInterval = 100 * time.Millisecond

	// projectionCompactEvery is the number of events this replica handles
	// between calls to [dogma.ProjectionMessageHandler.Compact].
	projectionCompactEvery = 1000
)

// Controller delivers events from the global log to one projection handler.
//
// Each replica runs one Controller goroutine per projection. At most one
// replica processes a given projection at any moment; the other replicas idle
// and retry.
//
// The entire application event log is treated as one stream; the stream ID
// passed to the handler is the application key UUID.
type Controller struct {
	Config *config.Projection
	DB     *sql.DB
	AppKey string // also used as the stream_id passed to the handler
	Logger *slog.Logger
}

// Run starts the controller and blocks until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	handlerKey := c.Config.Identity().GetKey().AsString()

	// Seed the singleton_handlers row for this projection. Idempotent
	// across replicas and across restarts.
	if _, err := c.DB.ExecContext(
		ctx,
		`INSERT INTO singleton_handlers (handler_key) VALUES ($1)
		 ON CONFLICT DO NOTHING`,
		handlerKey,
	); err != nil {
		return fmt.Errorf("seed singleton_handlers: %w", err)
	}

	handler := c.Config.Interface()
	subscribed := routes.MessageTypes(c.Config, config.HandlesEventRouteType)

	var (
		cp                 uint64
		cpLoaded           bool
		eventsSinceCompact int
	)

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		acted, err := c.cycle(
			ctx,
			handler,
			handlerKey,
			subscribed,
			&cp,
			&cpLoaded,
			&eventsSinceCompact,
		)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"projection cycle error",
				"handler", c.Config.Identity().GetName(),
				"error", err,
			)
		}

		if !acted {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
		}
	}
}

// cycle runs one polling iteration. It returns acted=true when the
// transaction performed work (handled an event or ran Compact); acted=false
// means the controller should sleep before the next cycle.
//
// cp, cpLoaded and eventsSinceCompact are pointers so cycle can update the
// controller's in-memory bookkeeping across calls.
func (c *Controller) cycle(
	ctx context.Context,
	handler dogma.ProjectionMessageHandler,
	handlerKey string,
	subscribed []string,
	cp *uint64,
	cpLoaded *bool,
	eventsSinceCompact *int,
) (acted bool, err error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("begin tx: %w", err)
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
		}
	}()

	// Acquire the singleton lock for this projection. SKIP LOCKED means a
	// losing replica gets zero rows and loops with delay, rather than
	// queuing on a connection.
	var locked int
	err = tx.QueryRowContext(
		ctx,
		`SELECT 1 FROM singleton_handlers
		 WHERE handler_key = $1
		 FOR UPDATE SKIP LOCKED`,
		handlerKey,
	).Scan(&locked)
	if errors.Is(err, sql.ErrNoRows) {
		// Another replica holds the lock.
		_ = tx.Rollback()
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("acquire singleton lock: %w", err)
	}

	// Compact branch: if this replica has handled enough events since its
	// last compact, run Compact under the same lock.
	if *eventsSinceCompact >= projectionCompactEvery {
		scope := &compactScope{logger: c.Logger}
		if compactErr := handler.Compact(ctx, scope); compactErr != nil {
			// Drop the lock so another replica can try.
			_ = tx.Rollback()
			return false, fmt.Errorf("compact: %w", compactErr)
		}
		*eventsSinceCompact = 0

		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit (compact): %w", commitErr)
		}
		return true, nil
	}

	// Lazy-load the checkpoint on the first cycle that obtains the lock.
	if !*cpLoaded {
		loaded, cpErr := handler.CheckpointOffset(ctx, c.AppKey)
		if cpErr != nil {
			_ = tx.Rollback()
			return false, fmt.Errorf("checkpoint offset: %w", cpErr)
		}
		*cp = loaded
		*cpLoaded = true
	}

	// Pull the next subscribed event at or after the checkpoint.
	var (
		off     int64
		envData []byte
	)
	err = tx.QueryRowContext(
		ctx,
		`SELECT "offset", envelope
		 FROM events
		 WHERE "offset" >= $1
		   AND type_id = ANY($2::uuid[])
		 ORDER BY "offset"
		 LIMIT 1`,
		*cp,
		subscribed,
	).Scan(&off, &envData)
	if errors.Is(err, sql.ErrNoRows) {
		// No events past the checkpoint. Release the lock and idle.
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit (idle): %w", commitErr)
		}
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query event: %w", err)
	}

	// Decode the envelope and unpack the event.
	evEnv := &envelopepb.Envelope{}
	if unmarshalErr := proto.Unmarshal(envData, evEnv); unmarshalErr != nil {
		// Skip past the malformed envelope; otherwise the controller would
		// re-fetch the same row forever.
		c.Logger.Error(
			"projection: skipping event with malformed envelope",
			"handler", c.Config.Identity().GetName(),
			"offset", off,
			"error", unmarshalErr,
		)
		*cp = uint64(off) + 1
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit (skip malformed): %w", commitErr)
		}
		return true, nil
	}

	rawMsg, unpackErr := envelopepb.Unpack(evEnv)
	if unpackErr != nil {
		c.Logger.Error(
			"projection: skipping event that cannot be unpacked",
			"handler", c.Config.Identity().GetName(),
			"offset", off,
			"error", unpackErr,
		)
		*cp = uint64(off) + 1
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit (skip unpack): %w", commitErr)
		}
		return true, nil
	}

	ev, ok := rawMsg.(dogma.Event)
	if !ok {
		c.Logger.Error(
			"projection: skipping non-event message",
			"handler", c.Config.Identity().GetName(),
			"offset", off,
			"type", fmt.Sprintf("%T", rawMsg),
		)
		*cp = uint64(off) + 1
		if commitErr := tx.Commit(); commitErr != nil {
			return false, fmt.Errorf("commit (skip non-event): %w", commitErr)
		}
		return true, nil
	}

	scope := &eventScope{
		streamID:         c.AppKey,
		offset:           uint64(off),
		checkpointOffset: *cp,
		recordedAt:       evEnv.GetBody().GetCreatedAt().AsTime(),
		logger:           c.Logger,
	}

	newCp, handleErr := handler.HandleEvent(ctx, scope, ev)
	if handleErr != nil {
		// Roll back; release the singleton lock so another replica or the
		// next cycle can try again.
		_ = tx.Rollback()
		c.Logger.Error(
			"projection: handle event failed",
			"handler", c.Config.Identity().GetName(),
			"offset", off,
			"error", handleErr,
		)
		// Don't propagate; matches process/aggregate-style retry semantics.
		// The error is logged; cycle returns acted=false to back off.
		return false, nil
	}

	// Update the in-memory checkpoint to whatever the handler reported.
	*cp = newCp
	if newCp == uint64(off)+1 {
		// Net-new event was handled. Bump the per-replica compact counter.
		*eventsSinceCompact++
	}
	// else: OCC conflict — handler signalled its persisted checkpoint is
	// behind newCp's value. The next cycle resumes from the handler's
	// reported position. Don't bump the counter (no net-new event handled).

	if commitErr := tx.Commit(); commitErr != nil {
		return false, fmt.Errorf("commit (handled): %w", commitErr)
	}
	return true, nil
}
