package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/routes"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"
)

const (
	// pollInterval is the sleep between empty controller cycles and is also
	// used by per-instance workers as the wait-tx grace period before
	// cleanup.
	pollInterval = 100 * time.Millisecond

	// batchLimit caps the number of rows the controller's cursor-advance
	// and spawn-search transactions process per cycle.
	batchLimit = 32
)

// Controller manages dispatch for one [dogma.ProcessMessageHandler] across
// all of its instances.
type Controller struct {
	Config *config.Process
	DB     *sql.DB
	Packer *envelopepb.Packer
	Logger *slog.Logger
}

// Run blocks until ctx is cancelled, polling for work and spawning per-
// instance worker goroutines as instances become eligible.
func (c *Controller) Run(ctx context.Context) error {
	if _, err := c.DB.ExecContext(
		ctx,
		`INSERT INTO process_cursors (handler_key, next_offset)
		 VALUES ($1, 0) ON CONFLICT DO NOTHING`,
		c.Config.Identity().GetKey().AsString(),
	); err != nil {
		return err
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error { return c.dispatchLoop(ctx) })
	g.Go(func() error { return c.spawnLoop(ctx, g) })
	return g.Wait()
}

// dispatchLoop advances the cursor and restores deadline rows on each tick.
func (c *Controller) dispatchLoop(ctx context.Context) error {
	for {
		acted, err := c.advanceCursor(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"process cursor-advance error",
				slog.String("handler", c.Config.Identity().GetName()),
				slog.String("error", err.Error()),
			)
		}

		if err := c.restoreDeadlineRows(ctx); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"process deadline-restore error",
				slog.String("handler", c.Config.Identity().GetName()),
				slog.String("error", err.Error()),
			)
		}

		if acted {
			continue
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// spawnLoop spawns a worker goroutine for each instance that has work to do.
// Duplicate spawns self-eliminate at the worker's first FOR UPDATE SKIP LOCKED.
func (c *Controller) spawnLoop(ctx context.Context, g *errgroup.Group) error {
	for {
		ids, err := c.findSpawnable(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"process spawn error",
				slog.String("handler", c.Config.Identity().GetName()),
				slog.String("error", err.Error()),
			)
		}

		for _, id := range ids {
			w := &worker{
				c:                     c,
				instanceID:            id,
				lastSeenMutationCount: -1,
			}
			g.Go(func() error { return w.run(ctx) })
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(pollInterval):
		}
	}
}

// advanceCursor reads up to batchLimit events past the cursor that match
// the handler's subscribed types, routes each to an instance, INSERTs a
// routed_process_events row, performs the lock-acquiring UPSERT on
// process_instances, and advances the cursor — all in one transaction.
//
// Events whose target instance has already ended skip the
// routed_process_events INSERT but still advance the cursor past them.
//
// Returns true if any rows were processed (so the controller skips its
// idle sleep).
func (c *Controller) advanceCursor(ctx context.Context) (bool, error) {
	id := c.Config.Identity()
	handlerKey := id.GetKey().AsString()
	handler := c.Config.Interface()

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	var cursor int64
	if err := tx.QueryRowContext(
		ctx,
		`SELECT next_offset FROM process_cursors
		 WHERE handler_key = $1 FOR UPDATE`,
		handlerKey,
	).Scan(&cursor); err != nil {
		return false, err
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT "offset", envelope FROM events
		 WHERE "offset" >= $1 AND type_id = ANY($2::uuid[])
		 ORDER BY "offset"
		 LIMIT $3`,
		cursor,
		routes.MessageTypes(c.Config, config.HandlesEventRouteType),
		batchLimit,
	)
	if err != nil {
		return false, err
	}

	type rawEvent struct {
		offset   int64
		envBytes []byte
	}
	var raws []rawEvent
	for rows.Next() {
		var r rawEvent
		if err := rows.Scan(&r.offset, &r.envBytes); err != nil {
			rows.Close()
			return false, err
		}
		raws = append(raws, r)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return false, err
	}
	rows.Close()

	if len(raws) == 0 {
		return false, nil
	}

	maxOffset := cursor - 1
	for _, r := range raws {
		if r.offset > maxOffset {
			maxOffset = r.offset
		}

		env := &envelopepb.Envelope{}
		if err := proto.Unmarshal(r.envBytes, env); err != nil {
			c.Logger.Error(
				"process: skipping unparseable event",
				slog.String("handler", id.GetName()),
				slog.Int64("offset", r.offset),
				slog.String("error", err.Error()),
			)
			continue
		}

		rawMsg, err := envelopepb.Unpack(env)
		if err != nil {
			c.Logger.Error(
				"process: skipping unpackable event",
				slog.String("handler", id.GetName()),
				slog.Int64("offset", r.offset),
				slog.String("error", err.Error()),
			)
			continue
		}

		ev, ok := rawMsg.(dogma.Event)
		if !ok {
			c.Logger.Error(
				"process: non-event payload at offset",
				slog.String("handler", id.GetName()),
				slog.Int64("offset", r.offset),
				slog.String("type", fmt.Sprintf("%T", rawMsg)),
			)
			continue
		}

		instanceID, ok, err := handler.RouteEventToInstance(ctx, ev)
		if err != nil {
			return false, err
		}
		if !ok {
			continue
		}

		// Lock-acquiring UPSERT: returns ended cheaply and acquires a row
		// lock that serialises against any worker mid-cleanup.
		var ended bool
		if err := tx.QueryRowContext(
			ctx,
			`INSERT INTO process_instances (handler_key, instance_id)
			 VALUES ($1, $2)
			 ON CONFLICT (handler_key, instance_id)
			 DO UPDATE SET state = process_instances.state
			 RETURNING ended`,
			handlerKey, instanceID,
		).Scan(&ended); err != nil {
			return false, err
		}

		if ended {
			// Cursor advances past this event but the routed row is not
			// inserted: the handler ignores events for ended instances.
			continue
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO routed_process_events (handler_key, "offset", instance_id)
			 VALUES ($1, $2, $3)
			 ON CONFLICT (handler_key, "offset") DO NOTHING`,
			handlerKey, r.offset, instanceID,
		); err != nil {
			return false, err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE process_cursors SET next_offset = $1
		 WHERE handler_key = $2`,
		maxOffset+1, handlerKey,
	); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// restoreDeadlineRows ensures every (handler_key, instance_id) with a due
// deadline has a process_instances row, in case an earlier worker cleaned
// it up while a future deadline was still pending.
//
// DO NOTHING is safe here: the worker's cleanup deletes only when no
// deadlines remain, so a missing row at the time we run means no
// concurrent worker is racing this insert.
func (c *Controller) restoreDeadlineRows(ctx context.Context) error {
	handlerKey := c.Config.Identity().GetKey().AsString()

	_, err := c.DB.ExecContext(
		ctx,
		`INSERT INTO process_instances (handler_key, instance_id)
		 SELECT DISTINCT d.handler_key, d.instance_id
		 FROM deadlines d
		 WHERE d.handler_key = $1 AND d.next_attempt_at <= now()
		 ON CONFLICT (handler_key, instance_id) DO NOTHING`,
		handlerKey,
	)
	return err
}

// findSpawnable returns the IDs of instances that have at least one
// routable event or due deadline and are not currently locked.
func (c *Controller) findSpawnable(ctx context.Context) ([]string, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()

	rows, err := tx.QueryContext(
		ctx,
		`SELECT pi.instance_id FROM process_instances pi
		 WHERE pi.handler_key = $1 AND pi.ended = false
		   AND (
		     EXISTS (
		       SELECT 1 FROM routed_process_events rpe
		       WHERE rpe.handler_key = pi.handler_key
		         AND rpe.instance_id = pi.instance_id
		         AND rpe.next_attempt_at <= now()
		     )
		     OR EXISTS (
		       SELECT 1 FROM deadlines d
		       WHERE d.handler_key = pi.handler_key
		         AND d.instance_id = pi.instance_id
		         AND d.next_attempt_at <= now()
		     )
		   )
		 LIMIT $2 FOR UPDATE OF pi SKIP LOCKED`,
		c.Config.Identity().GetKey().AsString(), batchLimit,
	)
	if err != nil {
		return nil, err
	}

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()

	// Commit before returning: spawned workers' FOR UPDATE SKIP LOCKED would
	// otherwise observe these rows as locked by us and exit immediately.
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return ids, nil
}
