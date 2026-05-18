package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/retry"
	"github.com/dogmatiq/reference-engine/internal/routes"
	"google.golang.org/protobuf/proto"
)

// worker is the per-instance dispatch loop for a single process instance.
//
// It serialises message handling for that instance via FOR UPDATE
// SKIP LOCKED on its process_instances row. It drains deadlines first,
// then events, persists each handler's effects atomically with the
// bookkeeping that removes the claim row, and exits when both queues are
// empty.
//
// All errors during message handling are non-fatal: the worker logs and
// continues; the offending row's next_attempt_at is pushed forward by
// retry.MarkRoutedEventFailed / retry.MarkDeadlineFailed.
type worker struct {
	c                     *Controller
	instanceID            string
	root                  dogma.ProcessRoot
	lastSeenMutationCount int64
}

// run is the worker's drain loop. It exits cleanly when ctx is done, when
// the drain-tx's FOR UPDATE SKIP LOCKED returns zero rows (lost lock or
// row deleted), or when the wait-tx confirms the queues are still empty
// after pollInterval.
func (w *worker) run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		exit, err := w.drainOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			w.c.Logger.Error(
				"process worker error",
				slog.String("handler", w.c.Config.Identity().GetName()),
				slog.String("instance", w.instanceID),
				slog.String("error", err.Error()),
			)
			continue
		}
		if exit {
			return nil
		}
	}
}

// drainOnce performs one iteration of the worker loop. It may handle a
// deadline, handle an event, or run the wait/cleanup tx. The bool return
// is true when the worker should exit cleanly (e.g. lost the lock, or
// idle cleanup performed).
func (w *worker) drainOnce(ctx context.Context) (bool, error) {
	handlerKey := w.c.Config.Identity().GetKey().AsString()

	tx, err := w.c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	// Liveness + cache-freshness check.
	var (
		state         []byte
		ended         bool
		mutationCount int64
	)
	err = tx.QueryRowContext(
		ctx,
		`SELECT state, ended, mutation_count FROM process_instances
		 WHERE handler_key = $1 AND instance_id = $2
		 FOR UPDATE SKIP LOCKED`,
		handlerKey, w.instanceID,
	).Scan(&state, &ended, &mutationCount)
	if errors.Is(err, sql.ErrNoRows) {
		// Either another worker holds the row, or the row was deleted by
		// an idle cleanup. Either way, we exit.
		return true, nil
	}
	if err != nil {
		return false, err
	}

	if ended {
		// Sweep any straggling deadlines (controller's restore tx may
		// have just re-inserted the process_instances row in response to
		// a deadline that survived an earlier crash) and exit.
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM deadlines WHERE handler_key = $1 AND instance_id = $2`,
			handlerKey, w.instanceID,
		); err != nil {
			return false, err
		}
		if err := tx.Commit(); err != nil {
			return false, err
		}
		return true, nil
	}

	if mutationCount != w.lastSeenMutationCount {
		w.root = w.c.Config.Interface().New()
		if state != nil {
			if err := w.root.UnmarshalBinary(state); err != nil {
				return false, err
			}
		}
		w.lastSeenMutationCount = mutationCount
	}

	// Deadlines first.
	handled, err := w.handleDeadline(ctx, tx)
	if err != nil {
		return false, err
	}
	if handled {
		// tx was committed inside handleDeadline.
		return false, nil
	}

	// Then events.
	handled, err = w.handleEvent(ctx, tx)
	if err != nil {
		return false, err
	}
	if handled {
		// tx was committed inside handleEvent.
		return false, nil
	}

	// Both queues empty. Run the wait/cleanup phase within this same tx
	// (we still hold FOR UPDATE).
	return w.waitOrCleanup(ctx, tx)
}

// handleDeadline claims the earliest due deadline for this instance,
// invokes the handler, persists effects, DELETEs the deadlines row, and
// COMMITs. Returns true if a deadline was handled (regardless of whether
// the handler succeeded — failures push next_attempt_at forward via
// MarkDeadlineFailed in a fresh tx).
func (w *worker) handleDeadline(ctx context.Context, tx *sql.Tx) (bool, error) {
	id := w.c.Config.Identity()
	handlerKey := id.GetKey().AsString()
	handler := w.c.Config.Interface()

	var (
		deadlineID   string
		scheduledFor time.Time
		attemptCount int
		envBytes     []byte
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT id::text, scheduled_for, attempt_count, envelope FROM deadlines
		 WHERE handler_key = $1 AND instance_id = $2 AND next_attempt_at <= now()
		 ORDER BY scheduled_for LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
		handlerKey, w.instanceID,
	).Scan(&deadlineID, &scheduledFor, &attemptCount, &envBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	dlEnv := &envelopepb.Envelope{}
	if err := proto.Unmarshal(envBytes, dlEnv); err != nil {
		return true, w.markDeadlineFailed(ctx, deadlineID, attemptCount, err)
	}
	rawMsg, err := envelopepb.Unpack(dlEnv)
	if err != nil {
		return true, w.markDeadlineFailed(ctx, deadlineID, attemptCount, err)
	}
	dl, ok := rawMsg.(dogma.Deadline)
	if !ok {
		return true, w.markDeadlineFailed(
			ctx, deadlineID, attemptCount,
			fmt.Errorf("expected deadline, got %T", rawMsg),
		)
	}

	s := &scope{
		instanceID: w.instanceID,
		root:       w.root,
		packer:     w.c.Packer.PackEffects(dlEnv, id, envelopepb.WithInstanceID(w.instanceID)),
		time:       scheduledFor,
		logger:     w.c.Logger,
	}

	if err := handler.HandleDeadline(ctx, w.root, s, dl); err != nil {
		return true, w.markDeadlineFailed(ctx, deadlineID, attemptCount, err)
	}

	if err := w.persistEffects(ctx, tx, s); err != nil {
		return true, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM deadlines WHERE id = $1`,
		deadlineID,
	); err != nil {
		return true, err
	}
	if err := tx.Commit(); err != nil {
		return true, err
	}
	return true, nil
}

// handleEvent claims the earliest due routed_process_events row for this
// instance, invokes the handler (subject to rejection checks for
// type-unsubscribed and instance-mismatch cases), persists effects,
// DELETEs the routed row, and COMMITs.
func (w *worker) handleEvent(ctx context.Context, tx *sql.Tx) (bool, error) {
	id := w.c.Config.Identity()
	handlerKey := id.GetKey().AsString()
	handler := w.c.Config.Interface()

	var (
		offset       int64
		attemptCount int
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT "offset", attempt_count FROM routed_process_events
		 WHERE handler_key = $1 AND instance_id = $2 AND next_attempt_at <= now()
		 ORDER BY "offset" LIMIT 1
		 FOR UPDATE SKIP LOCKED`,
		handlerKey, w.instanceID,
	).Scan(&offset, &attemptCount)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}

	var envBytes []byte
	if err := tx.QueryRowContext(
		ctx,
		`SELECT envelope FROM events WHERE "offset" = $1`,
		offset,
	).Scan(&envBytes); err != nil {
		return true, err
	}

	env := &envelopepb.Envelope{}
	if err := proto.Unmarshal(envBytes, env); err != nil {
		return true, w.markEventFailed(ctx, offset, attemptCount, err)
	}
	rawMsg, err := envelopepb.Unpack(env)
	if err != nil {
		return true, w.markEventFailed(ctx, offset, attemptCount, err)
	}
	ev, ok := rawMsg.(dogma.Event)
	if !ok {
		return true, w.markEventFailed(
			ctx, offset, attemptCount,
			fmt.Errorf("expected event, got %T", rawMsg),
		)
	}

	// Rejection check 1: handler no longer subscribes to this type.
	subscribed := routes.MessageTypes(w.c.Config, config.HandlesEventRouteType)
	typeID := env.GetBody().GetMessage().GetTypeId().AsString()
	if !slices.Contains(subscribed, typeID) {
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM routed_process_events
			 WHERE handler_key = $1 AND "offset" = $2`,
			handlerKey, offset,
		); err != nil {
			return true, err
		}
		if err := tx.Commit(); err != nil {
			return true, err
		}
		return true, nil
	}

	// Rejection check 2: handler now routes the event to a different
	// instance. UPDATE the routed row to point at the new instance and
	// commit; the new instance's worker will pick it up next cycle.
	newInstanceID, ok, err := handler.RouteEventToInstance(ctx, ev)
	if err != nil {
		return true, w.markEventFailed(ctx, offset, attemptCount, err)
	}
	if !ok {
		// Routing now ignores the event entirely.
		if _, err := tx.ExecContext(
			ctx,
			`DELETE FROM routed_process_events
			 WHERE handler_key = $1 AND "offset" = $2`,
			handlerKey, offset,
		); err != nil {
			return true, err
		}
		if err := tx.Commit(); err != nil {
			return true, err
		}
		return true, nil
	}
	if newInstanceID != w.instanceID {
		if _, err := tx.ExecContext(
			ctx,
			`UPDATE routed_process_events SET instance_id = $1
			 WHERE handler_key = $2 AND "offset" = $3`,
			newInstanceID, handlerKey, offset,
		); err != nil {
			return true, err
		}
		// Ensure the target instance has a row so the controller's spawn
		// loop can find it.
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO process_instances (handler_key, instance_id)
			 VALUES ($1, $2)
			 ON CONFLICT (handler_key, instance_id)
			 DO UPDATE SET state = process_instances.state`,
			handlerKey, newInstanceID,
		); err != nil {
			return true, err
		}
		if err := tx.Commit(); err != nil {
			return true, err
		}
		return true, nil
	}

	s := &scope{
		instanceID: w.instanceID,
		root:       w.root,
		packer:     w.c.Packer.PackEffects(env, id, envelopepb.WithInstanceID(w.instanceID)),
		time:       env.GetBody().GetCreatedAt().AsTime(),
		logger:     w.c.Logger,
	}

	if err := handler.HandleEvent(ctx, w.root, s, ev); err != nil {
		return true, w.markEventFailed(ctx, offset, attemptCount, err)
	}

	if err := w.persistEffects(ctx, tx, s); err != nil {
		return true, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM routed_process_events
		 WHERE handler_key = $1 AND "offset" = $2`,
		handlerKey, offset,
	); err != nil {
		return true, err
	}
	if err := tx.Commit(); err != nil {
		return true, err
	}
	return true, nil
}

// waitOrCleanup is the wait/cleanup tail of one drain iteration, run when
// neither a deadline nor an event was available. The drain tx is still
// open and still holds the FOR UPDATE on the process_instances row.
//
// It sleeps pollInterval, re-checks both queues; if work appeared,
// rolls back so the next iteration can claim it. Otherwise, if the row
// is still in the cleanup-eligible state, DELETEs it and commits.
//
// Returns true if the worker should exit.
func (w *worker) waitOrCleanup(ctx context.Context, tx *sql.Tx) (bool, error) {
	handlerKey := w.c.Config.Identity().GetKey().AsString()

	select {
	case <-ctx.Done():
		return true, nil
	case <-time.After(pollInterval):
	}

	// Re-check for due work; we still hold the FOR UPDATE so no new
	// routed_process_events / deadlines could have come in via this
	// instance from another worker on this replica, but cross-replica
	// controllers can have inserted rows.
	var hasWork bool
	if err := tx.QueryRowContext(
		ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM routed_process_events
		   WHERE handler_key = $1 AND instance_id = $2 AND next_attempt_at <= now()
		 ) OR EXISTS (
		   SELECT 1 FROM deadlines
		   WHERE handler_key = $1 AND instance_id = $2 AND next_attempt_at <= now()
		 )`,
		handlerKey, w.instanceID,
	).Scan(&hasWork); err != nil {
		return false, err
	}

	if hasWork {
		// Drop the lock and re-enter the drain loop.
		_ = tx.Rollback()
		return false, nil
	}

	// Cleanup: only if state IS NULL AND ended = false. If a Mutate has
	// been persisted, the instance's row is part of the durable state and
	// must not be deleted. We still hold the FOR UPDATE so the conditional
	// DELETE is race-free.
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM process_instances
		 WHERE handler_key = $1 AND instance_id = $2
		   AND state IS NULL AND ended = false`,
		handlerKey, w.instanceID,
	); err != nil {
		return false, err
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

// markEventFailed records a handler error against the routed_process_events
// row in a fresh transaction, scheduling the event for retry.
func (w *worker) markEventFailed(
	ctx context.Context,
	offset int64,
	attemptCount int,
	handlerErr error,
) error {
	handlerKey := w.c.Config.Identity().GetKey().AsString()
	tx, err := w.c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return retry.MarkRoutedEventFailed(
		ctx, tx,
		handlerKey, offset, attemptCount,
		handlerErr,
		w.c.Config.Identity(),
		w.c.Logger,
	)
}

// markDeadlineFailed records a handler error against the deadlines row in
// a fresh transaction, scheduling the deadline for retry.
func (w *worker) markDeadlineFailed(
	ctx context.Context,
	deadlineID string,
	attemptCount int,
	handlerErr error,
) error {
	tx, err := w.c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	return retry.MarkDeadlineFailed(
		ctx, tx,
		deadlineID, attemptCount,
		handlerErr,
		w.c.Config.Identity(),
		w.c.Logger,
	)
}
