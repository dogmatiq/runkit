package integration

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/reference-engine/internal/routes"
)

// integrationConcurrency caps the number of concurrently-handling
// max-parallel integration workers across all such handlers in this
// replica.
const integrationConcurrency = 16

// globalSem is the engine-wide semaphore shared by every max-parallel
// integration handler. Initialised exactly once via [globalSemOnce].
var (
	globalSem     chan struct{}
	globalSemOnce sync.Once
)

// initGlobalSem lazily initialises [globalSem] on first use.
func initGlobalSem() {
	globalSemOnce.Do(func() {
		globalSem = make(chan struct{}, integrationConcurrency)
	})
}

// runMaxParallel implements the max-parallel-mode loop. A single polling
// goroutine probes for pending work and, on hits, acquires a slot on
// [globalSem] and spawns a drain worker.
func (c *Controller) runMaxParallel(ctx context.Context) error {
	initGlobalSem()

	var wg sync.WaitGroup
	defer wg.Wait()

	types := routes.MessageTypes(c.Config, config.HandlesCommandRouteType)

	for {
		var hasWork bool
		err := c.DB.QueryRowContext(
			ctx,
			`SELECT EXISTS(
				SELECT 1 FROM commandqueue.commands
				WHERE message_type_id = ANY($1::uuid[]) AND next_attempt_at <= now()
			)`,
			types,
		).Scan(&hasWork)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			c.Logger.Error(
				"integration lookahead error",
				slog.String("handler", c.Config.Identity().GetName()),
				slog.String("error", err.Error()),
			)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		if !hasWork {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(pollInterval):
			}
			continue
		}

		// Acquire a global slot; spawn a drain worker.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case globalSem <- struct{}{}:
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-globalSem }()
			c.drainWorker(ctx)
		}()
	}
}

// drainWorker handles commands until its FOR UPDATE SKIP LOCKED returns 0
// rows (queue drained or absorbed by other workers). The worker exits on
// any error or context cancellation; the deferred slot release in the
// caller frees the global semaphore.
func (c *Controller) drainWorker(ctx context.Context) {
	for {
		if ctx.Err() != nil {
			return
		}

		dispatched, err := c.workerCycle(ctx)
		if err != nil {
			if ctx.Err() == nil {
				c.Logger.Error(
					"integration worker error",
					slog.String("handler", c.Config.Identity().GetName()),
					slog.String("error", err.Error()),
				)
			}
			return
		}
		if !dispatched {
			return
		}
	}
}

// workerCycle attempts one (claim, handle) cycle in its own transaction.
// Returns dispatched=true if a command was handled; dispatched=false if no
// rows were available to claim. A handler error is itself recorded via
// MarkCommandFailed and reported as dispatched=true with err=nil.
func (c *Controller) workerCycle(ctx context.Context) (bool, error) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()

	cmd, ok, err := c.claimCommand(ctx, tx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if err := c.handleClaimed(ctx, tx, cmd); err != nil {
		return true, err
	}
	return true, nil
}
