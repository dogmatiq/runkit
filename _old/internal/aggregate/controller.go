package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

// Controller manages the state of instances of a single aggregate type.
type Controller struct {
	// Config is the aggregate message handler's configuration.
	Config *config.Aggregate

	// DB is the database connection that the controller uses.
	DB *sql.DB

	// Packer is used for packing the events that the aggregate records into
	// envelopes.
	Packer *envelopepb.Packer

	// Logger is the target for log messages from both the engine and the
	// application.
	Logger *slog.Logger

	// PollInterval is the frequency at which the controller polls for new work.
	PollInterval time.Duration

	// MaxConcurrency is the maximum number of instances that the controller
	// will load into memory at the same time.
	MaxConcurrency int

	poll               *time.Ticker
	loadedInstances    []string
	maxLoadedInstances int
	instanceUnloaded   chan instanceUnloaded
}

// Run runs the controller until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	c.maxLoadedInstances = max(1, c.MaxConcurrency)
	c.instanceUnloaded = make(chan instanceUnloaded, c.maxLoadedInstances)

	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()

		// Wait for all workers to finish before returning.
		for len(c.loadedInstances) != 0 {
			c.handleInstanceUnloaded(<-c.instanceUnloaded)
		}
	}()

	c.poll = time.NewTicker(max(1, c.PollInterval))

	for {
		if err := c.tick(ctx); err != nil {
			if errors.Is(err, ctx.Err()) {
				return ctx.Err()
			}

			c.Logger.Error(
				"controller produced an error",
				slog.String("error", err.Error()),
			)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-c.poll.C:
			continue
		case signal := <-c.instanceUnloaded:
			c.handleInstanceUnloaded(signal)
		}
	}
}

func (c *Controller) tick(ctx context.Context) error {
	if len(c.loadedInstances) == c.maxLoadedInstances {
		// We've reached max concurrency, so we shouldn't poll for more work
		// until a worker finishes.
		//
		// This could happen if c.poll fired during the previous tick, we should
		// just ignore it and wait for the next tick.
		return nil
	}

	return database.Transact(
		ctx,
		c.DB,
		func(ctx context.Context, tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT
					i.instance_id
				FROM aggregate_instances AS i
				WHERE i.handler_key = $1
					AND i.instance_id != ALL($2::text[])
					AND EXISTS (
						SELECT 1
						FROM pending_commands AS c
						WHERE c.handler_key = i.handler_key
							AND c.aggregate_instance_id = i.instance_id
							AND c.next_attempt_at <= clock_timestamp()
					)
				LIMIT $3
				FOR UPDATE OF i
				SKIP LOCKED`,
				database.MarshalUUID(c.Config.Identity().GetKey()),
				c.loadedInstances,
				c.maxLoadedInstances-len(c.loadedInstances),
			)
			if err != nil {
				return fmt.Errorf("unable to query aggregate instances with pending commands: %w", err)
			}
			defer rows.Close()

			for rows.Next() {
				var aggregateInstanceID string
				if err := rows.Scan(&aggregateInstanceID); err != nil {
					return fmt.Errorf("unable to scan aggregate instance ID: %w", err)
				}

				c.loadInstance(ctx, aggregateInstanceID)
			}

			if err := rows.Err(); err != nil {
				return fmt.Errorf("unable to iterate aggregate instances: %w", err)
			}

			return nil
		},
	)
}

// loadInstance loads the aggregate instance with the given ID in its own
// goroutine.
func (c *Controller) loadInstance(ctx context.Context, instanceID string) {
	idx, found := slices.BinarySearch(c.loadedInstances, instanceID)
	if found {
		panic("instance is already loaded")
	}

	c.loadedInstances = slices.Insert(
		c.loadedInstances,
		idx,
		instanceID,
	)

	// If we've reached max concurrency, stop the ticker to prevent more
	// polling for more work until a worker finishes.
	if len(c.loadedInstances) == c.maxLoadedInstances {
		c.poll.Stop()
	}

	i := &instance{
		Config:     c.Config,
		InstanceID: instanceID,
		DB:         c.DB,
		Packer:     c.Packer,
		Logger: c.Logger.With(
			slog.String("instance_id", instanceID),
		),
	}

	go func() {
		c.instanceUnloaded <- instanceUnloaded{
			InstanceID: instanceID,
			Error:      i.Run(ctx),
		}
	}()
}

// instanceUnloaded is the signal sent by an instance when it is unloaded.
type instanceUnloaded struct {
	InstanceID string
	Error      error
}

// handleInstanceUnloaded is called when a worker finishes.
func (c *Controller) handleInstanceUnloaded(signal instanceUnloaded) {
	// If we were at max concurrency, restart the ticker now that a
	// worker has finished.
	if len(c.loadedInstances) == c.maxLoadedInstances {
		c.poll.Reset(max(1, c.PollInterval))
	}

	idx, found := slices.BinarySearch(c.loadedInstances, signal.InstanceID)
	if !found {
		panic("instance is not loaded")
	}

	c.loadedInstances = slices.Delete(
		c.loadedInstances,
		idx,
		idx+1,
	)

	if signal.Error != nil {
		c.Logger.Error(
			"worker produced an error",
			slog.String("instance_id", signal.InstanceID),
			slog.String("error", signal.Error.Error()),
		)
	}
}
