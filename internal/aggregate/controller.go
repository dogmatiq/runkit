package aggregate

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
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
)

const (
	// pollInterval is the frequency at which the controller polls for new work.
	pollInterval = 25 * time.Millisecond

	// maxWorkers is the maximum number of worker goroutines that the controller
	// will spawn *per handler*.
	maxWorkers = 50
)

// Controller manages the state of instances of a single aggregate type.
type Controller struct {
	// Config is the aggregate's configuration.
	Config *config.Aggregate

	// DB is the database connection that the controller and its workers use.
	DB *sql.DB

	// Packer is used for packing the events that the aggregate records into
	// envelopes.
	Packer *envelopepb.Packer

	// Logger is the target for log messages from both the engine and the
	// application.
	Logger *slog.Logger

	workerCount uint
	workerDone  chan error
}

// Run runs the controller until ctx is canceled.
func (c *Controller) Run(ctx context.Context) error {
	c.workerDone = make(chan error, maxWorkers)

	ctx, cancel := context.WithCancel(ctx)
	defer func() {
		cancel()

		// Wait for all workers to finish before returning.
		for c.workerCount != 0 {
			<-c.workerDone
			c.workerCount--
		}
	}()

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
		case <-time.After(pollInterval):
			continue
		case err := <-c.workerDone:
			c.workerCount--

			if err != nil {
				c.Logger.Error(
					"worker produced an error",
					slog.String("error", err.Error()),
				)
			}
		}
	}
}

func (c *Controller) tick(ctx context.Context) error {
	if err := c.routeCommandsToInstances(ctx); err != nil {
		return err
	}

	if err := c.startWorkers(ctx); err != nil {
		return err
	}

	return nil
}

// routeCommandsToInstances routes unrouted commands of types that are handled
// by this controller to their target instances.
func (c *Controller) routeCommandsToInstances(ctx context.Context) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	commandEnvelopes, err := commandqueue.Acquire(ctx, tx, c.Config.RouteSet())
	if err != nil {
		return err
	}

	for _, commandEnvelope := range commandEnvelopes {
		commandMessageID := commandEnvelope.GetBody().GetMessageId()

		command, ok, err := c.unpackCommand(ctx, tx, commandEnvelope)
		if err != nil {
			return err
		}

		if !ok {
			continue
		}

		aggregateInstanceID := c.Config.Interface().RouteCommandToInstance(command)

		if err := c.ensureInstanceExists(ctx, tx, aggregateInstanceID); err != nil {
			return err
		}

		if err := commandqueue.Route(
			ctx,
			tx,
			commandMessageID,
			c.Config.Identity().GetKey(),
			aggregateInstanceID,
		); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// unpackCommand attempts to unpack the given envelope into a command.
//
// If the envelope cannot be unpacked then the command is Nack'd and ok is false.
func (c *Controller) unpackCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) (dogma.Command, bool, error) {
	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		commandMessageID := commandEnvelope.GetBody().GetMessageId()

		c.Logger.Error(
			"unable to unpack command",
			slog.String("message_id", commandMessageID.AsString()),
			slog.String("error", err.Error()),
		)

		return nil, false, commandqueue.Nack(ctx, tx, commandMessageID)
	}

	return command, true, nil
}

// ensureInstanceExists ensures that an instance row exists for the given
// instance ID. The row must exist before any commands can be routed to the
// instance.
func (c *Controller) ensureInstanceExists(
	ctx context.Context,
	tx *sql.Tx,
	aggregateInstanceID string,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO aggregate_instances (
			handler_key,
			instance_id
		) VALUES ($1, $2)
		ON CONFLICT (handler_key, instance_id) DO NOTHING`,
		database.MarshalUUID(c.Config.Identity().GetKey()),
		aggregateInstanceID,
	); err != nil {
		return fmt.Errorf("unable to insert aggregate instance: %w", err)
	}

	return nil
}

// startWorkers starts a new worker for each instance that has pending commands,
// up to the maximum number of workers.
//
// It ignores any instances that are already locked by existing workers.
func (c *Controller) startWorkers(ctx context.Context) error {
	limit := maxWorkers - c.workerCount
	if limit == 0 {
		return nil
	}

	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			i.instance_id
		FROM aggregate_instances AS i
		WHERE i.handler_key = $1
			AND EXISTS (
				SELECT 1
				FROM command_queue AS c
				WHERE c.routed_to_handler_key = i.handler_key
					AND c.routed_to_aggregate_instance_id = i.instance_id
					AND c.next_attempt_at <= now()
			)
		LIMIT $2
		FOR UPDATE OF i
		SKIP LOCKED`,
		database.MarshalUUID(c.Config.Identity().GetKey()),
		limit,
	)

	for rows.Next() {
		var aggregateInstanceID string
		if err := rows.Scan(&aggregateInstanceID); err != nil {
			return err
		}

		c.startWorker(ctx, aggregateInstanceID)
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to query aggregate instances with pending commands: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// startWorker starts a new worker for the given instance ID.
func (c *Controller) startWorker(ctx context.Context, instanceID string) {
	c.workerCount++

	w := &worker{
		Config:              c.Config,
		AggregateInstanceID: instanceID,
		DB:                  c.DB,
		Packer:              c.Packer,
		Logger: c.Logger.With(
			slog.String("instance_id", instanceID),
		),
	}

	go func() {
		c.workerDone <- w.Run(ctx)
	}()
}
