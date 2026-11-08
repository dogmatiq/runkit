package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"golang.org/x/sync/errgroup"
)

// MessagePump is a helper for implementing a message pump that acquires and
// handles deliveries concurrently.
type MessagePump struct {
	// Driver acquires and handles deliveries.
	Driver Driver

	// DB is the database connection used to acquire and handle deliveries.
	DB *sql.DB

	// Workers is the number of deliveries that are handled concurrently.
	Workers int

	// PollInterval is the time to wait before checking for pending deliveries
	// again when there are none.
	PollInterval time.Duration

	// BackoffBase and BackoffCap bound the delay before a delivery that could
	// not be handled is attempted again.
	BackoffBase, BackoffCap time.Duration

	// EngineLogger is the target for log messages produced by the pump and its
	// supervisor.
	EngineLogger *slog.Logger
}

type deliveryTask struct {
	Tx           *sql.Tx
	Delivery     Delivery
	Envelope     *envelopepb.Envelope
	EngineLogger *slog.Logger
}

// Run runs the message pump until ctx is canceled.
//
// It must be called at most once.
func (p *MessagePump) Run(ctx context.Context) error {
	return supervisor.Run(ctx, p.EngineLogger, "deliver messages", p.run)
}

// run starts the dispatcher and workers, and runs them until ctx is canceled
// or any of them fails.
//
// The dispatcher and workers are restarted together, so that a delivery that
// can not be handled is retried no sooner than the supervisor's backoff
// allows. Restarting also rolls back any delivery that the dispatcher has
// acquired but not yet handed to a worker.
func (p *MessagePump) run(ctx context.Context) error {
	group, ctx := errgroup.WithContext(ctx)
	tasks := make(chan *deliveryTask)

	for range max(1, p.Workers) {
		group.Go(func() error {
			return p.runWorker(ctx, tasks)
		})
	}

	group.Go(func() error {
		return p.runDispatcher(ctx, tasks)
	})

	return group.Wait()
}

// runWorker handles the tasks sent by the dispatcher until ctx is canceled or
// a delivery can not be handled.
//
// A delivery that fails or is contended is postponed, which is not a failure
// of the worker; see [doHandle].
func (p *MessagePump) runWorker(ctx context.Context, tasks <-chan *deliveryTask) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case task := <-tasks:
			if err := p.doHandle(ctx, task); err != nil {
				return fmt.Errorf("unable to deliver message: %w", err)
			}
		}
	}
}

// runDispatcher acquires deliveries and sends them to workers via tasks until
// ctx is canceled or a delivery can not be acquired.
func (p *MessagePump) runDispatcher(ctx context.Context, tasks chan<- *deliveryTask) error {
	for {
		task, ok, err := p.doAcquireTask(ctx)
		if err != nil {
			return err
		}

		if !ok {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.PollInterval):
			}

			continue
		}

		task.EngineLogger.DebugContext(
			ctx,
			"acquired message for delivery",
		)

		select {
		case <-ctx.Done():
			task.Tx.Rollback()
			return ctx.Err()
		case tasks <- task:
		}
	}
}

// doAcquireTask attempts to acquire a pending delivery and unmarshal its
// envelope, returning a [task] ready to hand off to a worker.
//
// If there are no pending deliveries, ok is false.
func (p *MessagePump) doAcquireTask(ctx context.Context) (*deliveryTask, bool, error) {
	for {
		tx, del, ok, err := p.doAcquireDelivery(ctx)
		if err != nil || !ok {
			return nil, false, err
		}

		attrs := []any{
			xslog.UUID("id", uuidpb.Generate()),
			slog.Uint64("attempt", del.Failures+1),
		}

		logger := p.EngineLogger.With(slog.Group("delivery", attrs...))

		env := &envelopepb.Envelope{}
		err = env.UnmarshalBinary(del.EnvelopeBytes)
		if err == nil {
			err = env.Validate()
		}

		if err != nil {
			logger.ErrorContext(
				ctx,
				"unable to unmarshal message envelope",
				xslog.Error(err),
			)

			del.Failures++

			if err := p.Driver.PostponeDelivery(
				ctx,
				tx,
				del,
				p.computeBackoff(del.Failures),
			); err != nil {
				tx.Rollback()
				return nil, false, fmt.Errorf("unable to postpone delivery: %w", err)
			}

			if err := tx.Commit(); err != nil {
				return nil, false, fmt.Errorf("unable to commit transaction: %w", err)
			}

			continue
		}

		return &deliveryTask{
			tx,
			del,
			env,
			logger.With(
				xslog.Envelope("handled_message", env),
			),
		}, true, nil
	}
}

// doAcquireDelivery attempts to acquire a pending delivery.
//
// If there are no pending deliveries, ok is false.
func (p *MessagePump) doAcquireDelivery(ctx context.Context) (
	tx *sql.Tx,
	del Delivery,
	ok bool,
	err error,
) {
	tx, err = p.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, Delivery{}, false, fmt.Errorf("unable to begin transaction: %w", err)
	}

	del, ok, err = p.Driver.AcquireDelivery(ctx, tx)
	if err != nil {
		tx.Rollback()
		return nil, Delivery{}, false, fmt.Errorf("unable to acquire delivery: %w", err)
	}

	if !ok {
		if err := tx.Commit(); err != nil {
			return nil, Delivery{}, false, fmt.Errorf("unable to commit transaction: %w", err)
		}

		return nil, Delivery{}, false, nil
	}

	return tx, del, true, nil
}

// doHandle handles the delivery within a transaction, rolling back the
// transaction if an error occurs.
func (p *MessagePump) doHandle(ctx context.Context, task *deliveryTask) error {
	defer task.Tx.Rollback()

	err := p.Driver.HandleDelivery(
		ctx,
		task.Tx,
		task.Delivery,
		task.Envelope,
		task.EngineLogger,
	)

	var afterCommit func()

	switch {
	case err == nil:
		afterCommit = func() {
			task.EngineLogger.DebugContext(
				ctx,
				"message delivered successfully",
			)
		}

	case errors.Is(err, ErrFailed):
		task.Delivery.Failures++
		delay := p.computeBackoff(task.Delivery.Failures)

		if err := p.Driver.PostponeDelivery(
			ctx,
			task.Tx,
			task.Delivery,
			delay,
		); err != nil {
			return fmt.Errorf("unable to postpone delivery: %w", err)
		}

		afterCommit = func() {
			task.EngineLogger.DebugContext(
				ctx,
				"postponed message delivery due to failure",
				slog.Duration("delay", delay),
			)
		}

	case errors.Is(err, ErrBusy):
		delay := p.BackoffBase

		if err := p.Driver.PostponeDelivery(
			ctx,
			task.Tx,
			task.Delivery,
			delay,
		); err != nil {
			return fmt.Errorf("unable to postpone delivery: %w", err)
		}

		afterCommit = func() {
			task.EngineLogger.DebugContext(
				ctx,
				"postponed message delivery due to contention",
				slog.Duration("delay", delay),
			)
		}

	default:
		return err
	}

	if err := task.Tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	afterCommit()

	return nil
}

// computeBackoff returns a delay computed from a capped, equal-jitter
// exponential backoff window for a delivery that has failed n times.
func (p *MessagePump) computeBackoff(n uint64) time.Duration {
	if p.BackoffBase <= 0 || p.BackoffCap <= p.BackoffBase {
		return p.BackoffBase
	}

	limit := p.BackoffBase << n

	// base << iterations may overflow time.Duration (int64 nanoseconds) for
	// large iteration counts, hence the check for limit <= 0.
	if limit <= 0 || limit > p.BackoffCap {
		limit = p.BackoffCap
	}

	return p.BackoffBase + rand.N(limit-p.BackoffBase)
}
