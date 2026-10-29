package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
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

	// Logger is the target for log messages produced by the pump and the
	// supervisors of the dispatcher and workers it starts.
	Logger *slog.Logger
}

type deliveryTask struct {
	Tx       *sql.Tx
	Delivery Delivery
	Envelope *envelopepb.Envelope
	Logger   *slog.Logger
}

// Run runs the message pump until ctx is canceled.
//
// It must be called at most once.
func (p *MessagePump) Run(ctx context.Context) error {
	var group supervisor.Group
	tasks := make(chan *deliveryTask)

	for range max(1, p.Workers) {
		group.Go(ctx, p.Logger, func(ctx context.Context) error {
			return p.runWorker(ctx, tasks)
		})
	}

	group.Go(ctx, p.Logger, func(ctx context.Context) error {
		return p.runDispatcher(ctx, tasks)
	})

	group.Wait()

	return ctx.Err()
}

// runWorker handles the tasks sent by the dispatcher until ctx is canceled.
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
				if ctx.Err() != nil {
					return ctx.Err()
				}

				// Log the error and move on to the next task, so that one
				// failed delivery does not stall the pump. Returning the error
				// would restart the worker after the supervisor's backoff,
				// during which the dispatcher would block, holding a delivery
				// it has already acquired, and with a single worker the
				// handler would make no progress at all.
				//
				// The supervisor never sees the error, so check for a deadlock
				// here instead.
				xsql.PanicOnDeadlock(err)

				task.Logger.ErrorContext(
					ctx,
					"unable to deliver message",
					xslog.Error(err),
				)
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

		task.Logger.DebugContext(
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

		if del.Stream != nil {
			attrs = append(
				attrs,
				xslog.UUID("stream_id", del.Stream.ID),
				slog.Uint64("event_offset", del.Stream.EventOffset),
				slog.Uint64("checkpoint_offset", del.Stream.CheckpointOffset),
			)
		}

		logger := p.Logger.With(slog.Group("delivery", attrs...))

		envelope := &envelopepb.Envelope{}
		err = envelope.UnmarshalBinary(del.EnvelopeBytes)
		if err == nil {
			err = envelope.Validate()
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
			envelope,
			logger.With(
				xslog.Envelope("message", envelope),
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
		task.Logger,
	)

	var afterCommit func()

	switch {
	case err == nil:
		afterCommit = func() {
			task.Logger.DebugContext(
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
			task.Logger.DebugContext(
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
			task.Logger.DebugContext(
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
