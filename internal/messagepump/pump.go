package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// MessagePump is a helper for implementing a message pump that acquires and
// handles deliveries concurrently.
type MessagePump struct {
	Driver                  Driver
	DB                      *sql.DB
	Workers                 int
	PollInterval            time.Duration
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

type deliveryTask struct {
	Tx       *sql.Tx
	Delivery Delivery
	Envelope *envelopepb.Envelope
	Logger   *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (p *MessagePump) Run(ctx context.Context) {
	var (
		group sync.WaitGroup
		tasks = make(chan *deliveryTask)
	)

	for range max(1, p.Workers) {
		group.Go(func() {
			p.runWorker(ctx, tasks)
		})
	}

	group.Go(func() {
		p.runDispatcher(ctx, tasks)
	})

	group.Wait()
}

// runWorker consumes tasks from the dispatcher and hands each to [doHandle].
//
// It exits when tasks is closed by the dispatcher, or when ctx is canceled
// mid-handling.
func (p *MessagePump) runWorker(ctx context.Context, tasks <-chan *deliveryTask) {
	for task := range tasks {
		if err := p.doHandle(ctx, task); err != nil {
			if ctx.Err() != nil {
				return
			}

			xsql.PanicOnDeadlock(err)

			task.Logger.ErrorContext(
				ctx,
				"unable to deliver message",
				xslog.Error(err),
			)
		}
	}
}

// runDispatcher acquires deliveries and sends them to workers via tasks. It
// closes tasks on exit.
func (p *MessagePump) runDispatcher(ctx context.Context, tasks chan<- *deliveryTask) {
	defer close(tasks)

	var failures uint64

	for {
		delay := p.PollInterval

		task, ok, err := p.doAcquireTask(ctx)

		if err != nil {
			if ctx.Err() != nil {
				return
			}

			failures++
			delay = p.computeBackoff(failures)

			p.Logger.ErrorContext(
				ctx,
				"unable to acquire message for delivery",
				xslog.Error(err),
				slog.Uint64("attempt", failures),
				slog.Duration("delay", delay),
			)
		} else {
			failures = 0
		}

		if ok {
			task.Logger.DebugContext(
				ctx,
				"acquired message for delivery",
			)

			select {
			case <-ctx.Done():
				task.Tx.Rollback()
				return
			case tasks <- task:
				// Set delay to zero so that we poll again immediately after
				// successful task dispatch.
				delay = 0
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
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
