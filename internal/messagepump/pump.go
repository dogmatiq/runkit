package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

const (
	// IdleSleep is the amount of time to wait before attempting to acquire a
	// delivery after a previous attempt failed to acquire one.
	IdleSleep = 25 * time.Millisecond

	// ConcurrencyMultiplier is the number of worker goroutines to spawn per
	// available CPU core, as determined by [runtime.GOMAXPROCS].
	ConcurrencyMultiplier = 10
)

// ErrFailed is returned by [Driver.HandleDelivery] to indicate that the
// delivery failed and should be postponed for redelivery after an exponential
// backoff.
//
// Returning [ErrFailed] increments the delivery's failure count.
var ErrFailed = errors.New("delivery failed")

// ErrBusy is returned by [Driver.HandleDelivery] to indicate that the delivery
// target is temporarily unavailable (e.g. locked by another transaction) and
// should be retried soon.
//
// Returning [ErrBusy] does not increment the delivery's failure count.
var ErrBusy = errors.New("delivery target is busy")

// Driver provides the storage-specific operations that the [MessagePump]
// orchestrates.
type Driver interface {
	// AcquireDelivery attempts to acquire the next pending [Delivery] within
	// tx. It returns ok = false if no delivery is available.
	AcquireDelivery(ctx context.Context, tx *sql.Tx) (del Delivery, ok bool, err error)

	// HandleDelivery processes a [Delivery] within the transaction held by dc.
	//
	// It returns [ErrFailed] to indicate that the delivery should be postponed
	// for redelivery with an incremented failure count, or [ErrBusy] to
	// indicate that the delivery should be retried soon without incrementing
	// the failure count.
	HandleDelivery(ctx context.Context, dc *DeliveryContext) error

	// PostponeDelivery reschedules redelivery for delay from now, recording
	// failures as the delivery's new failure count. It is invoked within the
	// same transaction as the [Driver.HandleDelivery] call that produced the
	// request to postpone.
	PostponeDelivery(
		ctx context.Context,
		dc *DeliveryContext,
		failures uint64,
		delay time.Duration,
	) error
}

// Delivery is a unit of work scoped to the delivery of a single message to a
// single handler.
type Delivery struct {
	MessageID     *uuidpb.UUID
	MessageTypeID *uuidpb.UUID
	EnvelopeBytes []byte
	Failures      uint64

	// StreamID and StreamOffset describe the position of the message within an
	// event stream. They are set by stream-based pumps and are zero for
	// queue-based pumps.
	StreamID     *uuidpb.UUID
	StreamOffset uint64
}

// DeliveryContext encapsulates a [Delivery] and the transaction in which it was
// acquired, along with a logger that is pre-configured with information about the
// delivery.
type DeliveryContext struct {
	Delivery
	Tx     *sql.Tx
	Logger *slog.Logger
}

// MessagePump is a helper for implementing a message pump that acquires and
// handles deliveries concurrently.
type MessagePump struct {
	Driver                  Driver
	DB                      *sql.DB
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (p *MessagePump) Run(ctx context.Context) {
	var (
		group    sync.WaitGroup
		contexts = make(chan *DeliveryContext)
	)

	workers := ConcurrencyMultiplier * runtime.GOMAXPROCS(0)

	// Start the worker goroutines.
	for range workers {
		group.Go(func() {
			for dc := range contexts {
				if err := p.doHandle(ctx, dc); err != nil {
					if ctx.Err() != nil {
						return
					}

					xsql.PanicOnDeadlock(err)

					dc.Logger.ErrorContext(
						ctx,
						"unable to handle delivery",
						xslog.Error(err),
					)
				}
			}
		})
	}

	// Start the dispatcher goroutine.
	group.Go(func() {
		defer close(contexts)

		for {
			// Attempt to acquire a delivery.
			dc, ok, err := p.doAcquire(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				p.Logger.ErrorContext(
					ctx,
					"unable to acquire delivery",
					xslog.Error(err),
				)
			}

			if ok {
				// We found a delivery, dispatch it to a worker.
				dc.Logger.DebugContext(
					ctx,
					"acquired delivery",
				)

				select {
				case <-ctx.Done():
					dc.Tx.Rollback()
					return
				case contexts <- dc:
					continue
				}
			} else {
				// We didn't find a delivery, wait for [IdleSleep] before trying
				// to acquire one again.
				select {
				case <-ctx.Done():
					return
				case <-time.After(IdleSleep):
					continue
				}
			}
		}
	})

	group.Wait()
}

// doAcquire attempts to acquire a [Delivery].
//
// It returns a [DeliveryContext] for the delivery, or false if no delivery was
// available to acquire.
func (p *MessagePump) doAcquire(ctx context.Context) (dc *DeliveryContext, ok bool, err error) {
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	del, ok, err := p.Driver.AcquireDelivery(ctx, tx)
	if err != nil {
		return nil, false, fmt.Errorf("unable to acquire delivery: %w", err)
	}

	if !ok {
		if err := tx.Commit(); err != nil {
			return nil, false, fmt.Errorf("unable to commit transaction: %w", err)
		}

		return nil, false, nil
	}

	attrs := []any{
		xslog.UUID("id", uuidpb.Generate()),
		xslog.UUID("message_id", del.MessageID),
		xslog.UUID("message_type_id", del.MessageTypeID),
		slog.Uint64("attempt", del.Failures+1),
	}

	if del.StreamID != nil {
		attrs = append(
			attrs,
			xslog.UUID("stream_id", del.StreamID),
			slog.Uint64("stream_offset", del.StreamOffset),
		)
	}

	return &DeliveryContext{
		del,
		tx,
		p.Logger.With(
			slog.Group("delivery", attrs...),
		),
	}, true, nil
}

// doHandle handles the delivery within a transaction, rolling back the
// transaction if an error occurs.
//
// If [Driver.HandleDelivery] returns [ErrFailed] or [ErrBusy], the pump invokes
// [Driver.PostponeDelivery] within the same transaction to reschedule
// redelivery, then commits.
func (p *MessagePump) doHandle(ctx context.Context, dc *DeliveryContext) error {
	defer dc.Tx.Rollback()

	err := p.Driver.HandleDelivery(ctx, dc)

	switch {
	case err == nil:
		dc.Logger.DebugContext(
			ctx,
			"handled delivery",
		)

	case errors.Is(err, ErrFailed):
		failures := dc.Failures + 1
		delay := p.computeBackoff(failures)

		if err := p.Driver.PostponeDelivery(ctx, dc, failures, delay); err != nil {
			return fmt.Errorf("unable to postpone delivery: %w", err)
		}

		dc.Logger.DebugContext(
			ctx,
			"postponed redelivery due to failure",
			slog.Duration("delay", delay),
		)

	case errors.Is(err, ErrBusy):
		delay := p.BackoffBase

		if err := p.Driver.PostponeDelivery(ctx, dc, dc.Failures, delay); err != nil {
			return fmt.Errorf("unable to postpone delivery: %w", err)
		}

		dc.Logger.DebugContext(
			ctx,
			"postponed redelivery due to contention",
			slog.Duration("delay", delay),
		)

	default:
		return err
	}

	if err := dc.Tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

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
