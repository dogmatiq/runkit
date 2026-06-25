package messagepump

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
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

// AcquireFunc is a function that attempts to acquire a delivery.
type AcquireFunc func(context.Context, *sql.Tx) (Delivery, bool, error)

// HandleFunc is a function that handles a delivery.
type HandleFunc func(context.Context, *DeliveryContext) error

// Delivery is a unit of work scoped to the delivery of a single message to a
// single handler.
type Delivery struct {
	MessageID     *uuidpb.UUID
	MessageTypeID *uuidpb.UUID
	EnvelopeBytes []byte
	FailureCount  uint64
}

// DeliveryContext encapsulates a [Delivery] and the transaction in which it was
// acquired, along with a logger that is pre-configured with information about the
// delivery.
type DeliveryContext struct {
	Delivery
	Tx     *sql.Tx
	Logger *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func Run(
	ctx context.Context,
	db *sql.DB,
	logger *slog.Logger,
	acquire AcquireFunc,
	handle HandleFunc,
) {
	var (
		group    sync.WaitGroup
		contexts = make(chan *DeliveryContext)
	)

	workers := ConcurrencyMultiplier * runtime.GOMAXPROCS(0)

	// Start the worker goroutines.
	for range workers {
		group.Go(func() {
			for dc := range contexts {
				if err := doHandle(ctx, handle, dc); err != nil {
					if ctx.Err() != nil {
						return
					}

					xsql.PanicOnDeadlock(err)

					dc.Logger.ErrorContext(
						ctx,
						"unable to handle delivery",
						xslog.Error(err),
					)
				} else {
					dc.Logger.DebugContext(
						ctx,
						"handled delivery",
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
			dc, ok, err := doAcquire(ctx, db, logger, acquire)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				logger.ErrorContext(
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
func doAcquire(
	ctx context.Context,
	db *sql.DB,
	logger *slog.Logger,
	acquire AcquireFunc,
) (dc *DeliveryContext, ok bool, err error) {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		if !ok {
			tx.Rollback()
		}
	}()

	del, ok, err := acquire(ctx, tx)
	if err != nil {
		return nil, false, fmt.Errorf("unable to acquire delivery: %w", err)
	}

	if !ok {
		return nil, false, nil
	}

	return &DeliveryContext{
		del,
		tx,
		logger.With(
			slog.Group(
				"delivery",
				xslog.UUID("id", uuidpb.Generate()),
				xslog.UUID("message_id", del.MessageID),
				xslog.UUID("message_type_id", del.MessageTypeID),
				slog.Uint64("attempt", del.FailureCount+1),
			),
		),
	}, true, nil
}

// doHandle handles the delivery within a transaction, rolling back the
// transaction if an error occurs.
func doHandle(
	ctx context.Context,
	handle HandleFunc,
	dc *DeliveryContext,
) error {
	defer dc.Tx.Rollback()

	if err := handle(ctx, dc); err != nil {
		return err
	}

	if err := dc.Tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}
