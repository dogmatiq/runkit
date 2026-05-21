package engine

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/config/runtimeconfig"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/aggregate"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
	"github.com/dogmatiq/reference-engine/internal/integration"
	"github.com/dogmatiq/reference-engine/internal/process"
	_ "github.com/jackc/pgx/v5/stdlib" // register the "pgx" driver with database/sql
	"golang.org/x/sync/errgroup"
)

// Engine is a Dogma engine backed by a single PostgreSQL database.
//
// It implements [dogma.CommandExecutor].
type Engine struct {
	// App is the Dogma application to run.
	App dogma.Application

	// DSN is the PostgreSQL connection string (DSN or URL).
	DSN string

	// Logger is the structured logger used by the engine.
	//
	// If it is nil, [slog.Default] is used.
	Logger *slog.Logger

	once   sync.Once
	ready  chan struct{}
	db     *sql.DB
	app    *config.Application
	packer *envelopepb.Packer
}

// init lazily creates the ready channel; both Run and ExecuteCommand call it.
func (e *Engine) init() {
	e.once.Do(func() {
		e.ready = make(chan struct{})
	})
}

// Run starts the engine and blocks until ctx is canceled or an unrecoverable
// error occurs. It applies the schema, then runs one goroutine per handler.
//
// Multiple replicas may call Run concurrently against the same database; they
// share work via advisory locks and row-level locking.
func (e *Engine) Run(ctx context.Context) error {
	logger := e.Logger
	if logger == nil {
		logger = slog.Default()
	}
	e.init()

	var err error
	e.db, err = sql.Open("pgx", e.DSN)
	if err != nil {
		return fmt.Errorf("unable to open database: %w", err)
	}
	defer e.db.Close()

	if err := database.ApplySchema(ctx, e.db); err != nil {
		return fmt.Errorf("unable to apply schema: %w", err)
	}

	e.app = runtimeconfig.FromApplication(e.App)
	e.packer = &envelopepb.Packer{Application: e.app.Identity()}

	var controllers []interface {
		Run(context.Context) error
	}

	for _, h := range e.app.Handlers() {
		if !h.IsDisabled() {
			switch h := h.(type) {
			case *config.Aggregate:
				controllers = append(controllers, &aggregate.Controller{
					Config: h,
					DB:     e.db,
					Packer: e.packer,
					Logger: logger,
				})
			case *config.Integration:
				controllers = append(controllers, &integration.Controller{
					Config: h,
					DB:     e.db,
					Packer: e.packer,
					Logger: logger,
				})
			}
		}
	}

	g, ctx := errgroup.WithContext(ctx)

	for _, c := range controllers {
		g.Go(func() error {
			return c.Run(ctx)
		})
	}

	close(e.ready)

	return g.Wait()
}

// ExecuteCommand enqueues a command for execution.
//
// See [dogma.CommandExecutor] for more details.
func (e *Engine) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	e.init()

	// Wait until the engine is running.
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ready:
	}

	// Assert that the application handles this command type.
	if !e.app.RouteSet().HasMessageType(message.TypeOf(command)) {
		panic(fmt.Sprintf(
			"the %q application does not handle %T commands",
			e.app.Identity().GetName(),
			command,
		))
	}

	// Convert the [dogma.ExecuteCommandOption] values into pack options and
	// observer functions.
	var (
		packOptions []envelopepb.PackCommandOption
		observers   []func(context.Context, dogma.Event) (bool, error)
	)
	for _, opt := range options {
		switch o := opt.(type) {
		case dogma.IdempotencyKeyOption:
			packOptions = append(packOptions, envelopepb.WithIdempotencyKey(o.Key()))
		case dogma.EventObserverOption:
			observers = append(observers, o.Observer())
		default:
			panic(fmt.Sprintf("unsupported execute command option type: %T", o))
		}
	}

	// Pack the command into an envelope and add it to the command queue.
	commandEnvelope := e.packer.PackCommand(command, packOptions...)

	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := commandqueue.Enqueue(ctx, tx, commandEnvelope); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	// If there are event observers, capture all stream offsets before
	// committing the transaction, so that we can start observing events that
	// occur after the command is enqueued.
	var observedOffsets *uuidpb.Map[eventstream.Offset]
	if len(observers) != 0 {
		observedOffsets, err = eventstream.Offsets(ctx, tx)
		if err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	// Wait for an event observer to be satisfied, if any were provided.
	if len(observers) != 0 {
		return e.waitForObserver(
			ctx,
			commandEnvelope.GetHeader().GetCorrelationId(),
			observedOffsets,
			observers,
		)
	}

	return nil
}

// waitForObserver observes events with the given correlation ID until any of
// the observers is satisfied, the context ends, or the engine determines that
// no further relevant events can occur.
func (e *Engine) waitForObserver(
	ctx context.Context,
	correlationID *uuidpb.UUID,
	observedOffsets *uuidpb.Map[eventstream.Offset],
	observers []func(context.Context, dogma.Event) (bool, error),
) error {
	for {
		// Check if there are any pending commands within the same correlation
		// ID, which may produce more events.
		nextCommand, hasPendingCommand, err := commandqueue.NextAttemptByCorrelationID(ctx, e.db, correlationID)
		if err != nil {
			return err
		}

		// Check if there are any pending deadlines within the same correlation
		// ID, which may produce more commands.
		nextDeadline, hasPendingDeadline, err := process.NextDeadlineByCorrelationID(ctx, e.db, correlationID)
		if err != nil {
			return err
		}

		// Re-fetch stream offsets to discover any streams created since the
		// snapshot.
		currentOffsets, err := eventstream.Offsets(ctx, e.db)
		if err != nil {
			return err
		}

		// Next, observe any events that have occurred since the last offset on
		// each stream to see if any of them satisfy the observers.
		//
		// IMPORTANT: We must query the event stream _after_ checking for
		// pending commands and deadlines to avoid a race condition. The
		// aggregate subsystem appends events and acknowledges (removes)
		// commands atomically. If we queried events first, a command could
		// complete after that query but before the pending command check,
		// meaning we'd believe there is no way that more relevant events can
		// appear on the stream, but in fact they're already there.
		for eventStreamID, currentOffset := range currentOffsets.All() {
			observedOffset, _ := observedOffsets.Get(eventStreamID) // 0 for newly discovered streams

			if currentOffset <= observedOffset {
				continue
			}

			for eventEnvelope, err := range eventstream.ReadByCorrelationID(
				ctx,
				e.db,
				eventStreamID,
				observedOffset,
				correlationID,
			) {
				if err != nil {
					return err
				}

				event, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
				if err != nil {
					return err
				}

				// Feed the events to each observer, stopping if any of them is
				// satisfied or returns an error.
				for _, fn := range observers {
					ok, err := fn(ctx, event)
					if ok || err != nil {
						return err
					}
				}

				observedOffsets.Set(eventStreamID, eventstream.OffsetOf(eventEnvelope)+1)
			}
		}

		// If there are no pending commands or deadlines, we can't possibly
		// produce any more relevant events, so we can stop observing.
		if !hasPendingCommand && !hasPendingDeadline {
			return dogma.ErrEventObserverNotSatisfied
		}

		// Compute the time at which the next action would occur.
		var nextAction time.Time

		if !hasPendingCommand {
			nextAction = nextDeadline // no command, only a deadline
		} else if !hasPendingDeadline {
			nextAction = nextCommand // no deadline, only a command
		} else if nextCommand.Before(nextDeadline) {
			nextAction = nextCommand // both, but the command is sooner
		} else {
			nextAction = nextDeadline // both, but the deadline is sooner
		}

		if contextDeadline, ok := ctx.Deadline(); ok {
			if nextAction.After(contextDeadline) {
				// The event-producing action won't occur before the context
				// deadline, so we can stop observing now.
				return dogma.ErrEventObserverNotSatisfied
			}
		}

		// gracePeriod is the amount of time added to the sleep interval in an
		// attempt to wait until _after_ the handling of the [nextAction].
		const gracePeriod = 5 * time.Millisecond
		delay := time.Until(nextAction) + gracePeriod

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
