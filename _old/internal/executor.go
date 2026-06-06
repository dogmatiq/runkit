package engine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/aggregate"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
)

// ExecuteCommand enqueues a command for execution.
//
// See [dogma.CommandExecutor] for more details.
func (e *Engine) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	if err := e.ready.WaitContext(ctx); err != nil {
		return err
	}

	packOptions, eventTypes, eventObservers := e.resolveExecuteCommandOptions(options)
	handlerKey, aggregateInstanceID := e.routeCommand(command)
	commandEnvelope := e.packer.PackCommand(command, packOptions...)

	if err := database.Transact(
		ctx,
		e.db,
		func(ctx context.Context, tx *sql.Tx) error {
			if aggregateInstanceID != nil {
				if err := aggregate.EnsureInstanceExists(
					ctx,
					tx,
					handlerKey,
					*aggregateInstanceID,
				); err != nil {
					return err
				}
			}

			return commandqueue.Enqueue(
				ctx,
				tx,
				commandEnvelope,
				handlerKey,
				aggregateInstanceID,
			)
		},
	); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	if len(eventObservers) == 0 {
		return nil
	}

	return waitForEvents(
		ctx,
		e.db,
		commandEnvelope.GetHeader().GetCorrelationId(),
		eventTypes,
		eventObservers,
	)
}

// resolveExecuteCommandOptions resolves the given [dogma.ExecuteCommandOption]
// into types used internally by the engine.
func (e *Engine) resolveExecuteCommandOptions(
	options []dogma.ExecuteCommandOption,
) (
	packOptions []envelopepb.PackCommandOption,
	eventTypes uuidpb.Set,
	eventObservers []dogma.EventObserver[dogma.Event],
) {
	for _, opt := range options {
		switch opt := opt.(type) {
		case dogma.IdempotencyKeyOption:
			packOptions = append(packOptions, envelopepb.WithIdempotencyKey(opt.Key()))

		case dogma.EventObserverOption:
			eventObservers = append(eventObservers, opt.Observer())
			eventTypes.Add(
				uuidpb.MustParse(opt.EventType().ID()),
			)

		default:
			panic(fmt.Sprintf("unsupported execute command option type: %T", opt))
		}
	}

	return packOptions, eventTypes, eventObservers
}

// routeCommand returns the target handler (and optional aggregate instance ID) for a command.
func (e *Engine) routeCommand(command dogma.Command) (*uuidpb.UUID, *string) {
	messageType, ok := dogma.RegisteredMessageTypeOf(command)
	if !ok {
		panic(fmt.Sprintf(
			"%T is not in the message registry",
			command,
		))
	}

	commandRoutes := e.app.
		RouteSet().
		Filter(config.FilterByRouteType(config.HandlesCommandRouteType)).
		Routes()

	for route, handler := range commandRoutes {
		if route.MessageTypeID.Get() != messageType.ID() {
			continue
		}

		var aggregateInstanceID *string
		if handler, ok := handler.(*config.Aggregate); ok {
			aggregateInstanceID = new(
				handler.Interface().RouteCommandToInstance(command),
			)
		}

		return handler.Identity().GetKey(), aggregateInstanceID
	}

	panic(fmt.Sprintf(
		"the application does not handle %T commands",
		command,
	))
}

// waitForEvents waits for events that satisfy the given observers, or until the
// context expires.
func waitForEvents(
	ctx context.Context,
	q database.Executor,
	correlationID *uuidpb.UUID,
	eventTypes uuidpb.Set,
	eventObservers []dogma.EventObserver[dogma.Event],
) error {
	var nextOffsets uuidpb.Map[eventstream.Offset]

	for {
		// First, check if there are any pending commands or deadlines within this
		// causal chain, which may produce more events.
		possibleFutureEventAt, hasPossibleFutureEvent, err := possibleFutureEventAt(
			ctx,
			q,
			correlationID,
		)
		if err != nil {
			return err
		}

		// Next, observe any events that have occurred since the last observed
		// offset on each stream to see if any of them satisfy the observers.
		//
		// IMPORTANT: We must query the event _after_ checking for pending
		// commands/deadlines to avoid a race condition. If we queried events
		// first, a command/deadline could complete after that query but before
		// the pending command/deadline check. We'd believe there is no way that
		// more relevant events can appear on the stream, but in fact they're
		// already there.
		observerIsSatisfied, err := pollForEvents(
			ctx,
			q,
			correlationID,
			eventTypes,
			eventObservers,
			nextOffsets,
		)
		if observerIsSatisfied || err != nil {
			return err
		}

		if !hasPossibleFutureEvent {
			return dogma.ErrEventObserverNotSatisfied
		}

		// Wait until the next potential event-producing action occurs, or the
		// context deadline expires.
		//
		// We add 50ms to provide a minimum sleep, and to allow the future
		// event-producing action time to record its events before we check
		// again.
		sleepUntil := possibleFutureEventAt.Add(50 * time.Millisecond)

		// If the possible future event-producing action won't occur before the
		// context deadline we don't bother waiting.
		if contextDeadline, ok := ctx.Deadline(); ok {
			if sleepUntil.After(contextDeadline) {
				return dogma.ErrEventObserverNotSatisfied
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(sleepUntil)):
			// try again
		}
	}
}

// possibleFutureEventAt returns the time at which the next command or deadline
// with the given correlation is scheduled to be handled.
func possibleFutureEventAt(
	ctx context.Context,
	q database.Executor,
	correlationID *uuidpb.UUID,
) (at time.Time, ok bool, err error) {
	// TODO: union with deadlines table when they're implemented.
	row := q.QueryRowContext(
		ctx,
		`SELECT next_attempt_at
		FROM pending_commands
		WHERE correlation_id = $1
		ORDER BY next_attempt_at
		LIMIT 1`,
		database.MarshalUUID(correlationID),
	)

	if err := row.Scan(&at); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("unable to query pending commands and deadlines: %w", err)
	}

	return at, true, nil
}

// pollForEvents observes any events that have occurred since the last observed
// offset on each stream, and feeds them to the observer functions.
//
// It returns true if any of the observer functions is satisfied.
func pollForEvents(
	ctx context.Context,
	q database.Executor,
	correlationID *uuidpb.UUID,
	eventTypes uuidpb.Set,
	observers []dogma.EventObserver[dogma.Event],
	nextOffsets uuidpb.Map[eventstream.Offset],
) (satisfied bool, err error) {
	var (
		streamIDs    []string
		offsets      []eventstream.Offset
		eventTypeIDs []string
	)

	for streamID, offset := range nextOffsets.All() {
		streamIDs = append(streamIDs, streamID.AsString())
		offsets = append(offsets, offset)
	}

	for eventTypeID := range eventTypes.All() {
		eventTypeIDs = append(eventTypeIDs, eventTypeID.AsString())
	}

	rows, err := q.QueryContext(
		ctx,
		`WITH offsets AS (
			SELECT *
			FROM unnest(
				$1::uuid[],
				$2::bigint[]
			) AS o(
			 	stream_id,
				next_offset
			)
		)
		SELECT
			envelope
		FROM events AS e
		LEFT JOIN offsets AS o
			ON o.stream_id = e.event_stream_id
		WHERE e.offset >= COALESCE(o.next_offset, 0)
			AND e.correlation_id = $3
			AND e.message_type_id = ANY($4)
		ORDER BY e.offset
		LIMIT 100`,
		streamIDs,
		offsets,
		database.MarshalUUID(correlationID),
		eventTypeIDs,
	)
	if err != nil {
		return false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		eventEnvelope := &envelopepb.Envelope{}
		if err := rows.Scan(database.UnmarshalEnvelope(eventEnvelope)); err != nil {
			return false, fmt.Errorf("unable to scan event envelope: %w", err)
		}

		event, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
		if err != nil {
			return false, err
		}

		// Feed the events to each observer function, stopping if any of them is
		// satisfied or returns an error.
		for _, fn := range observers {
			satisfied, err := fn(ctx, event)
			if satisfied || err != nil {
				return satisfied, err
			}
		}

		pos, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](eventEnvelope.GetBody())
		if err != nil {
			return false, fmt.Errorf("unexpected error reading event stream position: %w", err)
		}
		if !ok {
			return false, fmt.Errorf("event envelope does not have an event stream position")
		}

		nextOffsets.Set(
			pos.GetStreamId(),
			eventstream.Offset(pos.GetOffset()+1),
		)
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("unable to iterate over event rows: %w", err)
	}

	return false, nil
}
