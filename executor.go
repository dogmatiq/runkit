package dogmaengine

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/contexthook"
	"github.com/dogmatiq/reference-engine/internal/x/xmessage"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// ExecuteCommand submits a [Command] for execution.
//
// It returns once the engine has taken ownership of the command. By
// default, it doesn't wait for handling to finish.
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
	commandEnvelope := e.packer.PackCommand(command, packOptions...)

	contexthook.Invoke(ctx, contexthook.ExecuteCommand{
		CommandEnvelope: commandEnvelope,
	})

	if err := command.Validate(
		xmessage.ValidationScope{
			IsNewMessage: true,
			Envelope:     commandEnvelope,
		},
	); err != nil {
		return err
	}

	if _, ok := e.commandTypes[reflect.TypeOf(command)]; !ok {
		return fmt.Errorf("no route found for command type: %T", command)
	}

	var messageID *uuidpb.UUID

	if err := xsql.Transact(
		ctx,
		e.DB,
		func(ctx context.Context, tx *sql.Tx) error {
			row := tx.QueryRowContext(
				ctx,
				`SELECT
					actual_message_id,
					enqueued
				FROM commandqueue.add($1, $2, $3, $4, $5)`,
				xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
				xsql.UUID(commandEnvelope.GetHeader().GetCorrelationId()),
				xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
				xsql.Envelope(commandEnvelope),
				commandEnvelope.GetBody().GetIdempotencyKey(),
			)

			messageID = &uuidpb.UUID{}
			var ok bool
			if err := row.Scan(xsql.UUID(messageID), &ok); err != nil {
				return fmt.Errorf("unable to add command to queue: %w", err)
			}

			if ok {
				e.Logger.InfoContext(
					ctx,
					command.MessageDescription(),
					xslog.Envelope("command", commandEnvelope),
				)
			} else {
				e.Logger.DebugContext(
					ctx,
					"command deduplicated",
					xslog.Envelope("command", commandEnvelope),
					xslog.UUID("duplicate_message_id", messageID),
				)
			}

			return nil
		},
	); err != nil {
		return err
	}

	if len(eventObservers) == 0 {
		return nil
	}

	return waitForEvents(
		ctx,
		e.DB,
		messageID,
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

// waitForEvents waits for events that satisfy the given observers, or until the
// context expires.
func waitForEvents(
	ctx context.Context,
	q xsql.Querier,
	correlationID *uuidpb.UUID,
	eventTypes uuidpb.Set,
	eventObservers []dogma.EventObserver[dogma.Event],
) error {
	var nextOffsets uuidpb.Map[uint64]

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
	q xsql.Querier,
	correlationID *uuidpb.UUID,
) (at time.Time, ok bool, err error) {
	// TODO: union with deadlines table when they're implemented.
	row := q.QueryRowContext(
		ctx,
		`SELECT execute_at
		FROM commandqueue.commands
		WHERE correlation_id = $1
		ORDER BY execute_at
		LIMIT 1`,
		xsql.UUID(correlationID),
	)

	if err := row.Scan(&at); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return time.Time{}, false, nil
		}
		return time.Time{}, false, fmt.Errorf("unable to query pending commands: %w", err)
	}

	return at, true, nil
}

// pollForEvents observes any events that have occurred since the last observed
// offset on each stream, and feeds them to the observer functions.
//
// It returns true if any of the observer functions is satisfied.
func pollForEvents(
	ctx context.Context,
	q xsql.Querier,
	correlationID *uuidpb.UUID,
	eventTypes uuidpb.Set,
	observers []dogma.EventObserver[dogma.Event],
	nextOffsets uuidpb.Map[uint64],
) (satisfied bool, err error) {
	var (
		streamIDs    []string
		offsets      []uint64
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
			e.stream_id,
			e.stream_offset,
			e.envelope
		FROM eventstream.events AS e
		LEFT JOIN offsets AS o
			ON o.stream_id = e.stream_id
		WHERE e.stream_offset >= COALESCE(o.next_offset, 0)
			AND e.correlation_id = $3
			AND e.message_type_id = ANY($4)
		ORDER BY e.stream_offset
		LIMIT 100`,
		streamIDs,
		offsets,
		xsql.UUID(correlationID),
		eventTypeIDs,
	)
	if err != nil {
		return false, fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			streamID      = &uuidpb.UUID{}
			offset        uint64
			eventEnvelope = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			xsql.UUID(streamID),
			&offset,
			xsql.Envelope(eventEnvelope),
		); err != nil {
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

		nextOffsets.Set(streamID, offset+1)
	}

	if err := rows.Err(); err != nil {
		return false, fmt.Errorf("unable to iterate over event rows: %w", err)
	}

	return false, nil
}
