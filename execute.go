package runkit

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
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// eventObserverPollInterval is the time between successive polls for events
// that may satisfy an event observer.
const eventObserverPollInterval = 25 * time.Millisecond

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

	if len(eventObservers) != 0 {
		_, ok := ctx.Deadline()
		if !ok {
			return errors.New("context must have a deadline when using event observers")
		}
	}

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
				FROM commandqueue.add($1, $2, $3, $4)`,
				xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
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

	if err := waitForEvents(
		ctx,
		e.DB,
		messageID,
		eventTypes,
		eventObservers,
	); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return dogma.ErrEventObserverNotSatisfied
		}

		return err
	}

	return nil
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
//
// This reference implementation does not attempt to detect when no further
// events are possible; only the context deadline bounds the wait time.
func waitForEvents(
	ctx context.Context,
	q xsql.Querier,
	correlationID *uuidpb.UUID,
	eventTypes uuidpb.Set,
	eventObservers []dogma.EventObserver[dogma.Event],
) error {
	var nextOffsets uuidpb.Map[uint64]

	for {
		satisfied, err := pollForEvents(
			ctx,
			q,
			correlationID,
			eventTypes,
			eventObservers,
			nextOffsets,
		)
		if satisfied || err != nil {
			return err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(eventObserverPollInterval):
		}
	}
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
		streamIDs []string
		offsets   []uint64
	)

	for streamID, offset := range nextOffsets.All() {
		streamIDs = append(streamIDs, streamID.AsString())
		offsets = append(offsets, offset)
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
		xsql.UUIDSeq(eventTypes.All()),
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
