package commandexecutor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"time"
	"uuid"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/runkit/internal/cluster"
	"github.com/dogmatiq/runkit/internal/contexthook"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Executor implements [dogma.CommandExecutor] for the engine. It adds each
// command to its queue and, when asked, waits for the command's event
// observers.
type Executor struct {
	// DB is the database connection used to add commands to queues and to read
	// the events that they cause.
	DB *sql.DB

	// Node is the node that receives the cluster-wide signal sent when events
	// with a given correlation ID are appended.
	Node *cluster.Node

	// EngineStopped is closed when the engine stops. Once Node stops, the
	// executor no longer receives the signals that it waits for.
	EngineStopped <-chan struct{}

	// Packer packs each command into an envelope.
	Packer *envelope.Packer

	// QueueRouter chooses the queue for each command.
	QueueRouter *messagequeue.Router

	// CommandTypes is the set of command types that the executor accepts.
	CommandTypes map[reflect.Type]struct{}

	// EngineLogger is the target for the engine's own log records.
	EngineLogger *slog.Logger

	// AppLogger is the target for the application's log records.
	AppLogger *slog.Logger
}

// ExecuteCommand submits a [Command] for execution.
//
// See [dogma.CommandExecutor] for more details.
func (e *Executor) ExecuteCommand(
	ctx context.Context,
	command dogma.Command,
	options ...dogma.ExecuteCommandOption,
) error {
	if _, ok := e.CommandTypes[reflect.TypeOf(command)]; !ok {
		return fmt.Errorf("no route found for command type: %T", command)
	}

	packOptions, eventObservers := resolveOptions(options)
	commandEnvelope := e.Packer.PackCommand(command, packOptions...)

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

	var (
		messageID uuid.UUID
		enqueued  bool
	)

	if err := xsql.Transact(
		ctx,
		e.DB,
		func(ctx context.Context, tx *sql.Tx) error {
			queueID, err := e.QueueRouter.RouteCommandToQueue(ctx, tx, commandEnvelope)
			if err != nil {
				return err
			}

			row := tx.QueryRowContext(
				ctx,
				`SELECT
					actual_message_id,
					enqueued
				FROM runkit.enqueue_command($1, $2, $3, $4, $5, $6)`,
				xsql.UUID(commandEnvelope.GetBody().GetMessageId().AsStd()),
				xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId().AsStd()),
				xsql.UUID(queueID),
				xsql.Envelope(commandEnvelope),
				commandEnvelope.GetBody().GetIdempotencyKey(),
				time.Now(),
			)

			if err := row.Scan(xsql.UUID(&messageID), &enqueued); err != nil {
				return fmt.Errorf("unable to add command to queue: %w", err)
			}

			return nil
		},
	); err != nil {
		return err
	}

	if enqueued {
		e.AppLogger.InfoContext(
			ctx,
			command.MessageDescription(),
			xslog.Envelope("produced_message", commandEnvelope),
		)
	} else {
		e.EngineLogger.DebugContext(
			ctx,
			"command deduplicated",
			xslog.Envelope("produced_message", commandEnvelope),
			xslog.UUID("duplicate_message_id", messageID),
		)
	}

	if len(eventObservers) == 0 {
		return nil
	}

	return e.observe(ctx, messageID, eventObservers)
}

// resolveOptions resolves the given [dogma.ExecuteCommandOption] into types
// used internally by the executor.
func resolveOptions(
	options []dogma.ExecuteCommandOption,
) (
	packOptions []envelope.PackCommandOption,
	eventObservers []dogma.EventObserverOption,
) {
	for _, opt := range options {
		switch opt := opt.(type) {
		case dogma.IdempotencyKeyOption:
			packOptions = append(packOptions, envelope.WithIdempotencyKey(opt.Key()))

		case dogma.EventObserverOption:
			eventObservers = append(eventObservers, opt)

		default:
			panic(fmt.Sprintf("unsupported execute command option type: %T", opt))
		}
	}

	return packOptions, eventObservers
}
