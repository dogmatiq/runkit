package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/message"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/commandqueue"
	"github.com/dogmatiq/reference-engine/internal/database"
	"github.com/dogmatiq/reference-engine/internal/eventstream"
)

// maxConsecutiveIdleTicks is the number of consecutive ticks that a worker can
// perform without doing any work before it shuts down.
const maxConsecutiveIdleTicks = 3

// worker handles commands for one aggregate instance.
type worker struct {
	// Config is the aggregate's configuration.
	Config *config.Aggregate

	// AggregateInstanceID is the ID of the aggregate instance that this worker manages.
	AggregateInstanceID string

	// DB is the database connection that the worker uses.
	DB *sql.DB

	// Packer is used for packing the events that the aggregate records into
	// envelopes.
	Packer *envelopepb.Packer

	// Logger is the target for log messages from both the engine and the
	// application.
	Logger *slog.Logger

	// root is the in-memory state of the aggregate instance that this worker
	// manages.
	root dogma.AggregateRoot

	// offsetAfterLastAppliedEvent is the offset after the most recent event that was
	// applied to root.
	offsetAfterLastAppliedEvent eventstream.Offset

	// snapshotIsStale is true if root has been updated with events that have
	// not been captured in a snapshot.
	snapshotIsStale bool
}

// Run runs the worker until it has performed [maxConsecutiveIdleTicks]
// consecutive ticks without doing any work, or until ctx is canceled.
func (w *worker) Run(ctx context.Context) error {
	w.root = w.Config.Interface().New()
	idleTicks := 0

	for idleTicks < maxConsecutiveIdleTicks {
		didWork, err := w.tick(ctx)
		if err != nil {
			return err
		}

		if didWork {
			idleTicks = 0
		} else {
			idleTicks++
		}
	}

	if w.snapshotIsStale {
		return w.saveSnapshot(ctx)
	}

	return nil
}

func (w *worker) tick(ctx context.Context) (didWork bool, err error) {
	tx, err := w.DB.BeginTx(ctx, nil)
	if err != nil {
		return false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	commandEnvelope, ok, err := w.acquireCommand(ctx, tx)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}

	if err := w.handleCommand(ctx, tx, commandEnvelope); err != nil {
		return false, err
	}

	if err := tx.Commit(); err != nil {
		return false, fmt.Errorf("unable to commit transaction: %w", err)
	}

	return true, nil
}

func (w *worker) acquireCommand(
	ctx context.Context,
	tx *sql.Tx,
) (*envelopepb.Envelope, bool, error) {
	handlerKey := w.Config.Identity().GetKey()

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			i.event_offset_after_last_event,
			i.event_offset_after_snapshot,
			CASE -- Fetch the snapshot data if it's newer than our in-memory state
				WHEN i.event_offset_after_snapshot > $1
				THEN i.snapshot
				ELSE NULL
			END,
			c.message_id,
			c.envelope
		FROM aggregate_instances AS i
		INNER JOIN aggregate_command_routes AS r
			USING (handler_key, instance_id)
		INNER JOIN pending_commands AS c
			USING (message_id)
		WHERE i.handler_key = $2
			AND i.instance_id = $3
			AND c.next_attempt_at <= now()
		ORDER BY c.next_attempt_at
		LIMIT 1
		FOR UPDATE OF i, c`,
		w.offsetAfterLastAppliedEvent,
		database.MarshalUUID(handlerKey),
		w.AggregateInstanceID,
	)

	var (
		offsetAfterLastRecordedEvent eventstream.Offset
		offsetAfterSnapshot          eventstream.Offset
		snapshotData                 []byte
		commandMessageID             = &uuidpb.UUID{}
		commandEnvelope              = &envelopepb.Envelope{}
	)

	if err := row.Scan(
		&offsetAfterLastRecordedEvent,
		&offsetAfterSnapshot,
		&snapshotData,
		database.UnmarshalUUID(commandMessageID),
		database.UnmarshalEnvelope(commandEnvelope),
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("unable to query for pending command: %w", err)
	}

	if err := w.refreshRoot(
		ctx,
		tx,
		offsetAfterLastRecordedEvent,
		offsetAfterSnapshot,
		snapshotData,
	); err != nil {
		return nil, false, err
	}

	return commandEnvelope, true, nil
}

// refreshRoot brings w.root up to date with the latest events.
func (w *worker) refreshRoot(
	ctx context.Context,
	tx *sql.Tx,
	offsetAfterLastRecordedEvent eventstream.Offset,
	offsetAfterSnapshot eventstream.Offset,
	snapshotData []byte,
) error {
	if w.offsetAfterLastAppliedEvent == offsetAfterLastRecordedEvent {
		return nil
	}

	if offsetAfterSnapshot > w.offsetAfterLastAppliedEvent {
		if err := w.root.UnmarshalBinary(snapshotData); err != nil {
			return fmt.Errorf("unable to apply snapshot: %w", err)
		}
		w.offsetAfterLastAppliedEvent = offsetAfterSnapshot
	}

	for eventEnvelope, err := range eventstream.ReadByAggregateInstance(
		ctx,
		tx,
		w.offsetAfterLastAppliedEvent,
		w.Config.Identity().GetKey(),
		w.AggregateInstanceID,
	) {
		if err != nil {
			return err
		}

		ev, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
		if err != nil {
			return fmt.Errorf("unable to unpack event at offset %d: %w", w.offsetAfterLastAppliedEvent, err)
		}

		w.root.ApplyEvent(ev)
		w.offsetAfterLastAppliedEvent = eventstream.OffsetOf(eventEnvelope) + 1
	}

	return nil
}

// handleCommand dispatches the given command to the handler and records any
// events it produces.
func (w *worker) handleCommand(
	ctx context.Context,
	tx *sql.Tx,
	commandEnvelope *envelopepb.Envelope,
) error {
	commandMessageID := commandEnvelope.GetBody().GetMessageId()
	command, err := envelopepb.Unpack[dogma.Command](commandEnvelope)
	if err != nil {
		return err
	}

	mt := message.TypeOf(command)

	// If this handler no longer subscribes to the command type, reset the
	// command so it can be re-routed to a different handler.
	if !w.Config.RouteSet().HasMessageType(mt) {
		return w.resetCommand(ctx, tx, commandMessageID)
	}

	// If the handler's routing function now returns a different instance, reset
	// the command so it can be re-routed to a different instance.
	if aggregateInstanceID := w.Config.Interface().RouteCommandToInstance(command); aggregateInstanceID != w.AggregateInstanceID {
		return w.resetCommand(ctx, tx, commandMessageID)
	}

	packer := w.Packer.PackEffects(
		commandEnvelope,
		w.Config.Identity(),
		envelopepb.WithInstanceID(w.AggregateInstanceID),
	)

	w.Config.Interface().HandleCommand(
		w.root,
		&scope{
			AggregateInstanceID: w.AggregateInstanceID,
			Root:                w.root,
			Packer:              packer,
			Logger:              w.Logger,
		},
		command,
	)

	if envelopes, ok := packer.Seal(); ok {
		offsetAfterLastEvent, err := eventstream.Append(ctx, tx, envelopes)
		if err != nil {
			return err
		}

		if _, err := tx.ExecContext(
			ctx,
			`UPDATE aggregate_instances SET
				event_offset_after_last_event = $1
			WHERE handler_key = $2
				AND instance_id = $3`,
			offsetAfterLastEvent,
			database.MarshalUUID(w.Config.Identity().GetKey()),
			w.AggregateInstanceID,
		); err != nil {
			return fmt.Errorf("unable to update offset after last event: %w", err)
		}

		w.offsetAfterLastAppliedEvent = offsetAfterLastEvent
		w.snapshotIsStale = true
	}

	return commandqueue.Ack(ctx, tx, commandMessageID)
}

// saveSnapshot saves a snapshot of the aggregate's state.
//
// It does not replace an existing snapshot if the snapshot in the database is
// newer than the snapshot taken from w.root.
func (w *worker) saveSnapshot(ctx context.Context) error {
	snapshotData, err := w.root.MarshalBinary()
	if err != nil {
		if errors.Is(err, dogma.ErrNotSupported) {
			return nil
		}
		return fmt.Errorf("unable to marshal snapshot: %w", err)
	}

	if _, err := w.DB.ExecContext(
		ctx,
		`UPDATE aggregate_instances SET
			event_offset_after_snapshot = $1,
			snapshot = $2
		WHERE handler_key = $3
			AND instance_id = $4
			AND event_offset_after_snapshot < $1`,
		w.offsetAfterLastAppliedEvent,
		snapshotData,
		database.MarshalUUID(w.Config.Identity().GetKey()),
		w.AggregateInstanceID,
	); err != nil {
		return fmt.Errorf("unable to update snapshot: %w", err)
	}

	return nil
}

// resetCommand removes the routing entry for a command so it can be re-routed
// to a different handler or instance.
func (w *worker) resetCommand(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM aggregate_command_routes
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to delete route for command %s: %w", messageID, err)
	}

	return nil
}
