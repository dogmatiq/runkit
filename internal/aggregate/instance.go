package aggregate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strings"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// instance is the engine's model of an aggregate instance, as stored in the
// aggregate.instances table.
//
// Its root is the application's model of the instance.
type instance struct {
	// HandlerKey is the identity key of the instance's handler.
	HandlerKey *uuidpb.UUID

	// ID is the instance's ID.
	ID string

	// StreamID is the ID of the event stream that holds the instance's events,
	// or nil if the instance hadn't recorded any events when it was locked.
	StreamID *uuidpb.UUID

	// Root is the instance's root.
	Root dogma.AggregateRoot
}

// RecordEvents appends the given events to the instance's event stream,
// binding the instance to a stream if it isn't bound to one, and stores a
// snapshot of its root, in a single database round-trip.
//
// It doesn't store a snapshot if the root can't be marshaled.
func (i *instance) RecordEvents(
	ctx context.Context,
	tx *sql.Tx,
	engineLogger *slog.Logger,
	eventEnvelopes *envelopepb.MultiEnvelope,
) error {
	var snapshotArg any
	if data, ok := i.marshalSnapshot(ctx, engineLogger); ok {
		snapshotArg = data
	}

	var (
		query strings.Builder
		args  []any
	)

	args = append(
		args,
		xsql.UUID(i.HandlerKey),
		i.ID,
		xsql.UUID(i.StreamID),
		xsql.UUID(eventEnvelopes.GetHeader().GetCorrelationId()),
		snapshotArg,
	)

	query.WriteString(`SELECT aggregate.complete_with_events($1, $2, $3, $4, $5, ARRAY[`)

	first := true
	for eventEnvelope := range eventEnvelopes.All() {
		if first {
			first = false
		} else {
			query.WriteString(", ")
		}

		n := len(args)
		fmt.Fprintf(
			&query,
			"ROW($%d, $%d, $%d)::eventstream.event",
			n+1, n+2, n+3,
		)

		args = append(
			args,
			xsql.UUID(eventEnvelope.GetBody().GetMessageId()),
			xsql.UUID(eventEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(eventEnvelope),
		)
	}

	query.WriteString(`])`)

	if _, err := tx.ExecContext(ctx, query.String(), args...); err != nil {
		return fmt.Errorf("unable to record events: %w", err)
	}

	return nil
}

// DeleteIfNoEvents deletes the instance if it has never recorded any events.
func (i *instance) DeleteIfNoEvents(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(
		ctx,
		`SELECT aggregate.complete_without_events($1, $2)`,
		xsql.UUID(i.HandlerKey),
		i.ID,
	); err != nil {
		return fmt.Errorf("unable to delete aggregate instance: %w", err)
	}

	return nil
}

// ApplyHistoricalEvents applies the instance's events to its root, starting at
// the given offset in its event stream.
//
// It returns [messagequeue.ErrFailed] if an event can't be unmarshaled or
// applied to the root.
func (i *instance) ApplyHistoricalEvents(
	ctx context.Context,
	tx *sql.Tx,
	engineLogger *slog.Logger,
	offset uint64,
) error {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT
			e.message_id,
			e.envelope
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
			AND e.stream_offset >= $2
			AND e.aggregate_handler_key = $3
			AND e.aggregate_instance_id = $4
		ORDER BY e.stream_offset`,
		xsql.UUID(i.StreamID),
		offset,
		xsql.UUID(i.HandlerKey),
		i.ID,
	)
	if err != nil {
		return fmt.Errorf("unable to query events: %w", err)
	}
	defer rows.Close()

	// Apply the events to the root in order.
	for rows.Next() {
		var (
			eventMessageID     = &uuidpb.UUID{}
			eventEnvelopeBytes []byte
		)

		if err := rows.Scan(
			xsql.UUID(eventMessageID),
			&eventEnvelopeBytes,
		); err != nil {
			return fmt.Errorf("unable to scan event: %w", err)
		}

		var (
			eventEnvelope = &envelopepb.Envelope{}
			eventForApply dogma.Event
		)

		if err := xmessage.Unpack(
			eventEnvelopeBytes,
			eventEnvelope,
			&eventForApply,
		); err != nil {
			engineLogger.ErrorContext(
				ctx,
				"unable to unmarshal event",
				slog.Group(
					"event",
					xslog.UUID("message_id", eventMessageID),
				),
				xslog.Error(err),
			)

			return messagequeue.ErrFailed
		}

		if err := xerrors.Recover(
			func() error {
				i.Root.ApplyEvent(eventForApply)
				return nil
			},
		); err != nil {
			engineLogger.ErrorContext(
				ctx,
				"unable to apply event to aggregate root",
				slog.Group(
					"event",
					xslog.UUID("message_id", eventMessageID),
				),
				xslog.Error(err),
			)

			return messagequeue.ErrFailed
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("unable to iterate historical events: %w", err)
	}

	return nil
}

// ApplySnapshot restores the instance's root from the given snapshot. It
// returns false if the root doesn't support snapshots, or can't unmarshal the
// snapshot.
func (i *instance) ApplySnapshot(
	ctx context.Context,
	engineLogger *slog.Logger,
	snap snapshot,
) bool {
	if err := xerrors.Recover(
		func() error {
			return i.Root.UnmarshalBinary(snap.Data)
		},
	); err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			engineLogger.ErrorContext(
				ctx,
				"unable to unmarshal snapshot",
				xslog.Error(err),
			)
		}

		return false
	}

	return true
}

// marshalSnapshot returns the binary representation of the instance's root, as
// returned by its MarshalBinary method. It returns false if the root doesn't
// support snapshots, or can't be marshaled.
func (i *instance) marshalSnapshot(
	ctx context.Context,
	engineLogger *slog.Logger,
) ([]byte, bool) {
	data, err := xerrors.RecoverT(
		func() ([]byte, error) {
			return i.Root.MarshalBinary()
		},
	)
	if err != nil {
		if !errors.Is(err, dogma.ErrNotSupported) {
			engineLogger.ErrorContext(
				ctx,
				"unable to marshal snapshot",
				xslog.Error(err),
			)
		}

		return nil, false
	}

	return data, true
}

// snapshot is a marshaled copy of an aggregate instance's root, taken after the
// root applied the events before a given offset in the instance's event stream.
type snapshot struct {
	// Data is the binary representation of the root, as returned by its
	// MarshalBinary method.
	Data []byte

	// NextOffset is the offset of the first event in the instance's stream that
	// the snapshot doesn't include. It's zero if the instance has no snapshot.
	NextOffset uint64
}
