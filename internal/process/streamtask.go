package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

type streamTask struct {
	Tx                      *sql.Tx
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	StreamID                *uuidpb.UUID
	CheckpointOffset        uint64
	EventTypeIDs            []string
	BackoffBase, BackoffCap time.Duration
	ParentLogger, Logger    *slog.Logger
}

var errFailed = errors.New("unable to handle event")

// Execute processes the task by handling pending events on the stream and
// committing the transaction.
func (t *streamTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	t.Logger = t.ParentLogger.With(
		xslog.UUID("stream_id", t.StreamID),
		slog.Uint64("checkpoint_offset", t.CheckpointOffset),
	)

	eventEnvelopes, err := t.fetchEvents(ctx)
	if err != nil {
		return err
	}

	if len(eventEnvelopes) != 0 {
		handleErr := t.handleEvents(ctx, eventEnvelopes)
		if handleErr != nil && !errors.Is(handleErr, errFailed) {
			return handleErr
		}

		if err := xsql.ExecOne(
			ctx,
			t.Tx,
			`UPDATE eventstream.handler_checkpoints SET
				checkpoint_offset = $1,
				failures = 0
			WHERE handler_key = $2
			AND stream_id = $3`,
			t.CheckpointOffset,
			xsql.UUID(t.Identity.GetKey()),
			xsql.UUID(t.StreamID),
		); err != nil {
			return fmt.Errorf("unable to update handler checkpoint: %w", err)
		}

		if handleErr != nil {
			if err := t.failAndPostpone(ctx); err != nil {
				return err
			}
		}
	}

	if err := t.Tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// fetchEvents queries the event stream for pending events starting at the
// checkpoint offset.
func (t *streamTask) fetchEvents(ctx context.Context) ([]*envelopepb.Envelope, error) {
	rows, err := t.Tx.QueryContext(
		ctx,
		`SELECT
			e.stream_offset,
			e.envelope
		FROM eventstream.events AS e
		WHERE e.stream_id = $1
		AND e.stream_offset >= $2
		AND e.message_type_id = ANY($3)
		ORDER BY e.stream_offset
		LIMIT 10`,
		xsql.UUID(t.StreamID),
		t.CheckpointOffset,
		t.EventTypeIDs,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query pending events: %w", err)
	}
	defer rows.Close()

	var eventEnvelopes []*envelopepb.Envelope

	for rows.Next() {
		var (
			offset        uint64
			eventEnvelope = &envelopepb.Envelope{}
		)

		if err := rows.Scan(
			&offset,
			xsql.Envelope(eventEnvelope),
		); err != nil {
			return nil, fmt.Errorf("unable to scan pending event: %w", err)
		}

		envelopepb.SetExtension(
			eventEnvelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(t.StreamID).
				WithOffset(offset).
				Build(),
		)

		eventEnvelopes = append(eventEnvelopes, eventEnvelope)
	}

	return eventEnvelopes, nil
}

// handleEvents iterates over a batch of event envelopes, dispatching each to
// the handler. It returns errFailed if a handler invocation fails, or nil if all
// events were handled (or an OCC conflict stopped iteration early).
func (t *streamTask) handleEvents(ctx context.Context, eventEnvelopes []*envelopepb.Envelope) error {
	for _, eventEnvelope := range eventEnvelopes {
		if err := t.handleEvent(ctx, eventEnvelope); err != nil {
			return err
		}
	}

	return nil
}

// handleEvent unpacks a single event envelope and dispatches it to the handler.
// It returns errFailed if the handler returns an error or panics.
//
// ok is false if the handler returned an optimistic concurrency conflict, in
// which case the task should stop processing further events and commit the
// transaction to update the checkpoint offset.
func (t *streamTask) handleEvent(
	ctx context.Context,
	eventEnvelope *envelopepb.Envelope,
) (oerr error) {
	eventForRouting, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
	if err != nil {
		return err
	}

	streamPosition, ok, err := envelopepb.GetExtension[*envelopepb.EventStreamPosition](eventEnvelope.GetBody())
	if err != nil {
		return fmt.Errorf("unable to get event stream position extension: %w", err)
	}
	if !ok {
		return fmt.Errorf("event stream position extension not found on event envelope")
	}

	eventOffset := streamPosition.GetOffset()

	eventLogger := t.Logger.With(
		slog.Uint64("event_offset", eventOffset),
		xslog.Envelope("event", eventEnvelope),
	)

	instanceID, ok, err := t.routeEventToInstance(ctx, eventForRouting)
	if !ok || err != nil {
		return err
	}

	t.Logger = t.ParentLogger.With(
		xslog.Envelope("event", eventEnvelope),
		slog.Group(
			"process_instance",
			slog.String("id", instanceID),
		),
	)

	root := t.Handler.New()

	if err := xerrors.ConvertPanicToError(
		func() error {
			return t.Handler.HandleEvent(
				ctx,
				root,
				&messageScope{
					instanceID: instanceID,
					root:       root,
					packer:     &envelopepb.EffectPacker{},
					time:       eventEnvelope.GetBody().GetCreatedAt().AsTime(),
					logger:     eventLogger,
				},
				eventForRouting,
			)
		},
	); err != nil {
		eventLogger.ErrorContext(
			ctx,
			"unable to handle event",
			xslog.Error(err),
		)

		return errFailed
	}

	return nil
}

func (t *streamTask) routeEventToInstance(
	ctx context.Context,
	event dogma.Event,
) (instanceID string, ok bool, err error) {
	if err := xerrors.ConvertPanicToError(
		func() error {
			instanceID, ok, err = t.Handler.RouteEventToInstance(ctx, event)
			if err != nil {
				return err
			}

			if ok && instanceID == "" {
				return fmt.Errorf("handler returned empty instance ID")
			}

			return nil
		},
	); err != nil {
		t.Logger.ErrorContext(
			ctx,
			"unable to route event to instance",
			xslog.Error(err),
		)

		return "", false, errFailed
	}

	return instanceID, ok, nil
}

func (t *streamTask) failAndPostpone(ctx context.Context) error {
	if _, err := t.Tx.ExecContext(
		ctx,
		`SELECT eventstream.fail_and_postpone($1, $2, $3, $4)`,
		xsql.UUID(t.Identity.GetKey()),
		xsql.UUID(t.StreamID),
		t.BackoffBase,
		t.BackoffCap,
	); err != nil {
		return fmt.Errorf("unable to postpone stream consumption after failure: %w", err)
	}

	return nil
}
