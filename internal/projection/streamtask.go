package projection

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
	"github.com/dogmatiq/reference-engine/internal/concurrency"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// EventBatchSize is the maximum number of events that will be handled in a single
// transaction.
const EventBatchSize = 10

type streamTask struct {
	Tx                      *sql.Tx
	Handler                 dogma.ProjectionMessageHandler
	Identity                *identitypb.Identity
	Concurrency             dogma.ConcurrencyPreference
	StreamID                *uuidpb.UUID
	CheckpointOffset        *uint64
	EventTypeIDs            []string
	BackoffBase, BackoffCap time.Duration
	ParentLogger, Logger    *slog.Logger
}

var (
	errFailed   = errors.New("unable to handle event")
	errConflict = errors.New("optimistic concurrency conflict")
)

// Execute processes the task by handling pending events on the stream and
// committing the transaction.
func (t *streamTask) Execute(ctx context.Context) error {
	defer t.Tx.Rollback()

	if err := t.loadCheckpointOffset(ctx); err != nil {
		return err
	}

	t.Logger = t.ParentLogger.With(
		xslog.UUID("stream_id", t.StreamID),
		slog.Uint64("checkpoint_offset", *t.CheckpointOffset),
	)

	eventEnvelopes, batchCheckpointOffset, err := t.fetchEvents(ctx)
	if err != nil {
		return err
	}

	if len(eventEnvelopes) != 0 {
		handleErr := t.handleEvents(ctx, eventEnvelopes)

		if handleErr != nil {
			// If an error occurred, we can only update the checkpoint offset to
			// the offset after the last successfully handled event.
			batchCheckpointOffset = *t.CheckpointOffset

			if errors.Is(handleErr, errFailed) {
				if err := t.failAndPostpone(ctx); err != nil {
					return err
				}
			} else if !errors.Is(handleErr, errConflict) {
				return handleErr
			}
		}
	}

	if err := xsql.ExecOne(
		ctx,
		t.Tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
		AND stream_id = $3`,
		batchCheckpointOffset,
		xsql.UUID(t.Identity.GetKey()),
		xsql.UUID(t.StreamID),
	); err != nil {
		return fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	if err := t.Tx.Commit(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

// loadCheckpointOffset queries the handler for its checkpoint offset if the
// engine does not already have one stored.
func (t *streamTask) loadCheckpointOffset(ctx context.Context) error {
	if t.CheckpointOffset != nil {
		return nil
	}

	handlerCheckpointOffset, err := t.Handler.CheckpointOffset(ctx, t.StreamID.AsString())
	if err != nil {
		return fmt.Errorf("unable to get checkpoint offset from handler: %w", err)
	}

	t.CheckpointOffset = &handlerCheckpointOffset

	return nil
}

// fetchEvents queries the event stream for pending events starting at the
// checkpoint offset.
//
// batchCheckpointOffset is the checkpoint offset the handler should resume from
// on the next poll, assuming all events in the batch are handled successfully.
func (t *streamTask) fetchEvents(ctx context.Context) (
	eventEnvelopes []*envelopepb.Envelope,
	batchCheckpointOffset uint64,
	err error,
) {
	batchCheckpointOffset = *t.CheckpointOffset

	rows, err := t.Tx.QueryContext(
		ctx,
		`SELECT
			e.stream_offset,
			e.envelope,
			s.next_offset
		FROM eventstream.streams AS s
		LEFT JOIN eventstream.events AS e
			ON e.stream_id = s.id
			AND e.stream_offset >= $2
			AND e.message_type_id = ANY($3)
		WHERE s.id = $1
		ORDER BY e.stream_offset NULLS LAST
		LIMIT $4`,
		xsql.UUID(t.StreamID),
		*t.CheckpointOffset,
		t.EventTypeIDs,
		EventBatchSize,
	)
	if err != nil {
		return nil, 0, fmt.Errorf("unable to query pending events: %w", err)
	}
	defer rows.Close()

	var lastEventOffset uint64

	for rows.Next() {
		var (
			eventOffset   *uint64
			envelopeBytes []byte
		)

		if err := rows.Scan(
			&eventOffset,
			&envelopeBytes,
			&batchCheckpointOffset,
		); err != nil {
			return nil, 0, fmt.Errorf("unable to scan pending event: %w", err)
		}

		if eventOffset == nil {
			// LEFT JOIN produced a row with no matching event; this means
			// there are no relevant events on the stream after the checkpoint.
			break
		}

		eventEnvelope := &envelopepb.Envelope{}
		if err := eventEnvelope.UnmarshalBinary(envelopeBytes); err != nil {
			return nil, 0, fmt.Errorf("unable to unmarshal event envelope: %w", err)
		}

		envelopepb.SetExtension(
			eventEnvelope.GetBody(),
			envelopepb.NewEventStreamPositionBuilder().
				WithStreamId(t.StreamID).
				WithOffset(*eventOffset).
				Build(),
		)

		eventEnvelopes = append(eventEnvelopes, eventEnvelope)
		lastEventOffset = *eventOffset
	}

	// If the batch is full there may be more matching events to handle before
	// we reach the end of the stream, so we can only safely skip to the offset
	// after the last event in the batch.
	if len(eventEnvelopes) == EventBatchSize {
		batchCheckpointOffset = lastEventOffset + 1
	}

	return eventEnvelopes, batchCheckpointOffset, nil
}

// handleEvents iterates over a batch of event envelopes, dispatching each to
// the handler. It returns [errFailed] if a handler invocation fails,
// [errConflict] if an OCC conflict stopped iteration early, or nil if all
// events were handled successfully.
func (t *streamTask) handleEvents(ctx context.Context, eventEnvelopes []*envelopepb.Envelope) error {
	for _, eventEnvelope := range eventEnvelopes {
		if err := t.handleEvent(ctx, eventEnvelope); err != nil {
			return err
		}
	}

	return nil
}

// handleEvent unpacks a single event envelope and dispatches it to the handler.
// It returns [errFailed] if the handler returns an error or panics, or
// [errConflict] if the handler returned an optimistic concurrency conflict.
func (t *streamTask) handleEvent(
	ctx context.Context,
	eventEnvelope *envelopepb.Envelope,
) error {
	event, err := envelopepb.Unpack[dogma.Event](eventEnvelope)
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

	var nextCheckpointOffset uint64

	if err := concurrency.EnforceConcurrencyPreference(
		ctx,
		t.Tx,
		t.Identity.GetKey(),
		t.Concurrency,
		func() error {
			return xerrors.ConvertPanicToError(
				func() error {
					var err error
					nextCheckpointOffset, err = t.Handler.HandleEvent(
						ctx,
						&eventScope{
							streamID:         t.StreamID.AsString(),
							offset:           eventOffset,
							recordedAt:       eventEnvelope.GetBody().GetCreatedAt().AsTime(),
							checkpointOffset: *t.CheckpointOffset,
							logger:           eventLogger,
						},
						event,
					)
					return err
				},
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

	prevCheckpointOffset := *t.CheckpointOffset
	*t.CheckpointOffset = nextCheckpointOffset

	if nextCheckpointOffset != eventOffset+1 {
		eventLogger.WarnContext(
			ctx,
			"optimistic concurrency conflict",
			slog.Uint64("engine_checkpoint_offset", prevCheckpointOffset),
			slog.Uint64("handler_checkpoint_offset", nextCheckpointOffset),
		)

		return errConflict
	}

	return nil
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
