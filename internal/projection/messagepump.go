package projection

import (
	"context"
	"database/sql"
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

// MessagePump is an engine component that periodically attempts to acquire
// pending events for dispatch to a projection message handler of a specific
// type.
type MessagePump struct {
	DB           *sql.DB
	Handler      dogma.ProjectionMessageHandler
	Identity     *identitypb.Identity
	Concurrency  dogma.ConcurrencyPreference
	EventTypeIDs []string
	BackoffBase  time.Duration
	BackoffCap   time.Duration
	Logger       *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (c *MessagePump) Run(ctx context.Context) {
	for {
		if err := c.tick(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}

			c.Logger.ErrorContext(
				ctx,
				"unable to process events",
				xslog.Error(err),
			)
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(25 * time.Millisecond):
		}
	}
}

func (c *MessagePump) tick(ctx context.Context) error {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			stream_id,
			checkpoint_offset
		FROM eventstream.acquire_for_read($1, $2)`,
		xsql.UUID(c.Identity.GetKey()),
		c.EventTypeIDs,
	)

	var (
		streamID         = &uuidpb.UUID{}
		checkpointOffset *uint64
	)

	if err := row.Scan(
		xsql.UUID(streamID),
		&checkpointOffset,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil
		}

		return fmt.Errorf("unable to acquire stream for read: %w", err)
	}

	if checkpointOffset == nil {
		handlerCheckpointOffset, err := c.Handler.CheckpointOffset(ctx, streamID.AsString())
		if err != nil {
			return fmt.Errorf("unable to get checkpoint offset from handler: %w", err)
		}
		checkpointOffset = &handlerCheckpointOffset
	}

	c.Logger.DebugContext(
		ctx,
		"acquired stream for reading",
		xslog.UUID("stream_id", streamID),
		slog.Uint64("checkpoint_offset", *checkpointOffset),
	)

	eventEnvelopes, err := c.fetchEvents(
		ctx,
		tx,
		streamID,
		*checkpointOffset,
	)
	if err != nil {
		return err
	}

	if c.Concurrency == dogma.MinimizeConcurrency {
		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO projection.handlers (
				handler_key
			)
			VALUES ($1)
			ON CONFLICT (handler_key) DO UPDATE SET
				handler_key = EXCLUDED.handler_key`,
			xsql.UUID(c.Identity.GetKey()),
		); err != nil {
			return fmt.Errorf("unable to acquire handler lock: %w", err)
		}
	}

	if len(eventEnvelopes) != 0 {
		for _, eventEnvelope := range eventEnvelopes {
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

			var nextCheckpointOffset uint64

			if err := xerrors.Recover(
				func() error {
					var err error
					nextCheckpointOffset, err = c.Handler.HandleEvent(
						ctx,
						&messageScope{
							streamID:         streamID.AsString(),
							offset:           eventOffset,
							recordedAt:       eventEnvelope.GetBody().GetCreatedAt().AsTime(),
							checkpointOffset: *checkpointOffset,
							logger:           c.Logger,
						},
						event,
					)
					return err
				},
			); err != nil {
				c.Logger.ErrorContext(
					ctx,
					"unable to handle event",
					xslog.UUID("stream_id", streamID),
					slog.Uint64("event_offset", eventOffset),
					xslog.Envelope("event", eventEnvelope),
					xslog.Error(err),
				)

				if _, err := tx.ExecContext(
					ctx,
					`SELECT eventstream.fail_and_postpone($1, $2, $3, $4)`,
					xsql.UUID(c.Identity.GetKey()),
					xsql.UUID(streamID),
					c.BackoffBase,
					c.BackoffCap,
				); err != nil {
					return fmt.Errorf("unable to postpone stream consumption after failure: %w", err)
				}

				if err := tx.Commit(); err != nil {
					return fmt.Errorf("unable to commit transaction: %w", err)
				}

				return nil
			}

			prevCheckpointOffset := *checkpointOffset
			*checkpointOffset = nextCheckpointOffset

			if nextCheckpointOffset != eventOffset+1 {
				c.Logger.WarnContext(
					ctx,
					"optimistic concurrency conflict",
					xslog.UUID("stream_id", streamID),
					slog.Uint64("event_offset", eventOffset),
					slog.Uint64("engine_checkpoint_offset", prevCheckpointOffset),
					slog.Uint64("handler_checkpoint_offset", nextCheckpointOffset),
					xslog.Envelope("event", eventEnvelope),
				)

				break
			}

			c.Logger.DebugContext(
				ctx,
				"handled event",
				xslog.UUID("stream_id", streamID),
				slog.Uint64("event_offset", eventOffset),
				slog.Uint64("checkpoint_offset", *checkpointOffset),
				xslog.Envelope("event", eventEnvelope),
			)
		}

		if err := xsql.ExecOne(
			ctx,
			tx,
			`UPDATE eventstream.handler_checkpoints SET
				checkpoint_offset = $1,
				failures = 0
			WHERE handler_key = $2
			AND stream_id = $3`,
			checkpointOffset,
			xsql.UUID(c.Identity.GetKey()),
			xsql.UUID(streamID),
		); err != nil {
			return fmt.Errorf("unable to update handler checkpoint: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (c *MessagePump) fetchEvents(
	ctx context.Context,
	tx *sql.Tx,
	streamID *uuidpb.UUID,
	checkpointOffset uint64,
) ([]*envelopepb.Envelope, error) {
	rows, err := tx.QueryContext(
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
		xsql.UUID(streamID),
		checkpointOffset,
		c.EventTypeIDs,
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
				WithStreamId(streamID).
				WithOffset(offset).
				Build(),
		)

		eventEnvelopes = append(eventEnvelopes, eventEnvelope)
	}

	return eventEnvelopes, nil
}

// messageScope implements [dogma.ProjectionEventScope].
type messageScope struct {
	streamID                 string
	offset, checkpointOffset uint64
	recordedAt               time.Time
	logger                   *slog.Logger
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *messageScope) StreamID() string {
	return s.streamID
}

func (s *messageScope) Offset() uint64 {
	return s.offset
}

func (s *messageScope) CheckpointOffset() uint64 {
	return s.checkpointOffset
}

func (s *messageScope) RecordedAt() time.Time {
	return s.recordedAt
}
