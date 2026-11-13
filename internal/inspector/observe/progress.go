package observe

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// StreamProgress is how far a process or projection handler has got through
// one event stream.
type StreamProgress struct {
	// HandlerKey is the key of the handler.
	HandlerKey *uuidpb.UUID

	// StreamID is the ID of the event stream.
	StreamID *uuidpb.UUID

	// CheckpointOffset is the offset of the next event that the handler
	// expects to handle.
	CheckpointOffset uint64

	// IsRemote is true if the stream belongs to another engine. The engine
	// has a checkpoint for such a stream, but not its length.
	IsRemote bool

	// Length is the number of events on the stream. It is only meaningful if
	// IsRemote is false.
	Length uint64

	// Unhandled is the number of events at or after the checkpoint offset of
	// the types that the handler handles, up to [countLimit]. It is only
	// meaningful if IsRemote is false.
	Unhandled int
}

// IsBehind returns true if the stream has events at or after the checkpoint
// offset that the handler handles.
func (p StreamProgress) IsBehind() bool {
	return p.Unhandled > 0
}

// processProgress returns how far the given process handler has got through
// each event stream, up to the given limit of local streams.
//
// The engine stores a process handler's checkpoint offsets in its own
// database. A stream with no checkpoint has not yet been consumed, so its
// checkpoint offset is zero.
func (d *Database) processProgress(
	ctx context.Context,
	h *Handler,
	limit int,
) ([]StreamProgress, error) {
	var progress []StreamProgress

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT
					s.id,
					s.next_offset,
					COALESCE(c.checkpoint_offset, 0),
					CASE WHEN s.next_offset > COALESCE(c.checkpoint_offset, 0) THEN (
						SELECT count(*) FROM (
							SELECT 1
							FROM eventstream.events AS e
							WHERE e.stream_id = s.id
								AND e.message_type_id = ANY($2)
								AND e.stream_offset >= COALESCE(c.checkpoint_offset, 0)
							LIMIT $3
						) AS x
					) ELSE 0 END
				FROM eventstream.streams AS s
				LEFT JOIN eventstream.handler_checkpoints AS c
					ON c.stream_id = s.id
					AND c.handler_key = $1
				ORDER BY s.id
				LIMIT $4`,
				xsql.UUID(h.Key),
				xsql.UUIDs(h.MessageTypeIDs(config.HandlesEventRouteType)...),
				countLimit,
				limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				p := StreamProgress{
					HandlerKey: h.Key,
					StreamID:   &uuidpb.UUID{},
				}

				if err := rows.Scan(
					xsql.UUID(p.StreamID),
					&p.Length,
					&p.CheckpointOffset,
					&p.Unhandled,
				); err != nil {
					return err
				}

				progress = append(progress, p)
			}

			if err := rows.Err(); err != nil {
				return err
			}

			// A checkpoint for a stream that has no row in the streams table
			// is for another engine's stream.
			rows, err = tx.QueryContext(
				ctx,
				`SELECT c.stream_id, c.checkpoint_offset
				FROM eventstream.handler_checkpoints AS c
				WHERE c.handler_key = $1
					AND NOT EXISTS (
						SELECT 1
						FROM eventstream.streams AS s
						WHERE s.id = c.stream_id
					)
				ORDER BY c.stream_id
				LIMIT $2`,
				xsql.UUID(h.Key),
				limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				p := StreamProgress{
					HandlerKey: h.Key,
					StreamID:   &uuidpb.UUID{},
					IsRemote:   true,
				}

				if err := rows.Scan(
					xsql.UUID(p.StreamID),
					&p.CheckpointOffset,
				); err != nil {
					return err
				}

				progress = append(progress, p)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read checkpoints of %s: %w", h.Name, err)
	}

	return progress, nil
}

// projectionProgress returns how far the given projection handler has got
// through each of the given local event streams.
//
// A projection handler stores its checkpoint offsets in its own storage, so
// it calls the handler's CheckpointOffset method for each stream, with the
// given timeout. That method only reads.
func (d *Database) projectionProgress(
	ctx context.Context,
	h *Handler,
	streams []EventStream,
	timeout time.Duration,
) ([]StreamProgress, error) {
	if h.Projection == nil {
		return nil, fmt.Errorf("%s is not a projection handler", h.Name)
	}

	var progress []StreamProgress

	for _, s := range streams {
		cp, err := xerrors.RecoverT(
			func() (uint64, error) {
				ctx, cancel := context.WithTimeout(ctx, timeout)
				defer cancel()

				return h.Projection.CheckpointOffset(ctx, s.ID.AsString())
			},
		)
		if err != nil {
			return nil, fmt.Errorf(
				"unable to load the checkpoint offset of %s for event stream %s: %w",
				h.Name,
				s.ID.AsString(),
				err,
			)
		}

		p := StreamProgress{
			HandlerKey:       h.Key,
			StreamID:         s.ID,
			CheckpointOffset: cp,
			Length:           s.Length,
		}

		if s.Length > cp {
			unhandled, err := d.countUnhandled(ctx, h, s.ID, cp)
			if err != nil {
				return nil, err
			}
			p.Unhandled = unhandled
		}

		progress = append(progress, p)
	}

	return progress, nil
}

// countUnhandled returns the number of events on the given stream at or after
// the given offset of the types that the given handler handles, up to
// [countLimit].
func (d *Database) countUnhandled(
	ctx context.Context,
	h *Handler,
	streamID *uuidpb.UUID,
	from uint64,
) (int, error) {
	var n int

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			return tx.QueryRowContext(
				ctx,
				`SELECT count(*) FROM (
					SELECT 1
					FROM eventstream.events AS e
					WHERE e.stream_id = $1
						AND e.message_type_id = ANY($2)
						AND e.stream_offset >= $3
					LIMIT $4
				) AS x`,
				xsql.UUID(streamID),
				xsql.UUIDs(h.MessageTypeIDs(config.HandlesEventRouteType)...),
				from,
				countLimit,
			).Scan(&n)
		},
	)
	if err != nil {
		return 0, fmt.Errorf("unable to count unhandled events: %w", err)
	}

	return n, nil
}
