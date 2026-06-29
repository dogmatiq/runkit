package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// EventPump is an engine component that periodically attempts to acquire
// pending events for dispatch to a process message handler of a specific type.
type EventPump struct {
	DB                      *sql.DB
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	EventTypeIDs            []string
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (p *EventPump) Run(ctx context.Context) {
	tasks := make(chan *eventTask)

	var g sync.WaitGroup

	g.Go(func() {
		defer close(tasks)

		for {
			task, ok, err := p.acquireTask(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				p.Logger.ErrorContext(
					ctx,
					"unable to acquire task",
					xslog.Error(err),
				)
			}

			if ok {
				task.Logger.DebugContext(
					ctx,
					"acquired task",
				)

				select {
				case <-ctx.Done():
					task.Tx.Rollback()
					return
				case tasks <- task:
					continue
				}
			} else {
				select {
				case <-ctx.Done():
					return
				case <-time.After(25 * time.Millisecond):
					continue
				}
			}
		}
	})

	for range runtime.GOMAXPROCS(0) {
		g.Go(func() {
			for task := range tasks {
				if err := task.Execute(ctx); err != nil {
					if ctx.Err() != nil {
						return
					}

					xsql.PanicOnDeadlock(err)

					task.Logger.ErrorContext(
						ctx,
						"unable to execute task",
						xslog.Error(err),
					)
				} else {
					task.Logger.DebugContext(
						ctx,
						"executed task",
					)
				}
			}
		})
	}

	g.Wait()
}

// acquireTask attempts to exclusively lock the next pending event for the
// handler and return it as a task.
func (p *EventPump) acquireTask(
	ctx context.Context,
) (
	task *eventTask,
	ok bool,
	err error,
) {
	tx, err := p.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		if !ok {
			tx.Rollback()
		}
	}()

	// Acquire a stream for reading and select the next relevant event
	// after the checkpoint offset for that stream.
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			s.id,
			s.next_offset,
			e.stream_offset,
			e.envelope
		FROM eventstream.acquire_for_read($1) AS a
		INNER JOIN eventstream.streams AS s
			ON s.id = a.stream_id
		LEFT JOIN eventstream.events AS e
			ON e.stream_id = a.stream_id
			AND e.stream_offset >= COALESCE(a.checkpoint_offset, 0)
			AND e.message_type_id = ANY($2)
		ORDER BY e.stream_offset
		LIMIT 1`,
		xsql.UUID(p.Identity.GetKey()),
		p.EventTypeIDs,
	)

	var (
		streamID      = &uuidpb.UUID{}
		nextOffset    uint64
		eventOffset   *uint64
		envelopeBytes []byte
	)

	if err := row.Scan(
		xsql.UUID(streamID),
		&nextOffset,
		&eventOffset,
		&envelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to acquire pending event: %w", err)
	}

	// If we found a pending event, return a task for processing it.
	if eventOffset != nil {
		return &eventTask{
			Tx:            tx,
			Handler:       p.Handler,
			Identity:      p.Identity,
			Packer:        p.Packer,
			StreamID:      streamID,
			EventOffset:   *eventOffset,
			EnvelopeBytes: envelopeBytes,
			BackoffBase:   p.BackoffBase,
			BackoffCap:    p.BackoffCap,
			ParentLogger:  p.Logger,
			Logger: p.Logger.With(
				xslog.UUID("stream_id", streamID),
				slog.Uint64("event_offset", *eventOffset),
			),
		}, true, nil
	}

	// Otherwise, advance the checkpoint offset for the stream to the end of the
	// stream.
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE eventstream.handler_checkpoints SET
			checkpoint_offset = $1,
			failures = 0
		WHERE handler_key = $2
			AND stream_id = $3`,
		nextOffset,
		xsql.UUID(p.Identity.GetKey()),
		xsql.UUID(streamID),
	); err != nil {
		return nil, false, fmt.Errorf("unable to update handler checkpoint: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, false, fmt.Errorf("unable to commit transaction: %w", err)
	}

	return nil, false, nil
}
