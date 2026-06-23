package projection

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
	"github.com/dogmatiq/enginekit/protobuf/identitypb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
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
	BackoffBase, BackoffCap time.Duration
	Logger       *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (c *MessagePump) Run(ctx context.Context) {
	tasks := make(chan *streamTask)

	var g sync.WaitGroup

	g.Go(func() {
		defer close(tasks)

		for {
			task, ok, err := c.acquireTask(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return
				}

				c.Logger.ErrorContext(
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

// acquireTask attempts to exclusively lock the next pending stream for the
// handler and return it as a task.
func (c *MessagePump) acquireTask(
	ctx context.Context,
) (
	task *streamTask,
	ok bool,
	err error,
) {
	tx, err := c.DB.BeginTx(ctx, nil)
	if err != nil {
		return nil, false, fmt.Errorf("unable to begin transaction: %w", err)
	}
	defer func() {
		if !ok {
			tx.Rollback()
		}
	}()

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			stream_id,
			checkpoint_offset
		FROM eventstream.acquire_for_read($1, $2)`,
		xsql.UUID(c.Identity.GetKey()),
		c.EventTypeIDs,
	)

	task = &streamTask{
		Tx:           tx,
		Handler:      c.Handler,
		Identity:     c.Identity,
		Concurrency:  c.Concurrency,
		StreamID:     &uuidpb.UUID{},
		EventTypeIDs: c.EventTypeIDs,
		BackoffBase:  c.BackoffBase,
		BackoffCap:   c.BackoffCap,
		ParentLogger: c.Logger,
		Logger:       c.Logger,
	}

	if err := row.Scan(
		xsql.UUID(task.StreamID),
		&task.CheckpointOffset,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to acquire stream for read: %w", err)
	}

	task.Logger = c.Logger.With(
		xslog.UUID("stream_id", task.StreamID),
	)

	return task, true, nil
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
