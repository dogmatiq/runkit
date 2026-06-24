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
// pending events and deadlines for dispatch to a process message handler of a
// specific type.
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
	tasks := make(chan *streamTask)

	// TODO: capture current stream offsets if this is a brand new handler

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
func (p *EventPump) acquireTask(
	ctx context.Context,
) (
	task *streamTask,
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

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			stream_id,
			checkpoint_offset
		FROM eventstream.acquire_for_read($1, $2)`,
		xsql.UUID(p.Identity.GetKey()),
		p.EventTypeIDs,
	)

	task = &streamTask{
		Tx:           tx,
		Handler:      p.Handler,
		Identity:     p.Identity,
		StreamID:     &uuidpb.UUID{},
		EventTypeIDs: p.EventTypeIDs,
		BackoffBase:  p.BackoffBase,
		BackoffCap:   p.BackoffCap,
		ParentLogger: p.Logger,
		Logger:       p.Logger,
	}

	var checkpointOffset *uint64

	if err := row.Scan(
		xsql.UUID(task.StreamID),
		&checkpointOffset,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to acquire stream for read: %w", err)
	}

	if checkpointOffset != nil {
		task.CheckpointOffset = *checkpointOffset
	}

	task.Logger = p.Logger.With(
		xslog.UUID("stream_id", task.StreamID),
	)

	return task, true, nil
}

// messageScope implements [dogma.ProcessEventScope] and
// [dogma.ProcessDeadlineScope].
type messageScope struct {
	instanceID string
	root       dogma.ProcessRoot
	packer     *envelopepb.EffectPacker
	time       time.Time
	logger     *slog.Logger
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *messageScope) InstanceID() string {
	return s.instanceID
}

func (s *messageScope) Mutate(func(dogma.ProcessRoot)) {
	panic("not implemented")
}

func (s *messageScope) End() {
	panic("not implemented")
}

func (s *messageScope) ExecuteCommand(dogma.Command) {
	panic("not implemented")
}

func (s *messageScope) ScheduleDeadline(dogma.Deadline, time.Time) {
	panic("not implemented")
}

func (s *messageScope) RecordedAt() time.Time {
	return s.time
}

func (s *messageScope) ScheduledFor() time.Time {
	return s.time
}
