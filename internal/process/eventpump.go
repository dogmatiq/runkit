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

	row := tx.QueryRowContext(
		ctx,
		`SELECT
			a.stream_id,
			e.stream_offset,
			e.envelope
		FROM eventstream.acquire_for_read($1, $2) AS a
		INNER JOIN eventstream.events AS e
			ON e.stream_id = a.stream_id
			AND e.stream_offset >= COALESCE(a.checkpoint_offset, 0)
			AND e.message_type_id = ANY($2)
		ORDER BY e.stream_offset
		LIMIT 1`,
		xsql.UUID(p.Identity.GetKey()),
		p.EventTypeIDs,
	)

	task = &eventTask{
		Tx:           tx,
		Handler:      p.Handler,
		Identity:     p.Identity,
		Packer:       p.Packer,
		StreamID:     &uuidpb.UUID{},
		BackoffBase:  p.BackoffBase,
		BackoffCap:   p.BackoffCap,
		ParentLogger: p.Logger,
		Logger:       p.Logger,
	}

	if err := row.Scan(
		xsql.UUID(task.StreamID),
		&task.EventOffset,
		&task.EnvelopeBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to acquire pending event: %w", err)
	}

	task.Logger = p.Logger.With(
		xslog.UUID("stream_id", task.StreamID),
		slog.Uint64("event_offset", task.EventOffset),
	)

	return task, true, nil
}

// messageScope implements [dogma.ProcessEventScope] and
// [dogma.ProcessDeadlineScope].
type messageScope struct {
	instanceID string
	root       dogma.ProcessRoot
	mutated    bool
	ended      bool
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

func (s *messageScope) Mutate(fn func(dogma.ProcessRoot)) {
	fn(s.root)
	s.mutated = true
}

func (s *messageScope) End() {
	s.ended = true
}

func (s *messageScope) ExecuteCommand(dogma.Command) {
	panic("not implemented")
}

func (s *messageScope) ScheduleDeadline(d dogma.Deadline, t time.Time) {
	s.packer.PackDeadline(d, envelopepb.WithScheduledFor(t))
}

func (s *messageScope) RecordedAt() time.Time {
	return s.time
}

func (s *messageScope) ScheduledFor() time.Time {
	return s.time
}
