package integration

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

// MessagePump is an engine component that periodically attempts to acquire
// pending commands for dispatch to an integration message handler of a specific
// type.
type MessagePump struct {
	DB             *sql.DB
	Handler        dogma.IntegrationMessageHandler
	Identity       *identitypb.Identity
	Concurrency    dogma.ConcurrencyPreference
	Packer         *envelopepb.Packer
	CommandTypeIDs []string
	BackoffBase    time.Duration
	BackoffCap     time.Duration
	Logger         *slog.Logger
}

// Run runs the message pump until ctx is canceled.
func (c *MessagePump) Run(ctx context.Context) {
	tasks := make(chan *commandTask)

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

// acquireTask attempts to exclusively lock the next pending command for the
// handler and return its message ID and envelope data.
func (c *MessagePump) acquireTask(
	ctx context.Context,
) (
	task *commandTask,
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
			c.message_id,
			c.envelope,
			c.failures
		FROM commandqueue.commands AS c
		WHERE message_type_id = ANY($1)
		AND execute_at <= clock_timestamp()
		ORDER BY execute_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		c.CommandTypeIDs,
	)

	task = &commandTask{
		Tx:            tx,
		MessageID:     &uuidpb.UUID{},
		Handler:       c.Handler,
		Identity:      c.Identity,
		Concurrency:   c.Concurrency,
		Packer:        c.Packer,
		BackoffBase:   c.BackoffBase,
		BackoffCap:    c.BackoffCap,
		EnvelopeBytes: []byte{},
		ParentLogger:  c.Logger,
		Logger:        c.Logger,
	}

	var failures uint64

	if err := row.Scan(
		xsql.UUID(task.MessageID),
		&task.EnvelopeBytes,
		&failures,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to scan pending command: %w", err)
	}

	task.Logger = c.Logger.With(
		slog.Group(
			"command",
			xslog.UUID("message_id", task.MessageID),
		),
		slog.Uint64("attempt", failures+1),
	)

	return task, true, nil
}

// messageScope implements [dogma.IntegrationCommandScope].
type messageScope struct {
	packer *envelopepb.EffectPacker
	logger *slog.Logger
}

func (s *messageScope) Now() time.Time {
	return time.Now()
}

func (s *messageScope) Log(format string, args ...any) {
	s.logger.Info(fmt.Sprintf(format, args...))
}

func (s *messageScope) RecordEvent(event dogma.Event) {
	eventEnvelope := s.packer.PackEvent(event)

	s.logger.Info(
		event.MessageDescription(),
		xslog.Envelope("event", eventEnvelope),
	)
}
