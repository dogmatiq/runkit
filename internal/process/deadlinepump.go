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

// DeadlinePump is an engine component that periodically attempts to acquire
// pending deadlines for dispatch to a process message handler.
type DeadlinePump struct {
	DB                      *sql.DB
	Handler                 dogma.ProcessMessageHandler[dogma.ProcessRoot]
	Identity                *identitypb.Identity
	Packer                  *envelopepb.Packer
	BackoffBase, BackoffCap time.Duration
	Logger                  *slog.Logger
}

// Run runs the deadline pump until ctx is canceled.
func (p *DeadlinePump) Run(ctx context.Context) {
	tasks := make(chan *deadlineTask)

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
					"unable to acquire deadline task",
					xslog.Error(err),
				)
			}

			if ok {
				task.Logger.DebugContext(
					ctx,
					"acquired deadline task",
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
						"unable to execute deadline task",
						xslog.Error(err),
					)
				} else {
					task.Logger.DebugContext(
						ctx,
						"executed deadline task",
					)
				}
			}
		})
	}

	g.Wait()
}

// acquireTask attempts to exclusively lock the next pending deadline for the
// handler and return it as a task.
func (p *DeadlinePump) acquireTask(
	ctx context.Context,
) (
	task *deadlineTask,
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
			d.message_id,
			d.instance_id,
			d.envelope,
			i.state
		FROM process.deadlines AS d
		INNER JOIN process.instances AS i
			ON i.handler_key = d.handler_key
			AND i.instance_id = d.instance_id
		WHERE d.handler_key = $1
		AND d.deliver_at <= clock_timestamp()
		ORDER BY d.deliver_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		xsql.UUID(p.Identity.GetKey()),
	)

	task = &deadlineTask{
		Tx:          tx,
		Handler:     p.Handler,
		Identity:    p.Identity,
		Packer:      p.Packer,
		MessageID:   &uuidpb.UUID{},
		BackoffBase: p.BackoffBase,
		BackoffCap:  p.BackoffCap,
		Logger:      p.Logger,
	}

	if err := row.Scan(
		xsql.UUID(task.MessageID),
		&task.InstanceID,
		&task.EnvelopeBytes,
		&task.InstanceStateBytes,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, false, nil
		}

		return nil, false, fmt.Errorf("unable to acquire pending deadline: %w", err)
	}

	task.Logger = p.Logger.With(
		xslog.UUID("message_id", task.MessageID),
		slog.String("instance_id", task.InstanceID),
	)

	return task, true, nil
}
