package process

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"time"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"
	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xmessage"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

func (c *DeadlineConsumer) deliver(ctx context.Context, candidate deadlineCandidate) error {
	return xsql.Transact(ctx, c.Sink.DB, func(ctx context.Context, tx *sql.Tx) error {
		state, ended, busy, err := c.lockInstance(ctx, tx, candidate.instanceID)
		if err != nil {
			return err
		}
		if busy {
			return c.deferDeadline(ctx, tx, candidate)
		}
		if ended {
			return nil
		}
		var data []byte
		var failures uint64
		err = tx.QueryRowContext(ctx, `SELECT envelope, failures FROM process.deadlines
		WHERE message_id = $1 AND handler_key = $2 AND instance_id = $3
			AND message_type_id = ANY($4) AND deliver_at <= $5
		FOR UPDATE`, xsql.UUID(candidate.messageID), xsql.UUID(c.Sink.Identity.GetKey()),
			candidate.instanceID, xsql.UUIDSeq(c.DeadlineTypeIDs.All()), time.Now()).Scan(&data, &failures)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("unable to acquire process deadline: %w", err)
		}
		logger := c.Logger.With(xslog.UUID("deadline_id", candidate.messageID),
			slog.Uint64("attempt", failures+1), slog.String("instance_id", candidate.instanceID))
		err = c.handle(ctx, tx, candidate, state, data, logger)
		if errors.Is(err, messagepump.ErrFailed) {
			logger.ErrorContext(ctx, "unable to prepare deadline delivery", xslog.Error(err))
			return c.postpone(ctx, tx, candidate, failures+1)
		}
		return err
	})
}

// lockInstance isolates a lock timeout in a savepoint, retaining the outer
// transaction for a contention deferral. The prior timeout is restored on both
// paths. Successful row locks survive RELEASE SAVEPOINT.
func (c *DeadlineConsumer) lockInstance(ctx context.Context, tx *sql.Tx, instanceID string) ([]byte, bool, bool, error) {
	if _, err := tx.ExecContext(ctx, `SAVEPOINT deadline_instance_lock`); err != nil {
		return nil, false, false, err
	}
	var previous string
	if err := tx.QueryRowContext(ctx, `SELECT current_setting('lock_timeout')`).Scan(&previous); err != nil {
		return nil, false, false, err
	}
	// PostgreSQL stores this timeout in milliseconds. Round positive durations
	// up rather than accidentally turn a sub-millisecond value into "disabled".
	ms := c.LockTimeout / time.Millisecond
	if c.LockTimeout%time.Millisecond != 0 {
		ms++
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, strconv.FormatInt(int64(ms), 10)+"ms"); err != nil {
		return nil, false, false, err
	}
	var state []byte
	var ended bool
	err := tx.QueryRowContext(ctx, `SELECT ended, state FROM process.instances
		WHERE handler_key = $1 AND instance_id = $2 FOR UPDATE`,
		xsql.UUID(c.Sink.Identity.GetKey()), instanceID).Scan(&ended, &state)
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == pgerrcode.LockNotAvailable {
		if _, err := tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT deadline_instance_lock`); err != nil {
			return nil, false, false, err
		}
		if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT deadline_instance_lock`); err != nil {
			return nil, false, false, err
		}
		return nil, false, true, nil
	}
	if err != nil {
		return nil, false, false, fmt.Errorf("unable to lock process instance: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `SELECT set_config('lock_timeout', $1, true)`, previous); err != nil {
		return nil, false, false, err
	}
	if _, err := tx.ExecContext(ctx, `RELEASE SAVEPOINT deadline_instance_lock`); err != nil {
		return nil, false, false, err
	}
	return state, ended, false, nil
}

func (c *DeadlineConsumer) deferDeadline(ctx context.Context, tx *sql.Tx, candidate deadlineCandidate) error {
	_, err := tx.ExecContext(ctx, `WITH ready AS (
		SELECT message_id FROM process.deadlines
		WHERE message_id = $1 AND handler_key = $2 AND instance_id = $3
			AND message_type_id = ANY($4) AND deliver_at <= $5
		FOR UPDATE SKIP LOCKED
	)
	UPDATE process.deadlines AS d SET deliver_at = clock_timestamp() + $6
	FROM ready WHERE d.message_id = ready.message_id`,
		xsql.UUID(candidate.messageID), xsql.UUID(c.Sink.Identity.GetKey()), candidate.instanceID,
		xsql.UUIDSeq(c.DeadlineTypeIDs.All()), time.Now(), c.ContentionDelay)
	if err != nil {
		return fmt.Errorf("unable to defer contended deadline: %w", err)
	}
	return nil
}

func (c *DeadlineConsumer) postpone(ctx context.Context, tx *sql.Tx, candidate deadlineCandidate, failures uint64) error {
	return xsql.ExecOne(ctx, tx, `UPDATE process.deadlines SET
		failures = $2, deliver_at = clock_timestamp() + $3 WHERE message_id = $1`,
		xsql.UUID(candidate.messageID), failures, messagepump.BackoffDelay(c.BackoffBase, c.BackoffCap, failures))
}

func (c *DeadlineConsumer) handle(ctx context.Context, tx *sql.Tx, candidate deadlineCandidate, state, data []byte, logger *slog.Logger) error {
	env := &envelopepb.Envelope{}
	if err := env.UnmarshalBinary(data); err != nil {
		return fmt.Errorf("%w: invalid deadline envelope: %v", messagepump.ErrFailed, err)
	}
	if err := env.Validate(); err != nil {
		return fmt.Errorf("%w: invalid deadline envelope: %v", messagepump.ErrFailed, err)
	}
	var deadline dogma.Deadline
	if err := xmessage.UnpackMessage(env, &deadline); err != nil {
		return fmt.Errorf("%w: invalid deadline: %v", messagepump.ErrFailed, err)
	}
	s := c.Sink
	root, err := newRoot(ctx, s.Handler, logger)
	if err != nil {
		return err
	}
	if state != nil {
		if err := xerrors.Recover(func() error { return root.UnmarshalBinary(state) }); err != nil {
			return fmt.Errorf("%w: unable to unmarshal process state: %v", messagepump.ErrFailed, err)
		}
	}
	description, err := xerrors.RecoverT(func() (string, error) {
		return root.ProcessInstanceDescription(false), nil
	})
	if err != nil {
		return fmt.Errorf("%w: unable to describe process instance: %v", messagepump.ErrFailed, err)
	}
	logger = logger.With(xslog.Envelope("message", env), slog.Group("process_instance",
		slog.String("id", candidate.instanceID), slog.String("description", description)))
	scope := &deadlineScope{messageScope: messageScope{
		instanceID:           candidate.instanceID,
		root:                 root,
		commandPacker:        s.Packer.PackEffects(env, s.Identity, envelope.WithInstanceID(candidate.instanceID)),
		deadlinePacker:       s.Packer.PackEffects(env, s.Identity, envelope.WithInstanceID(candidate.instanceID)),
		logger:               logger,
		outboundMessageTypes: s.OutboundMessageTypes,
	}, scheduledFor: env.GetBody().GetScheduledFor().AsTime()}
	if err := xerrors.Recover(func() error {
		ctx, cancel := context.WithTimeout(ctx, s.HandleTimeout)
		defer cancel()
		return s.Handler.HandleDeadline(ctx, root, scope, deadline)
	}); err != nil {
		return fmt.Errorf("%w: unable to handle deadline: %v", messagepump.ErrFailed, err)
	}
	effects, err := prepareEffects(&scope.messageScope)
	if err != nil {
		return err
	}
	if err := effects.persist(ctx, tx, s.Identity.GetKey(), candidate.instanceID); err != nil {
		return err
	}
	if effects.ended {
		return nil
	}
	return xsql.ExecOne(ctx, tx, `DELETE FROM process.deadlines WHERE message_id = $1`, xsql.UUID(candidate.messageID))
}
