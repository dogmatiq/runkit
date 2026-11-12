package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// instance is the engine's model of a process instance, as stored in the
// process.instances table.
//
// Its root is the application's model of the instance.
type instance struct {
	// HandlerKey is the identity key of the instance's handler.
	HandlerKey *uuidpb.UUID

	// ID is the instance's ID.
	ID string

	// Ended is true if the instance has ended.
	Ended bool

	// DeadlineQueueID is the ID of the queue for the instance's deadlines, or
	// the nil UUID if the instance isn't bound to a queue.
	DeadlineQueueID *uuidpb.UUID

	// Root is the instance's root. It's nil if the instance had already ended
	// when it was locked.
	Root dogma.ProcessRoot
}

// BindToDeadlineQueue binds the instance to a deadline queue, if it isn't bound
// to one. It binds it to the queue that the given router chooses, and sets
// [instance.DeadlineQueueID] to that queue's ID. The given transaction must
// hold the lock on the instance's row.
func (i *instance) BindToDeadlineQueue(
	ctx context.Context,
	tx *sql.Tx,
	router *messagequeue.Router,
) error {
	// A NULL deadline_queue_id scans as the nil UUID, whose bits are all zero.
	if !i.DeadlineQueueID.Equal(nil) {
		return nil
	}

	queueID, err := router.RouteDeadlinesToQueue(ctx, tx, i.HandlerKey, i.ID)
	if err != nil {
		return err
	}

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.instances SET
			deadline_queue_id = $1
		WHERE handler_key = $2
		AND instance_id = $3`,
		xsql.UUID(queueID),
		xsql.UUID(i.HandlerKey),
		i.ID,
	); err != nil {
		return fmt.Errorf("unable to bind process instance to deadline queue: %w", err)
	}

	i.DeadlineQueueID = queueID

	return nil
}

// Save stores the instance's state, marshaled from its root.
func (i *instance) Save(ctx context.Context, tx *sql.Tx, logger *slog.Logger) error {
	data, err := xerrors.RecoverT(
		func() ([]byte, error) {
			return i.Root.MarshalBinary()
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to marshal process instance state",
			xslog.Error(err),
		)

		return messagequeue.ErrFailed
	}

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.instances SET
			state = $1
		WHERE handler_key = $2
		AND instance_id = $3`,
		data,
		xsql.UUID(i.HandlerKey),
		i.ID,
	); err != nil {
		return fmt.Errorf("unable to save process instance: %w", err)
	}

	return nil
}

// End marks the instance as ended, clears its state and deletes its deadlines,
// except those that another transaction has locked.
//
// A deadline that it doesn't delete is still canceled. Before the deadline sink
// delivers a deadline, it checks whether the deadline's instance has ended. If
// it has, the consumer removes the deadline without delivering it.
func (i *instance) End(ctx context.Context, tx *sql.Tx) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.instances SET
			ended = true,
			state = NULL
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(i.HandlerKey),
		i.ID,
	); err != nil {
		return fmt.Errorf("unable to end process instance: %w", err)
	}

	// A deadline that another attempt has claimed is locked by that attempt's
	// transaction, so tx skips it rather than waiting for that transaction. The
	// deadline sink in that attempt finds the instance locked by tx, or ended
	// once tx commits, so the consumer postpones the delivery or removes the
	// deadline.
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM messagequeue.messages
		WHERE message_id IN (
			SELECT message_id
			FROM messagequeue.messages
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		)`,
		xsql.UUID(i.HandlerKey),
		i.ID,
	); err != nil {
		return fmt.Errorf("unable to delete deadlines for ended instance: %w", err)
	}

	i.Ended = true

	return nil
}

// AddDeadlinesToQueue adds the deadlines in the given packer to the instance's
// deadline queue. The instance must be bound to a queue.
func (i *instance) AddDeadlinesToQueue(
	ctx context.Context,
	tx *sql.Tx,
	packer *envelope.EffectPacker,
) error {
	deadlineEnvelopes, ok := packer.Seal()
	if !ok {
		return nil
	}

	for deadlineEnvelope := range deadlineEnvelopes.All() {
		data, err := deadlineEnvelope.MarshalBinary()
		if err != nil {
			return fmt.Errorf("unable to marshal deadline envelope: %w", err)
		}

		if _, err := tx.ExecContext(
			ctx,
			`INSERT INTO messagequeue.messages (
				message_id,
				message_type_id,
				queue_id,
				handler_key,
				instance_id,
				envelope,
				scheduled_for,
				next_attempt_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			xsql.UUID(deadlineEnvelope.GetBody().GetMessageId()),
			xsql.UUID(deadlineEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.UUID(i.DeadlineQueueID),
			xsql.UUID(deadlineEnvelope.GetHeader().GetSource().GetHandler().GetKey()),
			deadlineEnvelope.GetHeader().GetSource().GetInstanceId(),
			data,
			deadlineEnvelope.GetBody().GetScheduledFor().AsTime(),
		); err != nil {
			return fmt.Errorf("unable to add deadline to queue: %w", err)
		}
	}

	return nil
}

// lockInstance locks the instance with the given ID, creating it if it doesn't
// exist, and returns it. If another transaction holds the lock, it waits until
// that transaction ends.
//
// Unless the instance has ended, the returned instance is bound to a deadline
// queue, as [instance.BindToDeadlineQueue] describes, and has its root. It
// returns [messagequeue.ErrFailed] if the given handler can't create the root,
// or the root can't unmarshal the instance's stored state.
func lockInstance(
	ctx context.Context,
	tx *sql.Tx,
	handler dogma.ProcessMessageHandler[dogma.ProcessRoot],
	router *messagequeue.Router,
	handlerKey *uuidpb.UUID,
	instanceID string,
	logger *slog.Logger,
) (*instance, error) {
	// Upsert with a dummy update to acquire a row lock, guaranteeing
	// serialized access even for brand-new instances.
	row := tx.QueryRowContext(
		ctx,
		`INSERT INTO process.instances (
			handler_key,
			instance_id
		)
		VALUES ($1, $2)
		ON CONFLICT (handler_key, instance_id) DO UPDATE SET
			handler_key = EXCLUDED.handler_key
		RETURNING
			ended,
			state,
			deadline_queue_id`,
		xsql.UUID(handlerKey),
		instanceID,
	)

	inst := &instance{
		HandlerKey:      handlerKey,
		ID:              instanceID,
		DeadlineQueueID: &uuidpb.UUID{},
	}
	var state []byte

	if err := row.Scan(
		&inst.Ended,
		&state,
		xsql.UUID(inst.DeadlineQueueID),
	); err != nil {
		return nil, fmt.Errorf("unable to lock process instance: %w", err)
	}

	// An ended instance has no root.
	if inst.Ended {
		return inst, nil
	}

	if err := inst.BindToDeadlineQueue(ctx, tx, router); err != nil {
		return nil, err
	}

	root, err := newRoot(ctx, handler, state, logger)
	if err != nil {
		return nil, err
	}
	inst.Root = root

	return inst, nil
}

// tryLockInstance attempts to lock the existing instance with the given ID, and
// returns it.
//
// It returns false if the instance doesn't exist, and [messagequeue.ErrBusy] if
// another transaction holds the lock. Unless the instance has ended, the
// returned instance is bound to a deadline queue and has its root, as for
// [lockInstance]. It returns [messagequeue.ErrFailed] in the same cases as
// [lockInstance].
func tryLockInstance(
	ctx context.Context,
	tx *sql.Tx,
	handler dogma.ProcessMessageHandler[dogma.ProcessRoot],
	router *messagequeue.Router,
	handlerKey *uuidpb.UUID,
	instanceID string,
	logger *slog.Logger,
) (*instance, bool, error) {
	// Try to lock the instance and fetch its data in a single round-trip. The
	// data is loaded from the CTE that attempts to acquire the lock with FOR
	// UPDATE SKIP LOCKED so that it's only sent across the network if the lock
	// is acquired successfully.
	//
	// Another attempt to handle an event or deadline for the instance may hold
	// the instance's lock for up to the handle timeout. This function doesn't
	// wait for it, because waiting would keep tx and its connection open all
	// that time. SKIP LOCKED skips the row without an error, so tx remains
	// usable, and the consumer can use it to postpone the delivery.
	row := tx.QueryRowContext(
		ctx,
		`WITH exists AS (
			SELECT EXISTS (
				SELECT 1
				FROM process.instances
				WHERE handler_key = $1
				AND instance_id = $2
			) AS exists
		), locked AS (
			SELECT
				ended,
				state,
				deadline_queue_id
			FROM process.instances
			WHERE handler_key = $1
			AND instance_id = $2
			FOR UPDATE SKIP LOCKED
		)
		SELECT
			COALESCE(locked.ended, false),
			locked.state,
			locked.deadline_queue_id,
			exists.exists,
			(SELECT COUNT(*) FROM locked) > 0 AS locked
		FROM exists
		LEFT JOIN locked
			ON true`,
		xsql.UUID(handlerKey),
		instanceID,
	)

	inst := &instance{
		HandlerKey:      handlerKey,
		ID:              instanceID,
		DeadlineQueueID: &uuidpb.UUID{},
	}
	var (
		state          []byte
		exists, locked bool
	)

	if err := row.Scan(
		&inst.Ended,
		&state,
		xsql.UUID(inst.DeadlineQueueID),
		&exists,
		&locked,
	); err != nil {
		return nil, false, fmt.Errorf("unable to lock process instance: %w", err)
	}

	if !exists {
		return nil, false, nil
	}

	// SKIP LOCKED skipped the row, so another transaction holds its lock.
	if !locked {
		logger.DebugContext(
			ctx,
			"instance is locked by another transaction",
		)
		return nil, true, messagequeue.ErrBusy
	}

	// An ended instance has no root.
	if inst.Ended {
		return inst, true, nil
	}

	if err := inst.BindToDeadlineQueue(ctx, tx, router); err != nil {
		return nil, true, err
	}

	root, err := newRoot(ctx, handler, state, logger)
	if err != nil {
		return nil, true, err
	}
	inst.Root = root

	return inst, true, nil
}

// newRoot creates a new process root by calling the handler's New() method, and
// unmarshals the given state into it, if there is any.
//
// The state is NULL until the handler first mutates an instance's root.
func newRoot(
	ctx context.Context,
	handler dogma.ProcessMessageHandler[dogma.ProcessRoot],
	state []byte,
	logger *slog.Logger,
) (dogma.ProcessRoot, error) {
	root, err := xerrors.RecoverT(
		func() (dogma.ProcessRoot, error) {
			return handler.New(), nil
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to create process root",
			xslog.Error(err),
		)

		return nil, messagequeue.ErrFailed
	}

	if state == nil {
		return root, nil
	}

	if err := xerrors.Recover(
		func() error {
			return root.UnmarshalBinary(state)
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to unmarshal process instance state",
			xslog.Error(err),
		)

		return nil, messagequeue.ErrFailed
	}

	return root, nil
}

// instanceLogAttr returns an attribute that describes the process instance with
// the given ID and root, as it is now.
func instanceLogAttr(instanceID string, root dogma.ProcessRoot, ended bool) slog.Attr {
	return slog.Group(
		"process_instance",
		slog.String("id", instanceID),
		slog.String("description", root.ProcessInstanceDescription(ended)),
	)
}
