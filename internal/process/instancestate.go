package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/envelope"

	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/messagepump"
	"github.com/dogmatiq/runkit/internal/messagequeue"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// newRoot creates a new process root by calling the handler's New() method.
func newRoot(
	ctx context.Context,
	handler dogma.ProcessMessageHandler[dogma.ProcessRoot],
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

		return nil, messagepump.ErrFailed
	}

	return root, nil
}

// loadInstance loads the process root state for the given instance, and
// returns the ID of the queue for its deadlines. It returns false if the
// instance has ended.
//
// An instance that isn't yet bound to a queue is bound to the queue that the
// given router chooses.
func loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	router *messagequeue.Router,
	handlerKey *uuidpb.UUID,
	instanceID string,
	root dogma.ProcessRoot,
	logger *slog.Logger,
) (*uuidpb.UUID, bool, error) {
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

	var (
		ended bool
		state []byte
	)

	deadlineQueueID := &uuidpb.UUID{}

	if err := row.Scan(&ended, &state, xsql.UUID(deadlineQueueID)); err != nil {
		return nil, false, fmt.Errorf("unable to load process instance: %w", err)
	}

	if ended {
		return nil, false, nil
	}

	// A NULL deadline_queue_id scans as the nil UUID, whose bits are all zero.
	if deadlineQueueID.Equal(nil) {
		var err error
		deadlineQueueID, err = router.RouteDeadlinesToQueue(ctx, tx, handlerKey, instanceID)
		if err != nil {
			return nil, false, err
		}

		if err := xsql.ExecOne(
			ctx,
			tx,
			`UPDATE process.instances SET
				deadline_queue_id = $1
			WHERE handler_key = $2
			AND instance_id = $3`,
			xsql.UUID(deadlineQueueID),
			xsql.UUID(handlerKey),
			instanceID,
		); err != nil {
			return nil, false, fmt.Errorf("unable to bind process instance to deadline queue: %w", err)
		}
	}

	if state != nil {
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

			return nil, false, messagepump.ErrFailed
		}
	}

	return deadlineQueueID, true, nil
}

// saveInstance persists the process root state for the given instance.
func saveInstance(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
	instanceID string,
	root dogma.ProcessRoot,
	logger *slog.Logger,
) error {
	data, err := xerrors.RecoverT(
		func() ([]byte, error) {
			return root.MarshalBinary()
		},
	)
	if err != nil {
		logger.ErrorContext(
			ctx,
			"unable to marshal process instance state",
			xslog.Error(err),
		)

		return messagepump.ErrFailed
	}

	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.instances SET
			state = $1
		WHERE handler_key = $2
		AND instance_id = $3`,
		data,
		xsql.UUID(handlerKey),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to save process instance: %w", err)
	}

	return nil
}

// endInstance marks the process instance as ended, clears its state and
// deletes any pending deadlines.
func endInstance(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
	instanceID string,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE process.instances SET
			ended = true,
			state = NULL
		WHERE handler_key = $1
		AND instance_id = $2`,
		xsql.UUID(handlerKey),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to end process instance: %w", err)
	}

	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM messagequeue.messages
		WHERE handler_key = $1
			AND instance_id = $2`,
		xsql.UUID(handlerKey),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to delete deadlines for ended instance: %w", err)
	}

	return nil
}

// persistDeadlines adds any deadlines scheduled by the handler to the queue
// with the given ID.
func persistDeadlines(
	ctx context.Context,
	tx *sql.Tx,
	queueID *uuidpb.UUID,
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
				scheduled_at,
				deliver_at
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $7)`,
			xsql.UUID(deadlineEnvelope.GetBody().GetMessageId()),
			xsql.UUID(deadlineEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.UUID(queueID),
			xsql.UUID(deadlineEnvelope.GetHeader().GetSource().GetHandler().GetKey()),
			deadlineEnvelope.GetHeader().GetSource().GetInstanceId(),
			data,
			deadlineEnvelope.GetBody().GetScheduledFor().AsTime(),
		); err != nil {
			return fmt.Errorf("unable to persist deadline: %w", err)
		}
	}

	return nil
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
