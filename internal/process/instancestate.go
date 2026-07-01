package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"

	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/messagepump"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// newRoot creates a new process root by calling the handler's New() method.
func newRoot(
	ctx context.Context,
	handler dogma.ProcessMessageHandler[dogma.ProcessRoot],
	logger *slog.Logger,
) (dogma.ProcessRoot, error) {
	var root dogma.ProcessRoot

	if err := xerrors.ConvertPanicToError(
		func() error {
			root = handler.New()
			return nil
		},
	); err != nil {
		logger.ErrorContext(
			ctx,
			"unable to create process root",
			xslog.Error(err),
		)

		return nil, messagepump.ErrFailed
	}

	return root, nil
}

// loadInstance loads the process root state for the given instance.
// It returns false if the instance has ended.
func loadInstance(
	ctx context.Context,
	tx *sql.Tx,
	handlerKey *uuidpb.UUID,
	instanceID string,
	root dogma.ProcessRoot,
	logger *slog.Logger,
) (bool, error) {
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
			state`,
		xsql.UUID(handlerKey),
		instanceID,
	)

	var (
		ended bool
		state []byte
	)

	if err := row.Scan(&ended, &state); err != nil {
		return false, fmt.Errorf("unable to load process instance: %w", err)
	}

	if ended {
		return false, nil
	}

	if state != nil {
		if err := xerrors.ConvertPanicToError(
			func() error {
				return root.UnmarshalBinary(state)
			},
		); err != nil {
			logger.ErrorContext(
				ctx,
				"unable to unmarshal process instance state",
				xslog.Error(err),
			)
			return false, messagepump.ErrFailed
		}
	}

	return true, nil
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
	var data []byte

	if err := xerrors.ConvertPanicToError(
		func() error {
			var err error
			data, err = root.MarshalBinary()
			return err
		},
	); err != nil {
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
		`DELETE FROM process.deadlines
		WHERE handler_key = $1
			AND instance_id = $2`,
		xsql.UUID(handlerKey),
		instanceID,
	); err != nil {
		return fmt.Errorf("unable to delete deadlines for ended instance: %w", err)
	}

	return nil
}

// persistDeadlines inserts any deadlines scheduled by the handler into the
// process.deadlines table.
func persistDeadlines(
	ctx context.Context,
	tx *sql.Tx,
	packer *envelopepb.EffectPacker,
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
			`INSERT INTO process.deadlines (
				message_id,
				message_type_id,
				handler_key,
				instance_id,
				envelope,
				deliver_at
			) VALUES ($1, $2, $3, $4, $5, $6)`,
			xsql.UUID(deadlineEnvelope.GetBody().GetMessageId()),
			xsql.UUID(deadlineEnvelope.GetBody().GetMessage().GetTypeId()),
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
