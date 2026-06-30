package process

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"

	"github.com/dogmatiq/dogma"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xerrors"
	"github.com/dogmatiq/reference-engine/internal/x/xslog"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

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
		return errFailed
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

// addCommandsToQueue inserts any commands scheduled by the handler into the
// command queue.
func addCommandsToQueue(
	ctx context.Context,
	tx *sql.Tx,
	packer *envelopepb.EffectPacker,
) error {
	commandEnvelopes, ok := packer.Seal()
	if !ok {
		return nil
	}

	for commandEnvelope := range commandEnvelopes.All() {
		if _, err := tx.ExecContext(
			ctx,
			`SELECT commandqueue.add($1, $2, $3, $4, $5)`,
			xsql.UUID(commandEnvelope.GetBody().GetMessageId()),
			xsql.UUID(commandEnvelope.GetHeader().GetCorrelationId()),
			xsql.UUID(commandEnvelope.GetBody().GetMessage().GetTypeId()),
			xsql.Envelope(commandEnvelope),
			commandEnvelope.GetBody().GetIdempotencyKey(),
		); err != nil {
			return fmt.Errorf("unable to add command to queue: %w", err)
		}
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
				handler_key,
				instance_id,
				envelope,
				deliver_at
			) VALUES ($1, $2, $3, $4, $5)`,
			xsql.UUID(deadlineEnvelope.GetBody().GetMessageId()),
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
