package commandqueue

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/config"
	"github.com/dogmatiq/enginekit/protobuf/envelopepb"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/database"
)

const (
	// backoffBase is the base interval of the exponential backoff applied
	// to a Nack'd command before the next retry.
	backoffBase = 5 * time.Second

	// backoffCap is the maximum interval between retries.
	backoffCap = 5 * time.Minute

	// acquireLimit is the maximum number of commands returned by a single
	// call to [Acquire].
	acquireLimit = 32
)

// Enqueue enqueues a command for handling.
func Enqueue(
	ctx context.Context,
	tx *sql.Tx,
	envelope *envelopepb.Envelope,
) error {
	if idempotencyKey := envelope.GetBody().GetIdempotencyKey(); idempotencyKey != "" {
		if claimed, err := claimIdempotencyKey(ctx, tx, idempotencyKey); !claimed {
			return err
		}
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO command_queue (
			message_id,
			correlation_id,
			message_type_id,
			envelope
		) VALUES ($1, $2, $3, $4)`,
		database.MarshalUUID(envelope.GetBody().GetMessageId()),
		database.MarshalUUID(envelope.GetHeader().GetCorrelationId()),
		database.MarshalUUID(envelope.GetBody().GetMessage().GetTypeId()),
		database.MarshalEnvelope(envelope),
	); err != nil {
		return fmt.Errorf("unable to enqueue command: %w", err)
	}

	return nil
}

// claimIdempotencyKey attempts to claim the given idempotency key.
//
// It returns true if the key was successfully claimed by this call, or false if
// the key had already been claimed. claimed is always false if err is not nil.
func claimIdempotencyKey(
	ctx context.Context,
	tx *sql.Tx,
	idempotencyKey string,
) (claimed bool, err error) {
	res, err := tx.ExecContext(
		ctx,
		`INSERT INTO command_idempotency_keys (
				idempotency_key
			) VALUES ($1)
			ON CONFLICT (idempotency_key) DO NOTHING`,
		idempotencyKey,
	)
	if err != nil {
		return false, fmt.Errorf(
			"unable to claim idempotency key %q: %w",
			idempotencyKey,
			err,
		)
	}

	rowsAffected, err := res.RowsAffected()
	if err != nil {
		return false, fmt.Errorf(
			"unable to determine if idempotency key %q was claimed: %w",
			idempotencyKey,
			err,
		)
	}

	return rowsAffected == 1, nil
}

// Acquire locks a batch of unrouted commands of the types covered by the
// given route set and returns their envelopes. Rows are held under FOR
// UPDATE in tx until the caller commits or rolls back.
//
// The caller is expected to assign each returned command's route within
// the same transaction.
func Acquire(
	ctx context.Context,
	tx *sql.Tx,
	routes config.RouteSet,
) ([]*envelopepb.Envelope, error) {
	var messageTypeIDs []string
	commandRoutes := routes.
		Filter(config.FilterByRouteType(config.HandlesCommandRouteType)).
		Routes()

	for route := range commandRoutes {
		if id, ok := route.MessageTypeID.TryGet(); ok {
			messageTypeIDs = append(messageTypeIDs, id)
		}
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT envelope
		FROM command_queue
		WHERE message_type_id = ANY($1::uuid[])
			AND next_attempt_at <= now()
			AND routed_to_handler_key IS NULL
		ORDER BY next_attempt_at
		LIMIT $2
		FOR UPDATE SKIP LOCKED`,
		messageTypeIDs,
		acquireLimit,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to acquire unrouted commands: %w", err)
	}
	defer rows.Close()

	var envelopes []*envelopepb.Envelope
	for rows.Next() {
		env := &envelopepb.Envelope{}
		if err := rows.Scan(database.UnmarshalEnvelope(env)); err != nil {
			return nil, fmt.Errorf("unable to scan envelope: %w", err)
		}
		envelopes = append(envelopes, env)
	}

	return envelopes, rows.Err()
}

// Route assigns a command to a handler. aggregateInstanceID is the target
// instance for aggregate handlers, and the empty string for handlers that
// do not route per-instance (e.g. integrations).
//
// The caller must already hold a FOR UPDATE lock on the command's row in
// tx (typically because they obtained it via [Acquire]).
func Route(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
	handlerKey *uuidpb.UUID,
	aggregateInstanceID string,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE command_queue SET
			routed_to_handler_key = $1,
			routed_to_aggregate_instance_id = NULLIF($2, '')
		WHERE message_id = $3`,
		database.MarshalUUID(handlerKey),
		aggregateInstanceID,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to route command %s: %w", messageID, err)
	}

	return nil
}

// Ack confirms successful handling of the command with the given message ID.
//
// The caller must already hold a FOR UPDATE lock on the command's row in tx.
func Ack(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`DELETE FROM command_queue
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to ack command %s: %w", messageID, err)
	}

	return nil
}

// Nack records that the command with the given message ID failed, scheduling
// the command for retry with exponential backoff.
//
// The caller must already hold a FOR UPDATE lock on the command's row in tx.
func Nack(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE command_queue SET
			attempt_count = attempt_count + 1,
			next_attempt_at = now() + LEAST(
				pow(2, attempt_count) * $2,
				$3
			) * interval '1 second'
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
		backoffBase.Seconds(),
		backoffCap.Seconds(),
	); err != nil {
		return fmt.Errorf("unable to nack command %s: %w", messageID, err)
	}

	return nil
}

// Reset clears all transient state for the command, resetting it to the state
// it was when first enqueued. Any existing routing decision is also cleared
// so the command can be re-routed.
//
// The caller must already hold a FOR UPDATE lock on the command's row in tx.
func Reset(
	ctx context.Context,
	tx *sql.Tx,
	messageID *uuidpb.UUID,
) error {
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE command_queue SET
			routed_to_handler_key = NULL,
			routed_to_aggregate_instance_id = NULL,
			attempt_count = 0,
			next_attempt_at = clock_timestamp()
		WHERE message_id = $1`,
		database.MarshalUUID(messageID),
	); err != nil {
		return fmt.Errorf("unable to reset command %s: %w", messageID, err)
	}

	return nil
}
