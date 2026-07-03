package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// AcquireDeliveryFromCommandQueue attempts to acquire the next pending command
// for the handler from the command queue.
func AcquireDeliveryFromCommandQueue(
	ctx context.Context,
	tx *sql.Tx,
	messageTypeIDs *uuidpb.Set,
) (Delivery, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.message_id,
			c.message_type_id,
			c.envelope,
			c.failures
		FROM commandqueue.commands AS c
		WHERE message_type_id = ANY($1)
		AND deliver_at <= clock_timestamp()
		ORDER BY deliver_at
		LIMIT 1
		FOR UPDATE SKIP LOCKED`,
		xsql.UUIDSeq(messageTypeIDs.All()),
	)

	delivery := Delivery{
		MessageID:     &uuidpb.UUID{},
		MessageTypeID: &uuidpb.UUID{},
	}

	if err := row.Scan(
		xsql.UUID(delivery.MessageID),
		xsql.UUID(delivery.MessageTypeID),
		&delivery.EnvelopeBytes,
		&delivery.Failures,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Delivery{}, false, nil
		}

		return Delivery{}, false, fmt.Errorf("unable to scan pending command: %w", err)
	}

	return delivery, true, nil
}
