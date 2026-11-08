package messagepump

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// AcquireCommandDelivery attempts to acquire the next pending command for the
// handler from the command queue.
func AcquireCommandDelivery(
	ctx context.Context,
	tx *sql.Tx,
	messageTypeIDs *uuidcollection.Set,
) (Delivery, bool, error) {
	row := tx.QueryRowContext(
		ctx,
		`SELECT
			c.message_id,
			c.message_type_id,
			c.envelope,
			c.failures
		FROM messagequeue.messages AS c
		WHERE handler_key IS NULL
		AND message_type_id = ANY($1)
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

// PostponeCommandDelivery postpones a queued command for redelivery after delay.
func PostponeCommandDelivery(
	ctx context.Context,
	tx *sql.Tx,
	delivery Delivery,
	delay time.Duration,
) error {
	if err := xsql.ExecOne(
		ctx,
		tx,
		`UPDATE messagequeue.messages SET
			failures = $2,
			failing_since = CASE
				WHEN $2 = 0 THEN NULL
				WHEN failing_since IS NULL THEN clock_timestamp()
				ELSE failing_since
			END,
			deliver_at = clock_timestamp() + $3
		WHERE message_id = $1`,
		xsql.UUID(delivery.MessageID),
		delivery.Failures,
		delay,
	); err != nil {
		return fmt.Errorf("unable to postpone queued command: %w", err)
	}

	return nil
}
