package messagequeue

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

// candidate is the message that a consumer attempts next.
type candidate struct {
	// MessageID is the ID of the message.
	MessageID *uuidpb.UUID

	// NextAttemptAt is the earliest time that the message may be attempted.
	NextAttemptAt time.Time
}

// IsDue returns true if the message may be attempted now.
func (c candidate) IsDue() bool {
	return !time.Now().Before(c.NextAttemptAt)
}

// discover returns the message with the earliest next attempt time among the
// messages of the given types in the given queues, excluding the messages with
// the given IDs. It returns false if there are no such messages.
func discover(
	ctx context.Context,
	db *sql.DB,
	queueIDs *uuidcollection.Set,
	messageTypeIDs *uuidcollection.Set,
	excluded *uuidcollection.Set,
) (candidate, bool, error) {
	// Without any queues or message types, there are no messages to discover,
	// so the query is skipped.
	if queueIDs.IsEmpty() || messageTypeIDs.IsEmpty() {
		return candidate{}, false, nil
	}

	c := candidate{
		MessageID: &uuidpb.UUID{},
	}

	row := db.QueryRowContext(
		ctx,
		`SELECT m.message_id, m.next_attempt_at
		FROM unnest($1::uuid[]) AS k(queue_id)
		CROSS JOIN unnest($2::uuid[]) AS t(message_type_id)
		CROSS JOIN LATERAL (
			SELECT q.message_id, q.next_attempt_at
			FROM messagequeue.messages AS q
			WHERE q.queue_id = k.queue_id
				AND q.message_type_id = t.message_type_id
				AND q.message_id <> ALL($3::uuid[])
			ORDER BY q.next_attempt_at
			LIMIT 1
		) AS m
		ORDER BY m.next_attempt_at
		LIMIT 1`,
		xsql.UUIDSeq(queueIDs.All()),
		xsql.UUIDSeq(messageTypeIDs.All()),
		xsql.UUIDSeq(excluded.All()),
	)

	if err := row.Scan(
		xsql.UUID(c.MessageID),
		&c.NextAttemptAt,
	); err != nil {
		// There are no such messages.
		if errors.Is(err, sql.ErrNoRows) {
			return candidate{}, false, nil
		}

		return candidate{}, false, fmt.Errorf("unable to discover the next message: %w", err)
	}

	return c, true, nil
}
