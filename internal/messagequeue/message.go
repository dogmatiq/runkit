package messagequeue

import (
	"context"
	"fmt"
	"time"

	"github.com/dogmatiq/enginekit/uuidcollection"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// message is a message in one of the queues, without its envelope.
type message struct {
	// ID is the message's ID.
	ID *uuidpb.UUID

	// NextAttemptAt is the earliest time that the message may be attempted.
	NextAttemptAt time.Time
}

// IsDue returns true if the message may be attempted now.
func (m message) IsDue() bool {
	return !time.Now().Before(m.NextAttemptAt)
}

// findNextMessages returns the next messages to attempt.
//
// They're the messages in the given queues with the earliest next attempt
// times, other than the messages already being attempted and the messages in
// c.contendedMessages. It returns no more than fit in the given headroom.
//
// If it can't find them, it tries again after a delay, until it succeeds or ctx
// is canceled. It returns nil if ctx is canceled.
func (c *Consumer) findNextMessages(
	ctx context.Context,
	queues *uuidcollection.Set,
	headroom int,
) []message {
	for {
		messages, err := c.findNextMessagesOnce(ctx, queues, headroom)

		// The consumer is stopping, so the error isn't a failure to find the
		// next messages.
		if ctx.Err() != nil {
			return nil
		}

		// The next messages were found, which ends any streak of failures.
		if err == nil {
			c.failures.WorkSucceeded(ctx)
			return messages
		}

		nextAttemptAt := c.failures.WorkFailed(ctx, err)

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(time.Until(nextAttemptAt)):
		}
	}
}

// findNextMessagesOnce is [Consumer.findNextMessages] without the retries.
func (c *Consumer) findNextMessagesOnce(
	ctx context.Context,
	queues *uuidcollection.Set,
	headroom int,
) ([]message, error) {
	// Without any queues or message types, there are no next messages, so the
	// query is skipped.
	if queues.IsEmpty() || c.messageTypeIDs.IsEmpty() {
		return nil, nil
	}

	rows, err := c.DB.QueryContext(
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
				AND q.message_id <> ALL($4::uuid[])
			ORDER BY q.next_attempt_at
			LIMIT $5
		) AS m
		ORDER BY m.next_attempt_at
		LIMIT $5`,
		xsql.UUIDSeq(queues.All()),
		xsql.UUIDSeq(c.messageTypeIDs.All()),
		xsql.UUIDSeq(c.attemptsInProgress.All()),
		xsql.UUIDSeq(c.contendedMessages.Keys()),
		headroom,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to find the next messages: %w", err)
	}
	defer rows.Close()

	var messages []message

	for rows.Next() {
		m := message{
			ID: &uuidpb.UUID{},
		}

		if err := rows.Scan(
			xsql.UUID(m.ID),
			&m.NextAttemptAt,
		); err != nil {
			return nil, fmt.Errorf("unable to scan the next message: %w", err)
		}

		messages = append(messages, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate the next messages: %w", err)
	}

	return messages, nil
}
