package messagequeue

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"time"
	"uuid"

	"github.com/dogmatiq/runkit/internal/uuidset"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// message is a message in one of the queues, without its envelope.
type message struct {
	// ID is the message's ID.
	ID uuid.UUID

	// QueueID is the ID of the queue that holds the message.
	QueueID uuid.UUID

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
// c.contendedMessages. It returns no more than fit in the given headroom, and
// no more than one from each serial queue.
//
// If it can't find them, it tries again after a delay, until it succeeds or ctx
// is canceled. It returns nil if ctx is canceled.
func (c *Consumer) findNextMessages(
	ctx context.Context,
	queues *uuidset.SetR,
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
	queues *uuidset.SetR,
	headroom int,
) ([]message, error) {
	// Without any queues or message types, there are no next messages, so the
	// query is skipped.
	if queues.IsEmpty() || c.messageTypeIDs.IsEmpty() {
		return nil, nil
	}

	skippedMessageIDs := slices.Concat(
		slices.Collect(maps.Keys(c.attemptsInProgress)),
		slices.Collect(maps.Keys(c.contendedMessages)),
	)

	rows, err := c.DB.QueryContext(
		ctx,
		`-- Search each queue, then take the messages from all of the searches in
		-- the order in which they're due, up to the headroom.
		SELECT
			next.message_id,
			queues.queue_id,
			next.next_attempt_at

		-- Start with the queues for which this node owns the workload.
		FROM unnest($1::uuid[]) AS queues(queue_id)

		-- Search each of those queues, reading its messages in the order in
		-- which they're due.
		CROSS JOIN LATERAL (
			SELECT
				m.message_id,
				m.next_attempt_at
			FROM runkit.queued_messages AS m
			WHERE m.queue_id = queues.queue_id
				-- Skip messages of a type with no sink. Those messages stay
				-- in the queue until a node that can deliver them owns the
				-- queue's workload.
				AND m.message_type_id = ANY($3::uuid[])
				-- Skip the messages that are being attempted, and the messages
				-- that were locked by another transaction.
				AND m.message_id <> ALL($4::uuid[])
			ORDER BY m.next_attempt_at
			-- Take one message from a serial queue. Take up to the headroom
			-- from a concurrent queue, because every message in the batch
			-- might come from that queue.
			LIMIT CASE WHEN queues.queue_id = ANY($2::uuid[]) THEN 1 ELSE $5 END
		) AS next

		-- Sort the messages from all of the searches by when they're due, and
		-- keep the earliest, up to the headroom.
		ORDER BY next.next_attempt_at
		LIMIT $5`,
		xsql.UUIDSeq(queues.All()),
		xsql.UUIDSeq(c.SerialQueueIDs.All()),
		xsql.UUIDSeq(c.messageTypeIDs.All()),
		xsql.UUIDSeq(slices.Values(skippedMessageIDs)),
		headroom,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to find the next messages: %w", err)
	}
	defer rows.Close()

	var messages []message

	for rows.Next() {
		var m message

		if err := rows.Scan(
			xsql.UUID(&m.ID),
			xsql.UUID(&m.QueueID),
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
