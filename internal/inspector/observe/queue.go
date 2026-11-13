package observe

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/dogmatiq/protobuf/envelopepb"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// stalledAfter is how long a message may wait after it becomes due before the
// inspector reports its queue as stalled. A node that owns a queue's workload
// normally delivers a due message within moments, so a longer wait means that
// delivery has stopped.
const stalledAfter = 30 * time.Second

// Queue is a queue of messages, summarized from the messages it holds.
//
// The engine has no table of queues. A queue exists while it holds at least
// one message.
type Queue struct {
	// ID is the queue's ID. It is also the rendezvous key of the workload of
	// delivering the queue's messages.
	ID *uuidpb.UUID

	// Messages is the number of messages in the queue.
	Messages int

	// Deadlines is the number of the queue's messages that are deadlines. The
	// rest are commands.
	Deadlines int

	// Due is the number of messages whose next attempt may run now.
	Due int

	// Failing is the number of messages whose most recent attempt failed.
	Failing int

	// MostFailures is the largest number of failed attempts in a row of any
	// one message in the queue.
	MostFailures int

	// FirstFailedAt is the earliest time that a message in the queue began
	// failing, or zero if none are failing.
	FirstFailedAt time.Time

	// DueSince is the earliest time that a message in the queue became due,
	// or zero if none are due.
	DueSince time.Time
}

// LongestWait returns how long the queue's oldest due message has been due,
// at the given time.
func (q Queue) LongestWait(now time.Time) time.Duration {
	if q.DueSince.IsZero() {
		return 0
	}
	return now.Sub(q.DueSince)
}

// IsStalled returns true if a message in the queue has been due for longer
// than a node normally takes to deliver it.
func (q Queue) IsStalled(now time.Time) bool {
	return q.LongestWait(now) > stalledAfter
}

// QueuedMessage is a command or deadline that has not yet been delivered.
type QueuedMessage struct {
	// ID is the message's ID.
	ID *uuidpb.UUID

	// QueueID is the ID of the message's queue.
	QueueID *uuidpb.UUID

	// HandlerKey is the key of the process handler that the message is
	// delivered to, if it is a deadline. It is nil for a command.
	HandlerKey *uuidpb.UUID

	// InstanceID is the ID of the process instance that the message is
	// delivered to, if it is a deadline.
	InstanceID string

	// Envelope is the message's envelope.
	Envelope *envelopepb.Envelope

	// Failures is the number of failures in the message's failure streak,
	// which is the run of delivery attempts in a row that have failed.
	Failures int

	// FirstFailedAt is the time of the first failure in the failure streak,
	// or zero if the most recent attempt did not fail.
	FirstFailedAt time.Time

	// FailureLoggedAsError is true if the engine has logged a failure of the
	// failure streak as an ERROR record. It logs at most one per streak.
	FailureLoggedAsError bool

	// ScheduledFor is the time that the message was meant to be delivered.
	ScheduledFor time.Time

	// NextAttemptAt is the earliest time that the next attempt may run.
	NextAttemptAt time.Time
}

// IsDeadline returns true if the message is a deadline.
func (m QueuedMessage) IsDeadline() bool {
	return m.HandlerKey != nil
}

// IsDue returns true if the message's next attempt may run at the given time.
func (m QueuedMessage) IsDue(now time.Time) bool {
	return !m.NextAttemptAt.After(now)
}

// IsFailing returns true if the message's most recent attempt failed.
func (m QueuedMessage) IsFailing() bool {
	return m.Failures > 0
}

// queuedMessageColumns is the columns that [scanQueuedMessage] reads, in
// order.
const queuedMessageColumns = `
	m.message_id,
	m.queue_id,
	m.handler_key,
	m.instance_id,
	m.envelope,
	m.failures,
	m.first_failed_at,
	m.failure_logged_as_error,
	m.scheduled_for,
	m.next_attempt_at`

// scanQueuedMessage reads one row of [queuedMessageColumns].
func scanQueuedMessage(rows interface{ Scan(...any) error }) (QueuedMessage, error) {
	var (
		m             QueuedMessage
		handlerKey    sql.Null[string]
		instanceID    sql.Null[string]
		envelope      []byte
		firstFailedAt sql.Null[time.Time]
	)

	m.ID = &uuidpb.UUID{}
	m.QueueID = &uuidpb.UUID{}

	if err := rows.Scan(
		xsql.UUID(m.ID),
		xsql.UUID(m.QueueID),
		&handlerKey,
		&instanceID,
		&envelope,
		&m.Failures,
		&firstFailedAt,
		&m.FailureLoggedAsError,
		&m.ScheduledFor,
		&m.NextAttemptAt,
	); err != nil {
		return QueuedMessage{}, err
	}

	if handlerKey.Valid {
		key, err := uuidpb.Parse(handlerKey.V)
		if err != nil {
			return QueuedMessage{}, err
		}
		m.HandlerKey = key
		m.InstanceID = instanceID.V
	}

	m.FirstFailedAt = firstFailedAt.V

	m.Envelope = &envelopepb.Envelope{}
	if err := m.Envelope.UnmarshalBinary(envelope); err != nil {
		return QueuedMessage{}, fmt.Errorf("unable to unmarshal envelope: %w", err)
	}

	return m, nil
}

// queuedGroup is the number of queued messages of one type for one handler.
type queuedGroup struct {
	// QueueID is the ID of the queue that holds the messages.
	QueueID *uuidpb.UUID

	// HandlerKey is the key of the process handler that the messages are
	// deadlines for, or nil if they are commands.
	HandlerKey *uuidpb.UUID

	// TypeID is the ID of the messages' type.
	TypeID *uuidpb.UUID

	// Messages is the number of messages.
	Messages int

	// Failing is the number of the messages whose most recent attempt failed.
	Failing int
}

// queuedGroups returns the number of queued messages of each type for each
// handler in each queue, in any order.
func (d *Database) queuedGroups(ctx context.Context) ([]queuedGroup, error) {
	var groups []queuedGroup

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT
					m.queue_id,
					m.handler_key,
					m.message_type_id,
					count(*),
					count(*) FILTER (WHERE m.failures > 0)
				FROM messagequeue.messages AS m
				GROUP BY m.queue_id, m.handler_key, m.message_type_id`,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var (
					g          queuedGroup
					handlerKey sql.Null[string]
				)

				g.QueueID = &uuidpb.UUID{}
				g.TypeID = &uuidpb.UUID{}

				if err := rows.Scan(
					xsql.UUID(g.QueueID),
					&handlerKey,
					xsql.UUID(g.TypeID),
					&g.Messages,
					&g.Failing,
				); err != nil {
					return err
				}

				if handlerKey.Valid {
					key, err := uuidpb.Parse(handlerKey.V)
					if err != nil {
						return err
					}
					g.HandlerKey = key
				}

				groups = append(groups, g)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to count queued messages: %w", err)
	}

	return groups, nil
}

// QueuedMessages returns the messages in the queue with the given ID, in the
// order that they become due, up to the given limit.
func (d *Database) QueuedMessages(
	ctx context.Context,
	queueID *uuidpb.UUID,
	limit int,
) ([]QueuedMessage, error) {
	return d.queuedMessages(
		ctx,
		`SELECT `+queuedMessageColumns+`
		FROM messagequeue.messages AS m
		WHERE m.queue_id = $1
		ORDER BY m.next_attempt_at, m.message_id
		LIMIT $2`,
		xsql.UUID(queueID),
		limit,
	)
}

// QueuedCommands returns the commands of the given types in the queues with
// the given IDs, in the order that they become due, up to the given limit.
func (d *Database) QueuedCommands(
	ctx context.Context,
	queueIDs []*uuidpb.UUID,
	typeIDs []*uuidpb.UUID,
	limit int,
) ([]QueuedMessage, error) {
	return d.queuedMessages(
		ctx,
		`SELECT `+queuedMessageColumns+`
		FROM messagequeue.messages AS m
		WHERE m.queue_id = ANY($1)
			AND m.message_type_id = ANY($2)
			AND m.handler_key IS NULL
		ORDER BY m.next_attempt_at, m.message_id
		LIMIT $3`,
		xsql.UUIDs(queueIDs...),
		xsql.UUIDs(typeIDs...),
		limit,
	)
}

// QueuedDeadlines returns the deadlines of the process handler with the given
// key that have not yet been delivered, in the order that they become due, up
// to the given limit.
//
// If instanceID is not empty, it returns only the deadlines of that instance.
func (d *Database) QueuedDeadlines(
	ctx context.Context,
	handlerKey *uuidpb.UUID,
	instanceID string,
	limit int,
) ([]QueuedMessage, error) {
	return d.queuedMessages(
		ctx,
		`SELECT `+queuedMessageColumns+`
		FROM messagequeue.messages AS m
		WHERE m.handler_key = $1
			AND ($2 = '' OR m.instance_id = $2)
		ORDER BY m.next_attempt_at, m.message_id
		LIMIT $3`,
		xsql.UUID(handlerKey),
		instanceID,
		limit,
	)
}

// QueuedMessage returns the message with the given ID. It returns false if no
// queue holds the message.
func (d *Database) QueuedMessage(
	ctx context.Context,
	id *uuidpb.UUID,
) (QueuedMessage, bool, error) {
	messages, err := d.queuedMessages(
		ctx,
		`SELECT `+queuedMessageColumns+`
		FROM messagequeue.messages AS m
		WHERE m.message_id = $1`,
		xsql.UUID(id),
	)
	if err != nil || len(messages) == 0 {
		return QueuedMessage{}, false, err
	}

	return messages[0], true, nil
}

func (d *Database) queuedMessages(
	ctx context.Context,
	query string,
	args ...any,
) ([]QueuedMessage, error) {
	var messages []QueuedMessage

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(ctx, query, args...)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				m, err := scanQueuedMessage(rows)
				if err != nil {
					return err
				}
				messages = append(messages, m)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read queued messages: %w", err)
	}

	return messages, nil
}

// queues returns every queue, with the most messages first, up to the given
// limit.
func (d *Database) queues(ctx context.Context, limit int) ([]Queue, error) {
	var queues []Queue

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			rows, err := tx.QueryContext(
				ctx,
				`SELECT
					m.queue_id,
					count(*),
					count(*) FILTER (WHERE m.handler_key IS NOT NULL),
					count(*) FILTER (WHERE m.next_attempt_at <= now()),
					count(*) FILTER (WHERE m.failures > 0),
					max(m.failures),
					min(m.first_failed_at),
					min(m.next_attempt_at) FILTER (WHERE m.next_attempt_at <= now())
				FROM messagequeue.messages AS m
				GROUP BY m.queue_id
				ORDER BY count(*) DESC, m.queue_id
				LIMIT $1`,
				limit,
			)
			if err != nil {
				return err
			}
			defer rows.Close()

			for rows.Next() {
				var (
					q             Queue
					firstFailedAt sql.Null[time.Time]
					dueSince      sql.Null[time.Time]
				)

				q.ID = &uuidpb.UUID{}

				if err := rows.Scan(
					xsql.UUID(q.ID),
					&q.Messages,
					&q.Deadlines,
					&q.Due,
					&q.Failing,
					&q.MostFailures,
					&firstFailedAt,
					&dueSince,
				); err != nil {
					return err
				}

				q.FirstFailedAt = firstFailedAt.V
				q.DueSince = dueSince.V
				queues = append(queues, q)
			}

			return rows.Err()
		},
	)
	if err != nil {
		return nil, fmt.Errorf("unable to read queues: %w", err)
	}

	return queues, nil
}

// removedMessages returns the number of rows that have been deleted from the
// messagequeue.messages table since PostgreSQL's statistics were last reset.
//
// The engine deletes a message when it delivers it, and deletes a process
// instance's deadlines when the instance ends, so the number counts both. It
// comes from PostgreSQL's table statistics, which PostgreSQL keeps in shared
// memory, so reading it reads no table. PostgreSQL also counts rows deleted
// by a transaction that then failed to commit.
func (d *Database) removedMessages(ctx context.Context) (uint64, error) {
	var n int64

	err := d.read(
		ctx,
		func(tx *sql.Tx) error {
			return tx.QueryRowContext(
				ctx,
				`SELECT n_tup_del
				FROM pg_stat_user_tables
				WHERE relid = 'messagequeue.messages'::regclass`,
			).Scan(&n)
		},
	)
	if err != nil {
		return 0, fmt.Errorf("unable to read the number of removed messages: %w", err)
	}

	return uint64(n), nil
}
