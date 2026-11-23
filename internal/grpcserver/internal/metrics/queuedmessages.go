package metrics

import (
	"context"
	"fmt"
	"maps"
	"time"
	"uuid"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"github.com/dogmatiq/protobuf/uuidpb"
	"github.com/dogmatiq/runkit/internal/x/xsql"
	"google.golang.org/protobuf/proto"
)

// DueAndFailingQueuedMessages returns a [Group] of the metrics that count, for
// each queue and each handler, the queued messages that are due and the queued
// messages that are failing.
//
// A message is due when the time of its next attempt has come by the clock of
// the process that measures, and failing when it has failed at least once. A
// deadline's handler is the handler key in its row. A command's handler is the
// entry of commandHandlerKeys for the command's type ID. A command counts
// toward its handler only when commandHandlerKeys has an entry for its type,
// and always toward its queue.
func DueAndFailingQueuedMessages(
	commandHandlerKeys map[uuid.UUID]uuid.UUID,
) Group {
	commandHandlerKeys = maps.Clone(commandHandlerKeys)

	return NewGroup(
		"due and failing queued messages",
		func(
			ctx context.Context,
			q xsql.Querier,
			m Manifest,
		) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
			return measureDueAndFailingQueuedMessages(
				ctx,
				q,
				m,
				time.Now(),
				commandHandlerKeys,
			)
		},
		WithMetric[metricsgrpc.QueueCommandsDue](),
		WithMetric[metricsgrpc.QueueDeadlinesDue](),
		WithMetric[metricsgrpc.QueueCommandsFailing](),
		WithMetric[metricsgrpc.QueueDeadlinesFailing](),
		WithMetric[metricsgrpc.HandlerCommandsDue](),
		WithMetric[metricsgrpc.HandlerDeadlinesDue](),
		WithMetric[metricsgrpc.HandlerCommandsFailing](),
		WithMetric[metricsgrpc.HandlerDeadlinesFailing](),
	)
}

// messageCount identifies one count of queued messages: the messages that are
// due or failing, of one kind, in one queue or for one handler.
type messageCount struct {
	Failing  bool
	Deadline bool

	// Subject is the queue's ID or the handler's key.
	Subject uuid.UUID
}

// measureDueAndFailingQueuedMessages returns a measurement of the metrics of
// [DueAndFailingQueuedMessages]. A message is due if its next attempt is at or
// before the given time.
//
// One statement reads the messages that are due and the messages that are
// failing, grouped by queue, type and handler key, which the queue index and
// the index of failing messages hold. The rows are then added up into the
// counts of queues and handlers.
func measureDueAndFailingQueuedMessages(
	ctx context.Context,
	q xsql.Querier,
	m Manifest,
	measuredAt time.Time,
	commandHandlerKeys map[uuid.UUID]uuid.UUID,
) ([]*metricsgrpc.WatchMetricsResponse_Sample, error) {
	rows, err := q.QueryContext(
		ctx,
		`SELECT
			false,
			queue_id,
			message_type_id,
			handler_key,
			COUNT(*)
		FROM runkit.queued_messages
		WHERE next_attempt_at <= $1
		GROUP BY queue_id, message_type_id, handler_key
		UNION ALL
		SELECT
			true,
			queue_id,
			message_type_id,
			handler_key,
			COUNT(*)
		FROM runkit.queued_messages
		WHERE failures > 0
		GROUP BY queue_id, message_type_id, handler_key`,
		measuredAt,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to query due and failing queued messages: %w", err)
	}
	defer rows.Close()

	var (
		queues   = map[messageCount]uint64{}
		handlers = map[messageCount]uint64{}
	)

	for rows.Next() {
		var (
			failing    bool
			queueID    uuid.UUID
			typeID     uuid.UUID
			handlerKey uuid.UUID
			count      uint64
		)

		if err := rows.Scan(
			&failing,
			xsql.UUID(&queueID),
			xsql.UUID(&typeID),
			xsql.UUID(&handlerKey),
			&count,
		); err != nil {
			return nil, fmt.Errorf("unable to scan due and failing queued messages: %w", err)
		}

		// Only a deadline's row has a handler key.
		deadline := handlerKey != uuid.Nil()

		queues[messageCount{failing, deadline, queueID}] += count

		if !deadline {
			handlerKey = commandHandlerKeys[typeID]
		}

		if handlerKey != uuid.Nil() {
			handlers[messageCount{failing, deadline, handlerKey}] += count
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("unable to iterate due and failing queued messages: %w", err)
	}

	var samples []*metricsgrpc.WatchMetricsResponse_Sample

	for c, count := range queues {
		samples = append(
			samples,
			m.NewSample(c.Subject[:], newQueueMessageCount(c, count)),
		)
	}

	for c, count := range handlers {
		samples = append(
			samples,
			m.NewSample(c.Subject[:], newHandlerMessageCount(c, count)),
		)
	}

	return samples, nil
}

// newQueueMessageCount returns the message of the queue metric that holds the
// given count.
func newQueueMessageCount(c messageCount, count uint64) proto.Message {
	id := uuidpb.FromStd(c.Subject)

	switch {
	case !c.Failing && !c.Deadline:
		return metricsgrpc.NewQueueCommandsDueBuilder().
			WithQueueId(id).
			WithValue(count).
			Build()

	case !c.Failing:
		return metricsgrpc.NewQueueDeadlinesDueBuilder().
			WithQueueId(id).
			WithValue(count).
			Build()

	case !c.Deadline:
		return metricsgrpc.NewQueueCommandsFailingBuilder().
			WithQueueId(id).
			WithValue(count).
			Build()

	default:
		return metricsgrpc.NewQueueDeadlinesFailingBuilder().
			WithQueueId(id).
			WithValue(count).
			Build()
	}
}

// newHandlerMessageCount returns the message of the handler metric that holds
// the given count.
func newHandlerMessageCount(c messageCount, count uint64) proto.Message {
	key := uuidpb.FromStd(c.Subject)

	switch {
	case !c.Failing && !c.Deadline:
		return metricsgrpc.NewHandlerCommandsDueBuilder().
			WithHandlerKey(key).
			WithValue(count).
			Build()

	case !c.Failing:
		return metricsgrpc.NewHandlerDeadlinesDueBuilder().
			WithHandlerKey(key).
			WithValue(count).
			Build()

	case !c.Deadline:
		return metricsgrpc.NewHandlerCommandsFailingBuilder().
			WithHandlerKey(key).
			WithValue(count).
			Build()

	default:
		return metricsgrpc.NewHandlerDeadlinesFailingBuilder().
			WithHandlerKey(key).
			WithValue(count).
			Build()
	}
}
