package xtesting

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// ExpectCommandQueueDrainedEventually asserts that all pending commands are
// eventually removed from the queue.
func ExpectCommandQueueDrainedEventually(
	t testing.TB,
	q xsql.Querier,
) {
	ExpectQueryResultEventually(
		t,
		"command queue drained",
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands`,
	)
}

// ExpectCommandQueueToContain asserts that the command queue contains all
// commands with the given IDs.
func ExpectCommandQueueToContain(
	t testing.TB,
	q xsql.Querier,
	messageIDs ...*uuidpb.UUID,
) {
	ExpectQueryResult(
		t,
		fmt.Sprintf(
			"command queue contains %d specific messages",
			len(messageIDs),
		),
		len(messageIDs),
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = ANY($1)`,
		xsql.UUIDs(messageIDs),
	)
}

// ExpectCommandQueueNotToContainEventually asserts that the command queue
// eventually does not contain the commands with the given IDs.
func ExpectCommandQueueNotToContainEventually(
	t testing.TB,
	q xsql.Querier,
	messageIDs ...*uuidpb.UUID,
) {
	ExpectQueryResultEventually(
		t,
		fmt.Sprintf(
			"command queue does not contain %d specific messages",
			len(messageIDs),
		),
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = ANY($1)`,
		xsql.UUIDs(messageIDs),
	)
}
