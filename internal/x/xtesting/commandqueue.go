package xtesting

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// ExpectEmptyCommandQueueEventually asserts that all pending commands are
// eventually removed from the queue.
func ExpectEmptyCommandQueueEventually(
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

// ExpectCommandToBeQueued asserts that the command with the given ID is present
// in the queue.
func ExpectCommandToBeQueued(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	ExpectQueryResult(
		t,
		fmt.Sprintf("command queue contains message %q", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// ExpectCommandToBeRemovedFromQueueEventually asserts that the command with the
// given ID is eventually removed from the queue.
func ExpectCommandToBeRemovedFromQueueEventually(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	ExpectQueryResultEventually(
		t,
		fmt.Sprintf("command queue does not contain message %q", messageID),
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// ExpectCommandToBeDeferredEventually asserts that the command with the
// given ID on the queue to be handled at a future time.
func ExpectCommandToBeDeferredEventually(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	ExpectQueryResultEventually(
		t,
		fmt.Sprintf("command queue contains message %q with a next_attempt_at timestamp in the future", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = $1
			AND next_attempt_at > clock_timestamp()`,
		xsql.UUID(messageID),
	)
}
