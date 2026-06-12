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
	t.Helper()

	ExpectQueryResultEventually(
		t,
		"command queue drained",
		0,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands`,
	)
}

// ExpectCommandToBeQueued asserts that the command with the given ID is present
// in the queue.
func ExpectCommandToBeQueued(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		fmt.Sprintf("command queue contains message %q", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
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
	t.Helper()

	ExpectQueryResultEventually(
		t,
		fmt.Sprintf("command queue does not contain message %q", messageID),
		0,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// ExpectCommandToBeBackedOffDueToFailureEventually asserts that the command with
// the given ID has been backed off at least once due to a failure.
func ExpectCommandToBeBackedOffDueToFailureEventually(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	ExpectQueryResultEventually(
		t,
		fmt.Sprintf("command %q has been backed off due to failure", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1
		AND failures >= 1`,
		xsql.UUID(messageID),
	)
}
