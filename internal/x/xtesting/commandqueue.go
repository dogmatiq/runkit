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
	t.Helper()

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
	t.Helper()

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
	t.Helper()

	ExpectQueryResultEventually(
		t,
		fmt.Sprintf("command queue contains message %q with an attempt_at timestamp in the future", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands
		WHERE message_id = $1
		AND attempt_at > clock_timestamp()`,
		xsql.UUID(messageID),
	)
}

// ExpectCommandAttemptCount asserts that the command with the given ID has been
// attempted exactly the expected number of times.
func ExpectCommandAttemptCount(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
	want int,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		fmt.Sprintf("command %q has been attempted %d time(s)", messageID, want),
		want,
		q,
		`SELECT attempt_count
		FROM dogma.pending_commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}
