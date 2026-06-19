package xtesting

import (
	"fmt"
	"testing"

	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// WaitForEmptyCommandQueue waits until the command queue is empty. If this does
// not occur within [WaitTimeout], the test fails.
func WaitForEmptyCommandQueue(
	t testing.TB,
	q xsql.Querier,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		"wait for empty command queue",
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
		fmt.Sprintf("expect command %q to be queued", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// WaitForCommandToBeRemovedFromQueue waits until the command with the given ID
// is removed from the queue. If this does not occur within [WaitTimeout], the
// test fails.
func WaitForCommandToBeRemovedFromQueue(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		fmt.Sprintf("wait for command %q to be removed from queue", messageID),
		0,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1`,
		xsql.UUID(messageID),
	)
}

// WaitForCommandToBePostponed waits until the command with the given ID has
// been postponed. If this does not occur within [WaitTimeout], the test fails.
func WaitForCommandToBePostponed(
	t testing.TB,
	q xsql.Querier,
	messageID *uuidpb.UUID,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		fmt.Sprintf("wait for command %q to be postponed", messageID),
		1,
		q,
		`SELECT COUNT(*)
		FROM commandqueue.commands
		WHERE message_id = $1
		AND execute_at > enqueued_at`,
		xsql.UUID(messageID),
	)
}
