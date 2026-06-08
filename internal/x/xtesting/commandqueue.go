package xtesting

import (
	"testing"
)

// ExpectCommandQueueDrainedEventually asserts that all pending commands are eventually
func ExpectCommandQueueDrainedEventually(
	t testing.TB,
	x DatabaseExecutor,
) {
	ExpectQueryResultEventually(
		t,
		"all pending commands to be dequeued",
		0,
		x,
		`SELECT COUNT(*)
		FROM dogma.pending_commands`,
	)
}
