package xtesting

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// ExpectCommandQueueDrainedEventually asserts that all pending commands are eventually
func ExpectCommandQueueDrainedEventually(
	t testing.TB,
	q xsql.Querier,
) {
	ExpectQueryResultEventually(
		t,
		"all pending commands to be dequeued",
		0,
		q,
		`SELECT COUNT(*)
		FROM dogma.pending_commands`,
	)
}
