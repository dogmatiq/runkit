package xtesting

import (
	"testing"

	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// WaitForNoPendingDeadlines waits until there are no pending deadlines. If this
// does not occur within [WaitTimeout], the test fails.
func WaitForNoPendingDeadlines(
	t testing.TB,
	q xsql.Querier,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		"wait for no pending deadlines",
		0,
		q,
		`SELECT COUNT(*)
		FROM process.deadlines`,
	)
}
