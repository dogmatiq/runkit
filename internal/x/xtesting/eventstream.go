package xtesting

import (
	"testing"

	"github.com/dogmatiq/reference-engine/internal/x/xsql"
)

// ExpectEventStreamCount asserts that the number of event streams in the database
// matches the given value.
func ExpectEventStreamCount(
	t testing.TB,
	q xsql.Querier,
	want int,
) {
	t.Helper()

	ExpectQueryResult(
		t,
		"event stream count",
		want,
		q,
		`SELECT COUNT(*)
		FROM dogma.event_streams`,
	)
}
