package xtesting

import (
	"reflect"
	"testing"

	"github.com/dogmatiq/dapper"
)

// ExpectEqual asserts that got and want are deeply equal.
func ExpectEqual[T any](
	t testing.TB,
	description string,
	got, want T,
) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Logf("expectation failed: %s", description)
		t.Logf("+++ got:\n%s", dapper.Format(got))
		t.Logf("--- want:\n%s", dapper.Format(want))
		t.FailNow()
	}
}
