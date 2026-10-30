package xtesting

import (
	"testing"
	"time"

	"github.com/dogmatiq/dapper"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
)

// Expect asserts that got and want represent the same value.
func Expect[T any](
	t testing.TB,
	description string,
	got, want T,
) {
	t.Helper()

	gotString := dapper.Format(got)
	wantString := dapper.Format(want)

	edits := myers.ComputeEdits(
		"",
		wantString,
		gotString,
	)

	if len(edits) == 0 {
		return
	}

	diff := gotextdiff.ToUnified("want", "got", wantString, edits)

	t.Logf("expectation failed: %s", description)
	t.Log(diff)
	t.FailNow()
}

// ExpectN asserts that got and want represent the same values.
func ExpectN[T any](
	t testing.TB,
	description string,
	got []T,
	want ...T,
) {
	t.Helper()
	Expect(t, description, got, want)
}

// ExpectToReceiveN asserts that a channel receives the values in the want
// slice, in order.
func ExpectToReceiveN[T any](
	t testing.TB,
	description string,
	got <-chan T,
	want ...T,
) {
	t.Helper()

	var collected []T

	for len(collected) < len(want) {
		select {
		case <-time.After(WaitTimeout):
			t.Fatalf("timeout waiting for %s", description)
		case v := <-got:
			collected = append(collected, v)
		}
	}

	Expect(t, description, collected, want)
}

// ExpectNoReceive asserts that nothing is received on the channel within a
// short period.
func ExpectNoReceive[T any](
	t testing.TB,
	description string,
	ch <-chan T,
) {
	t.Helper()

	select {
	case v := <-ch:
		t.Fatalf("unexpected %s: %v", description, v)
	case <-time.After(100 * time.Millisecond):
	}
}
