package databasetest

import (
	"database/sql"
	"errors"
	"testing"
)

// Expect executes a database query and fails the test if it does not produce a
// row with the wanted value in the first column.
func Expect[T comparable](
	t testing.TB,
	description string,
	want T,
	x Executor,
	query string,
	args ...any,
) {
	t.Helper()

	row := x.QueryRowContext(
		t.Context(),
		query,
		args...,
	)

	var got T
	if err := row.Scan(&got); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("expectation failed: %s: got no rows, want %v", description, want)
		}

		t.Fatal(err)
	}

	if got != want {
		t.Fatalf("expectation failed: %s: got %v, want %v", description, got, want)
	}
}
