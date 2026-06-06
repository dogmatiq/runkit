package databasetest

import (
	"database/sql"
	"errors"
	"testing"
)

// Expect executes a database query and fails the test if it does not produce a
// row with a truth value.
func Expect(
	t testing.TB,
	description string,
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

	var result bool
	if err := row.Scan(&result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			t.Fatalf("%s: no rows returned", description)
		} else {
			t.Fatal(err)
		}
	}

	if !result {
		t.Fatalf("%s: unexpected result: got false, want true", description)
	}
}
