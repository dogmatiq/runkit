package xtesting

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/dogmatiq/reference-engine/internal/database"
)

// Wait polls a condition until it returns true or the timeout elapses.
func Wait(
	t testing.TB,
	description string,
	condition func() bool,
) {
	t.Helper()

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	for {
		select {
		case <-ticker.C:
			if condition() {
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for condition: %s", description)
		}
	}
}

// WaitQuery polls a database query until it returns true or the timeout elapses.
func WaitQuery(
	t testing.TB,
	q database.Querier,
	description, query string,
	args ...any,
) {
	t.Helper()

	Wait(
		t,
		description,
		func() bool {
			t.Helper()

			row := q.QueryRowContext(
				t.Context(),
				query,
				args...,
			)

			var result bool
			if err := row.Scan(&result); err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					return false
				}
				t.Fatal(err)
			}

			return result
		},
	)
}
