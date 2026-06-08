package xsql

import (
	"context"
	"fmt"
)

// ExecOne executes a query and asserts that exactly one row was affected.
func ExecOne(
	ctx context.Context,
	x Executor,
	query string,
	args ...any,
) error {
	res, err := x.ExecContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("unable to execute query: %w", err)
	}

	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to determine number of rows affected: %w", err)
	}

	if n != 1 {
		return fmt.Errorf("expected exactly one row to be affected, got %d", n)
	}

	return nil
}
