package xtesting

import (
	"database/sql"
	"fmt"
	"testing"
)

// WaitForProcessHandlerInitialization waits until the given process message
// handler has been fully initialized.
func WaitForProcessHandlerInitialization(
	t testing.TB,
	db *sql.DB,
	handlerKey string,
) {
	t.Helper()

	WaitForQueryResult(
		t,
		fmt.Sprintf("process handler %q is initialized", handlerKey),
		1,
		db,
		`SELECT COUNT(*)
		FROM process.handlers
		WHERE handler_key = $1`,
		handlerKey,
	)
}
