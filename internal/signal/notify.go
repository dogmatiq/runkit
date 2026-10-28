package signal

import (
	"context"
	"fmt"

	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// Notify sends a signal on the given topic.
//
// If db is a transaction, the signal is sent when it is committed.
func Notify(ctx context.Context, db xsql.Executor, topic string) error {
	if _, err := db.ExecContext(
		ctx,
		`SELECT pg_notify($1, $2)`,
		channel,
		topic,
	); err != nil {
		return fmt.Errorf("unable to send signal: %w", err)
	}

	return nil
}
