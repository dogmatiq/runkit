package xtesting

import (
	"database/sql"
	"log/slog"
	"testing"

	"github.com/dogmatiq/runkit/internal/signal"
)

// RunSignalListener runs a signal listener on db, and waits until it is
// listening for signals.
func RunSignalListener(
	t testing.TB,
	db *sql.DB,
	logger *slog.Logger,
) *signal.Listener {
	t.Helper()

	signals := &signal.Listener{
		DB:     db,
		Logger: logger,
	}

	// Once the listener is listening it signals every subscriber, whatever its
	// topic.
	listening := make(chan string, 1)
	defer signals.Subscribe("xtesting", listening)()

	GoE(t, signals.Run)
	ExpectToReceiveN(t, "signal listener to start listening", listening, "")

	return signals
}
