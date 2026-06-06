package xtesting

import (
	"context"
	"testing"
	"time"
)

const (
	// WaitTimeout is the duration that [WaitUntil] will wait before timing out.
	WaitTimeout = 5 * time.Second

	// WaitPollInterval is the duration that [WaitUntil] will wait between calls
	// to the predicate function.
	WaitPollInterval = 10 * time.Millisecond
)

// WaitUntil repeatedly calls fn until it returns true or [WaitTimeout] elapses.
func WaitUntil(
	t testing.TB,
	description string,
	fn func() bool,
) {
	t.Helper()

	ticker := time.NewTicker(WaitPollInterval)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	for {
		select {
		case <-ticker.C:
			if fn() {
				return
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for condition: %s", description)
		}
	}
}
