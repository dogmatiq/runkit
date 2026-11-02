package scanner_test

import (
	"testing"
	"time"

	. "github.com/dogmatiq/runkit/internal/eventstream/internal/scanner"
)

// expectNoBatches asserts that no batch is delivered within a short period.
func expectNoBatches(
	t testing.TB,
	batches <-chan Batch,
) {
	t.Helper()

	select {
	case batch := <-batches:
		t.Fatalf("unexpected batch with checkpoint %d and %d envelopes", batch.Checkpoint, len(batch.Envelopes))
	case <-time.After(100 * time.Millisecond):
	}
}
