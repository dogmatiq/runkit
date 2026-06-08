package xtesting

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dogmatiq/enginekit/x/xsync"
)

// WaitTimeout is the maximum amount of time that [ExpectLatchesSetEventually]
// will wait for latches to be set before failing the test.
const WaitTimeout = 3 * time.Second

// ExpectLatchesSetEventually waits for all of the given latches to be set. If
// fails the test if all latches are not set within [WaitTimeout].
func ExpectLatchesSetEventually(
	t testing.TB,
	latches ...*xsync.Latch,
) {
	t.Helper()

	ctx, cancel := context.WithTimeout(t.Context(), WaitTimeout)
	defer cancel()

	var (
		group sync.WaitGroup
		count atomic.Uint32
	)

	for _, latch := range latches {
		group.Go(func() {
			select {
			case <-ctx.Done():
			case <-latch.Chan():
				count.Add(1)
			}
		})
	}

	group.Wait()

	if got, want := count.Load(), uint32(len(latches)); got != want {
		t.Fatalf("timeout waiting for latches: got %d of %d latches set", got, want)
	}
}
