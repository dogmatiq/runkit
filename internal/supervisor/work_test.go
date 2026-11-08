package supervisor_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/spruce"
)

func TestWork(t *testing.T) {
	failure := errors.New("<error>")

	// expectDelay asserts that the given delay is within the jitter applied to
	// the given backoff, which is from half of it to all of it.
	expectDelay := func(t *testing.T, got, backoff time.Duration) {
		t.Helper()

		if got < backoff/2 || got > backoff {
			t.Fatalf("unexpected delay: got %s, want from %s to %s", got, backoff/2, backoff)
		}
	}

	t.Run("it starts each streak at the base delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
			}

			expectDelay(t, w.Failed(t.Context(), failure), 100*time.Millisecond)
		})
	})

	t.Run("it doubles the delay up to the transient limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
			}

			for i := range 10 {
				backoff := min(100*time.Millisecond<<i, 15*time.Second)
				expectDelay(t, w.Failed(t.Context(), failure), backoff)
			}
		})
	})

	t.Run("it limits a deterministic failure's delay to 5 minutes", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
			}

			for i := range 14 {
				backoff := min(100*time.Millisecond<<i, 5*time.Minute)
				expectDelay(t, w.FailedDeterministically(t.Context(), failure), backoff)
			}
		})
	})

	t.Run("it ends a streak at a success when the quiet period is zero", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
			}

			w.Failed(t.Context(), failure)
			w.Failed(t.Context(), failure)
			w.Succeeded(t.Context())

			expectDelay(t, w.Failed(t.Context(), failure), 100*time.Millisecond)
		})
	})

	t.Run("it continues a streak when a failure comes within the quiet period", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			w.Failed(t.Context(), failure)
			w.Failed(t.Context(), failure)
			w.Succeeded(t.Context())
			time.Sleep(10 * time.Second)

			expectDelay(t, w.Failed(t.Context(), failure), 400*time.Millisecond)
		})
	})

	t.Run("it ends a streak when the quiet period passes without a failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			w.Failed(t.Context(), failure)
			w.Failed(t.Context(), failure)
			w.Succeeded(t.Context())
			time.Sleep(31 * time.Second)

			expectDelay(t, w.Failed(t.Context(), failure), 100*time.Millisecond)
		})
	})

	t.Run("it doesn't end a streak when the quiet period passes after its context is canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			ctx, cancel := context.WithCancel(t.Context())

			w.Failed(ctx, failure)
			w.Failed(ctx, failure)
			w.Succeeded(ctx)
			cancel()

			time.Sleep(31 * time.Second)
			synctest.Wait()

			expectDelay(t, w.Failed(t.Context(), failure), 400*time.Millisecond)
		})
	})

	t.Run("it writes nothing once the work is complete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			w.Failed(t.Context(), failure)
			w.Succeeded(t.Context())
			w.Complete()

			time.Sleep(31 * time.Second)
			synctest.Wait()
		})
	})

	t.Run("it tolerates successes and failures from different goroutines", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			w := &Work{
				Description:  "<work>",
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  time.Millisecond,
			}

			var g sync.WaitGroup

			for range 100 {
				g.Go(func() {
					w.Failed(t.Context(), failure)
					w.Succeeded(t.Context())
				})
			}

			g.Wait()
			time.Sleep(time.Second)
			w.Complete()
		})
	})
}
