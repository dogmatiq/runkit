package failure_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/spruce"
)

func TestTracker(t *testing.T) {
	failure := errors.New("<error>")

	t.Run("it ends a streak at a success when the quiet period is zero", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
			}

			failures.WorkFailed(t.Context(), failure)
			failures.WorkFailed(t.Context(), failure)
			failures.WorkSucceeded(t.Context())

			expectNextAttempt(t, failures.WorkFailed(t.Context(), failure), 100*time.Millisecond)
		})
	})

	t.Run("it continues a streak when a failure comes within the quiet period", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			failures.WorkFailed(t.Context(), failure)
			failures.WorkFailed(t.Context(), failure)
			failures.WorkSucceeded(t.Context())
			time.Sleep(10 * time.Second)

			expectNextAttempt(t, failures.WorkFailed(t.Context(), failure), 400*time.Millisecond)
		})
	})

	t.Run("it ends a streak when the quiet period passes without a failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			failures.WorkFailed(t.Context(), failure)
			failures.WorkFailed(t.Context(), failure)
			failures.WorkSucceeded(t.Context())
			time.Sleep(31 * time.Second)

			expectNextAttempt(t, failures.WorkFailed(t.Context(), failure), 100*time.Millisecond)
		})
	})

	t.Run("it doesn't end a streak when the quiet period passes after its context is canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			ctx, cancel := context.WithCancel(t.Context())

			failures.WorkFailed(ctx, failure)
			failures.WorkFailed(ctx, failure)
			failures.WorkSucceeded(ctx)
			cancel()

			time.Sleep(31 * time.Second)
			synctest.Wait()

			expectNextAttempt(t, failures.WorkFailed(t.Context(), failure), 400*time.Millisecond)
		})
	})

	t.Run("it writes nothing once the work is complete", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  30 * time.Second,
			}

			failures.WorkFailed(t.Context(), failure)
			failures.WorkSucceeded(t.Context())
			failures.Complete()

			time.Sleep(31 * time.Second)
			synctest.Wait()
		})
	})

	t.Run("it tolerates successes and failures from different goroutines", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			failures := &Tracker{
				Description:  "<work>",
				Policy:       testPolicy,
				EngineLogger: spruce.NewTestLogger(t),
				QuietPeriod:  time.Millisecond,
			}

			var g sync.WaitGroup

			for range 100 {
				g.Go(func() {
					failures.WorkFailed(t.Context(), failure)
					failures.WorkSucceeded(t.Context())
				})
			}

			g.Wait()
			time.Sleep(time.Second)
			failures.Complete()
		})
	})
}
