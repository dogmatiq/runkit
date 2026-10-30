package supervisor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/spruce"
)

func TestRun(t *testing.T) {
	t.Run("it returns a fatal error without retrying", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)

		want := errors.New("<error>")
		var calls atomic.Int32

		err := Run(
			t.Context(),
			logger,
			func(context.Context) error {
				calls.Add(1)
				return Fatal(want)
			},
		)

		if err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		if got := calls.Load(); got != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", got)
		}
	})
}

func TestGroup(t *testing.T) {
	t.Run("it finishes when fn succeeds", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)

		var (
			group Group
			calls atomic.Int32
		)

		group.Go(
			t.Context(),
			logger,
			func(context.Context) error {
				calls.Add(1)
				return nil
			},
		)

		group.Wait()

		if got := calls.Load(); got != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", got)
		}
	})

	t.Run("it retries after a delay until fn succeeds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)
			start := time.Now()

			var (
				group Group
				calls atomic.Int32
			)

			group.Go(
				t.Context(),
				logger,
				func(context.Context) error {
					if calls.Add(1) < 3 {
						return errors.New("<error>")
					}
					return nil
				},
			)

			group.Wait()

			if got := calls.Load(); got != 3 {
				t.Fatalf("unexpected number of calls: got %d, want 3", got)
			}

			// Two failures means two backoff delays. The first is at least
			// half of the base delay (50ms) and the second at least half of
			// the doubled delay (100ms), so at least 150ms must have elapsed
			// on the virtual clock.
			if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
				t.Fatalf("expected at least 150ms of backoff, got %s", elapsed)
			}
		})
	})

	t.Run("it finishes when the context is canceled during a delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)

			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
			defer cancel()

			var (
				group Group
				calls atomic.Int32
			)

			group.Go(
				ctx,
				logger,
				func(context.Context) error {
					calls.Add(1)
					return errors.New("<error>")
				},
			)

			group.Wait()

			if ctx.Err() == nil {
				t.Fatal("expected the group to finish only after the context was canceled")
			}

			if calls.Load() == 0 {
				t.Fatal("expected fn to be called at least once")
			}
		})
	})

	t.Run("it does not retry when fn returns after the context is canceled", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)

		ctx, cancel := context.WithCancel(t.Context())

		var (
			group Group
			calls atomic.Int32
		)

		group.Go(
			ctx,
			logger,
			func(context.Context) error {
				calls.Add(1)
				cancel()
				return errors.New("<error>")
			},
		)

		group.Wait()

		if got := calls.Load(); got != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", got)
		}
	})

	t.Run("it waits for multiple functions", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)

		var (
			group Group
			calls atomic.Int32
		)

		for range 3 {
			group.Go(
				t.Context(),
				logger,
				func(context.Context) error {
					calls.Add(1)
					return nil
				},
			)
		}

		group.Wait()

		if got := calls.Load(); got != 3 {
			t.Fatalf("unexpected number of calls: got %d, want 3", got)
		}
	})

}
