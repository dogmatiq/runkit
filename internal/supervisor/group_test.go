package supervisor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/spruce"
)

func TestGroup(t *testing.T) {
	t.Run("it finishes when fn succeeds", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)
		group, _ := WithContext(t.Context())

		var calls atomic.Int32

		group.Go(
			"<work>",
			testPolicy,
			logger,
			func(context.Context) error {
				calls.Add(1)
				return nil
			},
		)

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}

		if got := calls.Load(); got != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", got)
		}
	})

	t.Run("it retries after a delay until fn succeeds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)
			group, _ := WithContext(t.Context())
			start := time.Now()

			var calls atomic.Int32

			group.Go(
				"<work>",
				testPolicy,
				logger,
				func(context.Context) error {
					if calls.Add(1) < 3 {
						return errors.New("<error>")
					}
					return nil
				},
			)

			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}

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

	t.Run("it finishes without an error when the context is canceled during a delay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)

			ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
			defer cancel()

			group, _ := WithContext(ctx)

			var calls atomic.Int32

			group.Go(
				"<work>",
				testPolicy,
				logger,
				func(context.Context) error {
					calls.Add(1)
					return errors.New("<error>")
				},
			)

			if err := group.Wait(); err != nil {
				t.Fatal(err)
			}

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
		group, _ := WithContext(ctx)

		var calls atomic.Int32

		group.Go(
			"<work>",
			testPolicy,
			logger,
			func(context.Context) error {
				calls.Add(1)
				cancel()
				return errors.New("<error>")
			},
		)

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}

		if got := calls.Load(); got != 1 {
			t.Fatalf("unexpected number of calls: got %d, want 1", got)
		}
	})

	t.Run("it waits for multiple functions", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)
		group, _ := WithContext(t.Context())

		var calls atomic.Int32

		for range 3 {
			group.Go(
				"<work>",
				testPolicy,
				logger,
				func(context.Context) error {
					calls.Add(1)
					return nil
				},
			)
		}

		if err := group.Wait(); err != nil {
			t.Fatal(err)
		}

		if got := calls.Load(); got != 3 {
			t.Fatalf("unexpected number of calls: got %d, want 3", got)
		}
	})

	t.Run("it returns a fatal error and stops the other functions", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)
		group, ctx := WithContext(t.Context())
		want := xerrors.Fatal(errors.New("<error>"))

		group.Go(
			"<work>",
			testPolicy,
			logger,
			func(ctx context.Context) error {
				<-ctx.Done()
				return ctx.Err()
			},
		)

		group.Go(
			"<work>",
			testPolicy,
			logger,
			func(context.Context) error {
				return want
			},
		)

		if err := group.Wait(); err != want {
			t.Fatalf("unexpected error: got %v, want %v", err, want)
		}

		if ctx.Err() == nil {
			t.Fatal("expected the group's context to be canceled")
		}
	})
}
