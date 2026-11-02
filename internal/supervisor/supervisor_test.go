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

func TestRun_backoff(t *testing.T) {
	// delays runs fn under Run until it has been called n times, and returns
	// the delay before each retry. fn is passed the number of the call, from
	// zero, and its error is returned for every call except the last.
	delays := func(
		t *testing.T,
		n int,
		fn func(call int) error,
	) []time.Duration {
		var (
			calls    int
			returned time.Time
			delays   []time.Duration
		)

		err := Run(
			t.Context(),
			spruce.NewTestLogger(t),
			func(context.Context) error {
				if calls > 0 {
					delays = append(delays, time.Since(returned))
				}

				call := calls
				calls++

				var err error
				if calls < n {
					err = fn(call)
				}

				returned = time.Now()
				return err
			},
		)
		if err != nil {
			t.Fatal(err)
		}

		return delays
	}

	t.Run("it caps the delay at 15 seconds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				15,
				func(int) error {
					return errors.New("<error>")
				},
			)

			for i, d := range got {
				if d > 15*time.Second {
					t.Fatalf("delay %d exceeds 15s: %s", i, d)
				}
			}

			if last := got[len(got)-1]; last < 7500*time.Millisecond {
				t.Fatalf("expected the final delay to reach the cap, got %s", last)
			}
		})
	})

	t.Run("it allows a longer delay after a persistent error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				15,
				func(int) error {
					return Persistent(errors.New("<error>"))
				},
			)

			for i, d := range got {
				if d > 5*time.Minute {
					t.Fatalf("delay %d exceeds 5m: %s", i, d)
				}
			}

			// Delays longer than the reset threshold must not reset the backoff,
			// so the final delays are close to the cap.
			if last := got[len(got)-1]; last < 150*time.Second {
				t.Fatalf("expected the final delay to reach the cap, got %s", last)
			}
		})
	})

	t.Run("it grows the delay from the transient cap after a persistent error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				17,
				func(call int) error {
					if call < 15 {
						return errors.New("<error>")
					}
					return Persistent(errors.New("<error>"))
				},
			)

			// The first persistent error follows 15 transient errors, so its
			// delay is drawn from double the 15 second cap.
			if d := got[15]; d > 30*time.Second {
				t.Fatalf("expected the delay to grow from the transient cap, got %s", d)
			}
		})
	})

	t.Run("it resets the delay when fn runs for long enough before failing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				6,
				func(call int) error {
					if call == 4 {
						time.Sleep(3 * time.Minute)
					}
					return errors.New("<error>")
				},
			)

			// The delay after the long-running call is drawn from the base
			// delay, which is at most 100ms.
			if d := got[4]; d > 100*time.Millisecond {
				t.Fatalf("expected the delay to reset, got %s", d)
			}
		})
	})
}

func TestGo(t *testing.T) {
	t.Run("it holds the context's error when canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)
			started := make(chan struct{})

			result, cancel := Go(
				t.Context(),
				logger,
				func(ctx context.Context) error {
					close(started)
					<-ctx.Done()
					return ctx.Err()
				},
			)

			<-started
			cancel()

			if err := result.Wait(); !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: got %v, want %v", err, context.Canceled)
			}
		})
	})

	t.Run("it holds a fatal error", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			logger := spruce.NewTestLogger(t)
			want := errors.New("<error>")

			result, cancel := Go(
				t.Context(),
				logger,
				func(context.Context) error {
					return Fatal(want)
				},
			)
			defer cancel()

			if err := result.Wait(); err != want {
				t.Fatalf("unexpected error: got %v, want %v", err, want)
			}
		})
	})
}
