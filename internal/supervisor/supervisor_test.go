package supervisor_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dogmatiq/runkit/internal/failure"
	. "github.com/dogmatiq/runkit/internal/supervisor"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/spruce"
)

// testPolicy is the failure policy that the tests use unless they need a
// particular value.
var testPolicy = failure.Policy{
	RetryDelay:                   100 * time.Millisecond,
	TransientRetryDelayLimit:     15 * time.Second,
	DeterministicRetryDelayLimit: 5 * time.Minute,
	ErrorLogThreshold:            30 * time.Second,
}

func TestRun(t *testing.T) {
	t.Run("it returns a fatal error without retrying", func(t *testing.T) {
		logger := spruce.NewTestLogger(t)

		want := xerrors.Fatal(errors.New("<error>"))
		var calls atomic.Int32

		err := Run(
			t.Context(),
			"<work>",
			testPolicy,
			logger,
			func(context.Context) error {
				calls.Add(1)
				return want
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
	// delaysWithPolicy runs fn under Run with the given failure policy until it
	// has been called n times, and returns the delay before each retry. fn is
	// passed the number of the call, from zero, and its error is returned for
	// every call except the last.
	delaysWithPolicy := func(
		t *testing.T,
		policy failure.Policy,
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
			"<work>",
			policy,
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

	// delays is delaysWithPolicy with the default failure policy.
	delays := func(
		t *testing.T,
		n int,
		fn func(call int) error,
	) []time.Duration {
		return delaysWithPolicy(t, testPolicy, n, fn)
	}

	t.Run("it limits the delay to the policy's limits", func(t *testing.T) {
		policy := testPolicy
		policy.TransientRetryDelayLimit = time.Second
		policy.DeterministicRetryDelayLimit = 2 * time.Second

		for name, err := range map[string]error{
			"transient":     errors.New("<error>"),
			"deterministic": xerrors.Deterministic(errors.New("<error>")),
		} {
			t.Run(name, func(t *testing.T) {
				synctest.Test(t, func(t *testing.T) {
					limit := policy.TransientRetryDelayLimit
					if xerrors.IsDeterministic(err) {
						limit = policy.DeterministicRetryDelayLimit
					}

					got := delaysWithPolicy(
						t,
						policy,
						15,
						func(int) error { return err },
					)

					for i, d := range got {
						if d > limit {
							t.Fatalf("delay %d exceeds %s: %s", i, limit, d)
						}
					}

					if last := got[len(got)-1]; last < limit/2 {
						t.Fatalf("expected the final delay to reach the limit, got %s", last)
					}
				})
			})
		}
	})

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

	t.Run("it allows a longer delay after a deterministic failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				15,
				func(int) error {
					return xerrors.Deterministic(errors.New("<error>"))
				},
			)

			for i, d := range got {
				if d > 5*time.Minute {
					t.Fatalf("delay %d exceeds 5m: %s", i, d)
				}
			}

			// Delays longer than the quiet period must not end the streak, so the
			// final delays are close to the cap.
			if last := got[len(got)-1]; last < 150*time.Second {
				t.Fatalf("expected the final delay to reach the cap, got %s", last)
			}
		})
	})

	t.Run("it allows a longer delay after a panic", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				15,
				func(int) error {
					return xerrors.PanicError{
						Value: "<panic>",
					}
				},
			)

			if last := got[len(got)-1]; last < 150*time.Second {
				t.Fatalf("expected the final delay to reach the deterministic cap, got %s", last)
			}
		})
	})

	t.Run("it resets the delay when fn runs for the quiet period before failing", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			got := delays(
				t,
				6,
				func(call int) error {
					if call == 4 {
						time.Sleep(31 * time.Second)
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
