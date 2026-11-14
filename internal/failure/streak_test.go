package failure_test

import (
	"errors"
	"math"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/spruce"
)

// testPolicy is the failure policy that the tests use unless they need a
// particular value.
var testPolicy = Policy{
	RetryDelay:                   100 * time.Millisecond,
	TransientRetryDelayLimit:     15 * time.Second,
	DeterministicRetryDelayLimit: 5 * time.Minute,
	ErrorLogThreshold:            30 * time.Second,
}

func TestStreak(t *testing.T) {
	failure := errors.New("<error>")
	deterministic := xerrors.Deterministic(failure)

	t.Run("it returns the policy's retry delay for the first failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			policy := testPolicy
			policy.RetryDelay = 8 * time.Second
			var s Streak

			nextAttemptAt := s.Extend(t.Context(), "<work>", policy, engineLogger, failure)
			expectNextAttempt(t, nextAttemptAt, 8*time.Second)
		})
	})

	t.Run("it doubles the delay for each failure up to the transient limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			for i := range 10 {
				nextAttemptAt := s.Extend(t.Context(), "<work>", testPolicy, engineLogger, failure)
				expectNextAttempt(t, nextAttemptAt, min(100*time.Millisecond<<i, 15*time.Second))
			}
		})
	})

	t.Run("it uses the deterministic limit for a deterministic failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			for i := range 14 {
				nextAttemptAt := s.Extend(t.Context(), "<work>", testPolicy, engineLogger, deterministic)
				expectNextAttempt(t, nextAttemptAt, min(100*time.Millisecond<<i, 5*time.Minute))
			}
		})
	})

	t.Run("it bases a deterministic failure's delay on the number of failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			s := Streak{
				Failures:      10,
				FirstFailedAt: time.Now(),
			}

			nextAttemptAt := s.Extend(t.Context(), "<work>", testPolicy, engineLogger, deterministic)
			expectNextAttempt(t, nextAttemptAt, 100*time.Millisecond<<10)
		})
	})

	t.Run("it limits the delay to the policy's limits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			policy := testPolicy
			policy.TransientRetryDelayLimit = 2 * time.Second
			policy.DeterministicRetryDelayLimit = 4 * time.Second

			transientStreak := Streak{
				Failures:      20,
				FirstFailedAt: time.Now(),
			}
			nextAttemptAt := transientStreak.Extend(t.Context(), "<work>", policy, engineLogger, failure)
			expectNextAttempt(t, nextAttemptAt, 2*time.Second)

			deterministicStreak := Streak{
				Failures:      20,
				FirstFailedAt: time.Now(),
			}
			nextAttemptAt = deterministicStreak.Extend(t.Context(), "<work>", policy, engineLogger, deterministic)
			expectNextAttempt(t, nextAttemptAt, 4*time.Second)
		})
	})

	t.Run("it limits the first delay to the limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			policy := testPolicy
			policy.RetryDelay = time.Minute
			policy.TransientRetryDelayLimit = 10 * time.Second
			var s Streak

			nextAttemptAt := s.Extend(t.Context(), "<work>", policy, engineLogger, failure)
			expectNextAttempt(t, nextAttemptAt, 10*time.Second)
		})
	})

	t.Run("it doesn't overflow after many failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			s := Streak{
				Failures:      math.MaxUint64 - 1,
				FirstFailedAt: time.Now(),
			}

			nextAttemptAt := s.Extend(t.Context(), "<work>", testPolicy, engineLogger, failure)
			expectNextAttempt(t, nextAttemptAt, 15*time.Second)
		})
	})

	t.Run("it doesn't overflow with a very long retry delay and limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			policy := testPolicy
			policy.RetryDelay = time.Hour
			policy.TransientRetryDelayLimit = math.MaxInt64
			s := Streak{
				Failures:      math.MaxUint64 - 1,
				FirstFailedAt: time.Now(),
			}

			nextAttemptAt := s.Extend(t.Context(), "<work>", policy, engineLogger, failure)
			expectNextAttempt(t, nextAttemptAt, math.MaxInt64)
		})
	})

	t.Run("it starts the streak at the first failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			s.Extend(t.Context(), "<work>", testPolicy, engineLogger, failure)

			if want := time.Now(); !s.FirstFailedAt.Equal(want) {
				t.Fatalf("unexpected time of the first failure: got %s, want %s", s.FirstFailedAt, want)
			}

			if s.Failures != 1 {
				t.Fatalf("unexpected number of failures: got %d, want 1", s.Failures)
			}
		})
	})

	t.Run("it keeps the start of a streak in progress", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			want := time.Now()
			time.Sleep(time.Minute)

			s := Streak{
				Failures:      3,
				FirstFailedAt: want,
			}

			s.Extend(t.Context(), "<work>", testPolicy, engineLogger, failure)

			if !s.FirstFailedAt.Equal(want) {
				t.Fatalf("unexpected time of the first failure: got %s, want %s", s.FirstFailedAt, want)
			}

			if s.Failures != 4 {
				t.Fatalf("unexpected number of failures: got %d, want 4", s.Failures)
			}
		})
	})

	t.Run("it marks that it logged its ERROR once it qualifies for one", func(t *testing.T) {
		t.Run("when the failure is deterministic", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				engineLogger := spruce.NewTestLogger(t)
				var s Streak

				s.Extend(t.Context(), "<work>", testPolicy, engineLogger, deterministic)

				if !s.FailureLoggedAsError {
					t.Fatal("expected the streak to have logged its ERROR")
				}
			})
		})

		t.Run("when the streak is as old as the policy's error log threshold", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				engineLogger := spruce.NewTestLogger(t)
				policy := testPolicy
				policy.ErrorLogThreshold = 10 * time.Second
				var s Streak

				s.Extend(t.Context(), "<work>", policy, engineLogger, failure)
				time.Sleep(9 * time.Second)
				s.Extend(t.Context(), "<work>", policy, engineLogger, failure)

				if s.FailureLoggedAsError {
					t.Fatal("did not expect the streak to have logged its ERROR before the threshold")
				}

				time.Sleep(time.Second)
				s.Extend(t.Context(), "<work>", policy, engineLogger, failure)

				if !s.FailureLoggedAsError {
					t.Fatal("expected the streak to have logged its ERROR")
				}
			})
		})
	})

	t.Run("it ends the streak when the work succeeds", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			s.Extend(t.Context(), "<work>", testPolicy, engineLogger, deterministic)
			s.End(t.Context(), "<work>", engineLogger)

			if s != (Streak{}) {
				t.Fatalf("expected no streak to be in progress, got %+v", s)
			}
		})
	})
}

// expectNextAttempt asserts that the given time of the next attempt is after
// now by the jitter applied to the given backoff, which is from half of it to
// all of it.
//
// It must be called within a synctest bubble, so that the clock hasn't moved
// since the failure.
func expectNextAttempt(t *testing.T, got time.Time, backoff time.Duration) {
	t.Helper()

	if delay := time.Until(got); delay < backoff/2 || delay > backoff {
		t.Fatalf("unexpected delay: got %s, want from %s to %s", delay, backoff/2, backoff)
	}
}
