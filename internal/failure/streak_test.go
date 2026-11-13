package failure_test

import (
	"errors"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/spruce"
)

func TestStreak(t *testing.T) {
	failure := errors.New("<error>")
	deterministic := xerrors.Deterministic(failure)

	t.Run("it returns the base delay for the first failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			nextAttemptAt := s.Extend(t.Context(), engineLogger, "<work>", failure)
			expectNextAttempt(t, nextAttemptAt, 100*time.Millisecond)
		})
	})

	t.Run("it doubles the delay for each failure up to the transient limit", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			for i := range 10 {
				nextAttemptAt := s.Extend(t.Context(), engineLogger, "<work>", failure)
				expectNextAttempt(t, nextAttemptAt, min(100*time.Millisecond<<i, 15*time.Second))
			}
		})
	})

	t.Run("it uses the deterministic limit for a deterministic failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			for i := range 14 {
				nextAttemptAt := s.Extend(t.Context(), engineLogger, "<work>", deterministic)
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

			nextAttemptAt := s.Extend(t.Context(), engineLogger, "<work>", deterministic)
			expectNextAttempt(t, nextAttemptAt, 100*time.Millisecond<<10)
		})
	})

	t.Run("it doesn't overflow after many failures", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			s := Streak{
				Failures:      1000,
				FirstFailedAt: time.Now(),
			}

			nextAttemptAt := s.Extend(t.Context(), engineLogger, "<work>", failure)
			expectNextAttempt(t, nextAttemptAt, 15*time.Second)
		})
	})

	t.Run("it starts the streak at the first failure", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			engineLogger := spruce.NewTestLogger(t)
			var s Streak

			s.Extend(t.Context(), engineLogger, "<work>", failure)

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

			s.Extend(t.Context(), engineLogger, "<work>", failure)

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

				s.Extend(t.Context(), engineLogger, "<work>", deterministic)

				if !s.FailureLoggedAsError {
					t.Fatal("expected the streak to have logged its ERROR")
				}
			})
		})

		t.Run("when the streak is 30 seconds old", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				engineLogger := spruce.NewTestLogger(t)
				var s Streak

				s.Extend(t.Context(), engineLogger, "<work>", failure)

				if s.FailureLoggedAsError {
					t.Fatal("did not expect the streak to have logged its ERROR before it is 30 seconds old")
				}

				time.Sleep(30 * time.Second)
				s.Extend(t.Context(), engineLogger, "<work>", failure)

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

			s.Extend(t.Context(), engineLogger, "<work>", deterministic)
			s.End(t.Context(), engineLogger, "<work>")

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
