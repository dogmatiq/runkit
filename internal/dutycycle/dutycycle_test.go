package dutycycle_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/dogmatiq/runkit/internal/dutycycle"
)

func TestRun(t *testing.T) {
	t.Run("it calls fn immediately", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			begin := time.Now()
			var calls []time.Time

			err := Run(
				ctx,
				time.Hour,
				1,
				func(context.Context) error {
					calls = append(calls, time.Now())
					cancel()
					return nil
				},
			)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: got %v, want %v", err, context.Canceled)
			}

			if len(calls) != 1 {
				t.Fatalf("unexpected number of calls: got %d, want 1", len(calls))
			}

			if !calls[0].Equal(begin) {
				t.Fatalf("the first call started %s after Run was called", calls[0].Sub(begin))
			}
		})
	})

	t.Run("it doesn't call fn if ctx is already canceled", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			cancel()

			err := Run(
				ctx,
				time.Second,
				1,
				func(context.Context) error {
					t.Fatal("fn was called")
					return nil
				},
			)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: got %v, want %v", err, context.Canceled)
			}
		})
	})

	t.Run("it returns the error from fn", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			want := errors.New("<error>")
			calls := 0

			err := Run(
				t.Context(),
				time.Second,
				1,
				func(context.Context) error {
					calls++
					if calls == 3 {
						return want
					}
					return nil
				},
			)
			if !errors.Is(err, want) {
				t.Fatalf("unexpected error: got %v, want %v", err, want)
			}
		})
	})

	t.Run("it stops when ctx is canceled while it waits", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			go func() {
				time.Sleep(10 * time.Millisecond)
				cancel()
			}()

			calls := 0

			err := Run(
				ctx,
				time.Hour,
				1,
				func(context.Context) error {
					calls++
					return nil
				},
			)
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("unexpected error: got %v, want %v", err, context.Canceled)
			}

			if calls != 1 {
				t.Fatalf("unexpected number of calls: got %d, want 1", calls)
			}
		})
	})
}

func TestRun_spacing(t *testing.T) {
	cases := []struct {
		Name         string
		MinInterval  time.Duration
		MaxDutyCycle float64
		CallDuration time.Duration
		WantGap      time.Duration
	}{
		{
			Name:         "a quick call waits for the minimum interval",
			MinInterval:  100 * time.Millisecond,
			MaxDutyCycle: 0.5,
			CallDuration: 10 * time.Millisecond,
			WantGap:      100 * time.Millisecond,
		},
		{
			Name:         "a slow call waits for the duty cycle",
			MinInterval:  10 * time.Millisecond,
			MaxDutyCycle: 0.25,
			CallDuration: 30 * time.Millisecond,
			WantGap:      120 * time.Millisecond,
		},
		{
			Name:         "a duty cycle of 1 allows the next call as the last ends",
			MinInterval:  10 * time.Millisecond,
			MaxDutyCycle: 1,
			CallDuration: 30 * time.Millisecond,
			WantGap:      30 * time.Millisecond,
		},
		{
			Name:         "a call as long as the minimum interval has the same gap under either limit",
			MinInterval:  50 * time.Millisecond,
			MaxDutyCycle: 1,
			CallDuration: 50 * time.Millisecond,
			WantGap:      50 * time.Millisecond,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var starts []time.Time
				stop := errors.New("<stop>")

				err := Run(
					t.Context(),
					tc.MinInterval,
					tc.MaxDutyCycle,
					func(context.Context) error {
						starts = append(starts, time.Now())

						if len(starts) == 4 {
							return stop
						}

						time.Sleep(tc.CallDuration)
						return nil
					},
				)
				if !errors.Is(err, stop) {
					t.Fatal(err)
				}

				for i := 1; i < len(starts); i++ {
					if got := starts[i].Sub(starts[i-1]); got != tc.WantGap {
						t.Fatalf(
							"unexpected time from start %d to start %d: got %s, want %s",
							i-1,
							i,
							got,
							tc.WantGap,
						)
					}
				}
			})
		})
	}
}

func TestRun_invalidArguments(t *testing.T) {
	cases := []struct {
		Name         string
		MinInterval  time.Duration
		MaxDutyCycle float64
	}{
		{
			Name:         "zero minimum interval",
			MinInterval:  0,
			MaxDutyCycle: 0.5,
		},
		{
			Name:         "negative minimum interval",
			MinInterval:  -time.Second,
			MaxDutyCycle: 0.5,
		},
		{
			Name:         "zero maximum duty cycle",
			MinInterval:  time.Second,
			MaxDutyCycle: 0,
		},
		{
			Name:         "negative maximum duty cycle",
			MinInterval:  time.Second,
			MaxDutyCycle: -0.1,
		},
		{
			Name:         "maximum duty cycle above 1",
			MinInterval:  time.Second,
			MaxDutyCycle: 1.1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.Name, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("expected a panic")
				}
			}()

			Run(
				t.Context(),
				tc.MinInterval,
				tc.MaxDutyCycle,
				func(context.Context) error {
					return nil
				},
			)
		})
	}
}
