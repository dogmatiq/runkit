package runkit_test

import (
	"testing"
	"time"

	"github.com/dogmatiq/runkit"
)

func TestWithMessageQueueConcurrency(t *testing.T) {
	t.Run("it panics if the concurrency is zero", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithMessageQueueConcurrency(0)
	})

	t.Run("it accepts the default concurrency", func(t *testing.T) {
		runkit.WithMessageQueueConcurrency(
			runkit.DefaultMessageQueueConcurrency(),
		)
	})
}

func TestWithMessageQueueContentionDelay(t *testing.T) {
	t.Run("it panics if the delay is zero", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("expected a panic")
			}
		}()

		runkit.WithMessageQueueContentionDelay(0)
	})
}

func TestDefaults(t *testing.T) {
	t.Run("each option accepts its default", func(t *testing.T) {
		runkit.WithProjectionCompactInterval(runkit.DefaultProjectionCompactInterval())
		runkit.WithProjectionCompactTimeout(runkit.DefaultProjectionCompactTimeout())
		runkit.WithHandleTimeout(runkit.DefaultHandleTimeout())
		runkit.WithRouteTimeout(runkit.DefaultRouteTimeout())
		runkit.WithMessageQueueContentionDelay(runkit.DefaultMessageQueueContentionDelay())
	})
}

func TestFailurePolicyOptions(t *testing.T) {
	options := []struct {
		name   string
		option func(runkit.FailurePolicy) runkit.EngineOption
	}{
		{"WithEngineFailurePolicy", runkit.WithEngineFailurePolicy},
		{
			"WithMessageQueueDeliveryFailurePolicy",
			runkit.WithMessageQueueDeliveryFailurePolicy,
		},
		{
			"WithEventStreamDispatchFailurePolicy",
			runkit.WithEventStreamDispatchFailurePolicy,
		},
		{
			"WithProjectionCompactionFailurePolicy",
			runkit.WithProjectionCompactionFailurePolicy,
		},
	}

	fields := []struct {
		name string
		set  func(*runkit.FailurePolicy, time.Duration)
	}{
		{
			"RetryDelay",
			func(p *runkit.FailurePolicy, d time.Duration) { p.RetryDelay = d },
		},
		{
			"TransientRetryDelayLimit",
			func(p *runkit.FailurePolicy, d time.Duration) { p.TransientRetryDelayLimit = d },
		},
		{
			"DeterministicRetryDelayLimit",
			func(p *runkit.FailurePolicy, d time.Duration) { p.DeterministicRetryDelayLimit = d },
		},
		{
			"ErrorLogThreshold",
			func(p *runkit.FailurePolicy, d time.Duration) { p.ErrorLogThreshold = d },
		},
	}

	for _, o := range options {
		t.Run(o.name, func(t *testing.T) {
			t.Run("it accepts the default policy", func(t *testing.T) {
				o.option(runkit.DefaultFailurePolicy())
			})

			for _, f := range fields {
				for _, d := range []time.Duration{0, -time.Second} {
					t.Run("it panics if "+f.name+" is "+d.String(), func(t *testing.T) {
						defer func() {
							if recover() == nil {
								t.Fatal("expected a panic")
							}
						}()

						policy := runkit.DefaultFailurePolicy()
						f.set(&policy, d)

						o.option(policy)
					})
				}
			}
		})
	}
}
