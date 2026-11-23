package dutycycle

import (
	"context"
	"math"
	"time"
)

// Run calls fn repeatedly, waiting between calls so that fn is running for at
// most a given fraction of the time.
//
// It stops when ctx is canceled or fn returns an error. It returns ctx's error
// in the first case, and fn's error in the second. It stops calling fn as soon
// as ctx is canceled.
//
// The first call starts immediately. Each later call starts as soon as two
// limits allow:
//
//   - It starts at least minInterval after the start of the previous call.
//   - It starts late enough to keep the duty cycle of fn at or below
//     maxDutyCycle. A duty cycle is the fraction of the time during which fn is
//     running, counted from the start of a call to the start of the next. For
//     example, if maxDutyCycle is 0.1, the next call starts at least 20 seconds
//     after the start of a call that took 2 seconds.
//
// It panics unless minInterval is positive and maxDutyCycle is above 0 and at
// most 1.
func Run(
	ctx context.Context,
	minInterval time.Duration,
	maxDutyCycle float64,
	fn func(context.Context) error,
) error {
	if minInterval <= 0 {
		panic("minimum interval must be positive")
	}

	if maxDutyCycle <= 0 || maxDutyCycle > 1 {
		panic("maximum duty cycle must be above 0 and at most 1")
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		started := time.Now()

		if err := fn(ctx); err != nil {
			return err
		}

		// The duty cycle of the call is its elapsed time divided by the time
		// from its start to the next call's start, so the shortest time that
		// keeps it at maxDutyCycle is the elapsed time divided by maxDutyCycle.
		// Rounding up keeps the duty cycle from exceeding maxDutyCycle.
		elapsed := time.Since(started)
		dutyCycleLimit := time.Duration(math.Ceil(float64(elapsed) / maxDutyCycle))
		next := started.Add(max(minInterval, dutyCycleLimit))

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(next)):
		}
	}
}
