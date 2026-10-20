package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	// baseDelay is the initial backoff duration.
	baseDelay = 100 * time.Millisecond

	// maxDelay is the maximum backoff duration.
	maxDelay = 15 * time.Second

	// resetThreshold is the duration after which the backoff delay is
	// reset to [baseDelay] if no errors occur.
	resetThreshold = 3 * time.Minute
)

// Backoff provides an exponential backoff mechanism for use within components
// that must retry operations after failures.
type Backoff struct {
	delay   time.Duration
	errorAt time.Time
}

// Wait blocks for the backoff delay or until the context is canceled.
//
// It returns true if the entire backoff delay elapses, or false if ctx is
// canceled.
//
// Each call blocks for an increasing amount of time, up to a maximum of
// [maxDelay]. If no errors occur for [resetThreshold], the backoff delay is
// reset to [baseDelay].
func (b *Backoff) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-b.Chan():
		return true
	}
}

// Chan returns a channel that receives a single [time.Time] value after the
// backoff delay has elapsed.
func (b *Backoff) Chan() <-chan time.Time {
	now := time.Now()

	if b.delay == 0 || now.Sub(b.errorAt) >= resetThreshold {
		b.delay = baseDelay
	} else {
		b.delay = min(2*b.delay, maxDelay)
	}

	b.errorAt = now

	// Randomize between 50%-100% of the nominal delay (i.e. "equal jitter"), to
	// prevent thundering herd problems.
	delay := b.delay/2 + rand.N(b.delay/2)

	return time.After(delay)
}
