package backoff

import (
	"context"
	"math/rand/v2"
	"time"
)

const (
	// base is the initial backoff duration.
	base = 100 * time.Millisecond

	// limit is the maximum backoff duration.
	limit = 15 * time.Second

	// reset is the duration after which the backoff delay is reset if no errors
	// occur.
	reset = 3 * time.Minute
)

// Backoff provides a simple exponential backoff mechanism for use within
// components that must retry operations after failures.
//
// There is no notion of a "successful" attempt. The backoff delay is reset
// after no errors occur for [reset] duration.
type Backoff struct {
	delay     time.Duration
	lastError time.Time
}

// Wait blocks for the backoff delay or until the context is canceled.
//
// It returns true if the backoff duration was reached, or false if the context
// was canceled.
func (b *Backoff) Wait(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return false
	case <-b.Chan():
		return true
	}
}

// Chan returns a channel that will receive a single time.Time value after the
// backoff delay has elapsed.
func (b *Backoff) Chan() <-chan time.Time {
	now := time.Now()

	if b.delay == 0 || now.Sub(b.lastError) >= reset {
		b.delay = base
	} else {
		b.delay = min(2*b.delay, limit)
	}

	b.lastError = now

	// Randomize between 50%-100% of the nominal delay (i.e. "equal jitter"), to
	// prevent thundering herd problems.
	delay := b.delay/2 + rand.N(b.delay/2)

	return time.After(delay)
}
