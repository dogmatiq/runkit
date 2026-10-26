// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// baseDelay is the initial backoff duration.
	baseDelay = 100 * time.Millisecond

	// maxDelay is the maximum backoff duration.
	maxDelay = 15 * time.Second

	// resetThreshold is how long fn must go without failing before the backoff
	// is reset to [baseDelay].
	resetThreshold = 3 * time.Minute
)

// Group runs supervised functions in their own goroutines and waits for them
// to finish.
//
// It is used in the same way as [sync.WaitGroup]: the zero value is ready to
// use, and it must not be copied after first use.
type Group struct {
	wg sync.WaitGroup
}

// Go calls fn in a new goroutine until it succeeds or ctx is canceled.
//
// A nil return from fn is success. Any error is logged and fn is called again
// after an exponential backoff. Once ctx is canceled, fn is not called again
// regardless of what it returned.
//
// The group is an isolation boundary: nothing that happens within fn is
// reported to the caller. Failures that can not be recovered by retrying are
// programming errors and should panic.
//
// fn is always called with the same ctx, so state it keeps outside of its own
// scope persists across calls.
func (g *Group) Go(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) {
	g.wg.Go(func() {
		run(ctx, logger, fn)
	})
}

// Wait blocks until every function started with [Group.Go] has finished.
func (g *Group) Wait() {
	g.wg.Wait()
}

// run calls fn until it succeeds or ctx is canceled, as described by
// [Group.Go].
func run(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) {
	var (
		backoff  time.Duration
		failedAt time.Time
		failures uint64
	)

	for {
		err := fn(ctx)

		if ctx.Err() != nil || err == nil {
			return
		}

		xsql.PanicOnDeadlock(err)

		now := time.Now()

		if backoff == 0 || now.Sub(failedAt) >= resetThreshold {
			backoff = baseDelay
			failures = 0
		} else {
			backoff = min(2*backoff, maxDelay)
		}

		failedAt = now
		failures++

		// Randomize between 50%-100% of the backoff to avoid thundering herds.
		delay := backoff/2 + rand.N(backoff/2)

		logger.ErrorContext(
			ctx,
			"component failed, restarting after delay",
			xslog.Error(err),
			slog.Uint64("failures", failures),
			slog.Duration("delay", delay),
		)

		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}
