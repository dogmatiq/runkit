// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"errors"
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

// Go calls [Run] in a new goroutine.
//
// The error returned by [Run] is discarded, so fn must not return an error
// created by [Fatal].
func (g *Group) Go(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) {
	g.wg.Go(func() {
		Run(ctx, logger, fn)
	})
}

// Wait blocks until every function started with [Group.Go] has finished.
func (g *Group) Wait() {
	g.wg.Wait()
}

// Run calls fn until it succeeds, fails fatally, or ctx is canceled.
//
// A nil return from fn is success. An error created by [Fatal] is returned to
// the caller without retrying. Any other error is logged and fn is called again
// after an exponential backoff. Once ctx is canceled, fn is not called again
// regardless of what it returned.
//
// fn is always called with the same ctx, so state it keeps outside of its own
// scope persists across calls.
//
// It returns nil if fn succeeded, the error passed to [Fatal] if fn failed
// fatally, otherwise ctx's error.
func Run(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) error {
	var (
		backoff  time.Duration
		failedAt time.Time
		failures uint64
	)

	for {
		err := fn(ctx)

		if ctx.Err() != nil || err == nil {
			return ctx.Err()
		}

		if fatal, ok := errors.AsType[fatalError](err); ok {
			return fatal.err
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
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Fatal returns an error that causes [Run] to stop retrying and return err.
//
// It is used for failures that retrying can not recover from.
func Fatal(err error) error {
	return fatalError{err}
}

type fatalError struct {
	err error
}

func (e fatalError) Error() string {
	return e.err.Error()
}

func (e fatalError) Unwrap() error {
	return e.err
}
