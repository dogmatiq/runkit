// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/dogmatiq/enginekit/x/xsync"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// baseDelay is the initial backoff duration.
	baseDelay = 100 * time.Millisecond

	// transientMaxDelay is the maximum backoff duration after a failure that
	// is not created by [Persistent].
	transientMaxDelay = 15 * time.Second

	// persistentMaxDelay is the maximum backoff duration after a failure
	// created by [Persistent].
	persistentMaxDelay = 5 * time.Minute

	// resetThreshold is how long fn must run before failing for the backoff to
	// be reset to [baseDelay].
	resetThreshold = 3 * time.Minute
)

// Go calls [Run] in a new goroutine, with a context derived from ctx.
//
// It returns a future that holds the error returned by [Run] once it returns,
// and a function that cancels the context.
func Go(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) (*xsync.Future[error], context.CancelFunc) {
	ctx, cancel := context.WithCancel(ctx)
	result := &xsync.Future[error]{}

	go func() {
		result.Store(Run(ctx, logger, fn))
	}()

	return result, cancel
}

// Run calls fn until it succeeds, fails fatally, or ctx is canceled.
//
// A nil return from fn is success. An error created by [Fatal] is returned to
// the caller without retrying. Any other error is logged and fn is called again
// after a delay. Once ctx is canceled, fn is not called again regardless of
// what it returned.
//
// The delay doubles with each consecutive failure, up to a limit. The limit is
// higher when the error was created by [Persistent], so that failures unlikely
// to resolve themselves are retried less often. The delay returns to its
// initial value once fn runs for long enough before failing.
func Run(
	ctx context.Context,
	logger *slog.Logger,
	fn func(context.Context) error,
) error {
	var (
		backoff  time.Duration
		failures uint64
	)

	for {
		startedAt := time.Now()
		err := fn(ctx)

		if ctx.Err() != nil || err == nil {
			return ctx.Err()
		}

		if fatal, ok := errors.AsType[fatalError](err); ok {
			return fatal.err
		}

		xsql.PanicOnDeadlock(err)

		if backoff == 0 || time.Since(startedAt) >= resetThreshold {
			backoff = baseDelay
			failures = 0
		} else {
			backoff = min(2*backoff, persistentMaxDelay)
		}

		failures++

		limit := transientMaxDelay
		if _, ok := errors.AsType[persistentError](err); ok {
			limit = persistentMaxDelay
		}

		// Randomize between 50%-100% of the backoff to avoid thundering herds.
		capped := min(backoff, limit)
		delay := capped/2 + rand.N(capped/2)

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
