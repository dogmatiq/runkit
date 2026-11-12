// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

const (
	// baseDelay is the initial backoff duration.
	baseDelay = 100 * time.Millisecond

	// transientMaxDelay is the maximum backoff duration after a failure that
	// is not deterministic.
	transientMaxDelay = 15 * time.Second

	// deterministicMaxDelay is the maximum backoff duration after a
	// deterministic failure.
	deterministicMaxDelay = 5 * time.Minute

	// quietPeriod is how long fn must run without failing for [Run] to end a
	// streak of failures.
	quietPeriod = 30 * time.Second
)

// Run calls fn until it succeeds, fails fatally, or ctx is canceled.
//
// A nil return from fn is success. Once ctx is canceled, fn is not called again
// regardless of what it returned.
//
// A fatal error is returned to the caller without retrying, and is still a
// fatal error to that caller.
//
// Any other error is recorded as a failure of a [Work] with the given
// description, and fn is called again after the delay that the [Work] decides.
// A deterministic error is logged as an ERROR immediately, and [Run] waits
// longer before calling fn again than after any other failure.
//
// [Run] can't see whether fn is making progress, so a streak of failures ends
// once fn has run for the quiet period without failing. If fn returns nil
// during a streak, the streak ends without a recovery record.
//
// See [xerrors.IsFatal] and [xerrors.IsDeterministic].
func Run(
	ctx context.Context,
	engineLogger *slog.Logger,
	description string,
	fn func(context.Context) error,
) error {
	work := &Work{
		Description:  description,
		EngineLogger: engineLogger,
		QuietPeriod:  quietPeriod,
	}
	defer work.Complete()

	for {
		work.Succeeded(ctx)
		err := fn(ctx)

		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err == nil {
			return nil
		}

		if xerrors.IsFatal(err) {
			return err
		}

		xsql.PanicOnDeadlock(err)

		var delay time.Duration
		if xerrors.IsDeterministic(err) {
			delay = work.FailedDeterministically(ctx, err)
		} else {
			delay = work.Failed(ctx, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
