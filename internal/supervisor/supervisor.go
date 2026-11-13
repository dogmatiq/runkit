// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xsql"
)

// quietPeriod is how long fn must run without failing for [Run] to end a
// streak of failures.
const quietPeriod = 30 * time.Second

// Run calls fn until it succeeds, fails fatally, or ctx is canceled.
//
// A nil return from fn is success. Once ctx is canceled, fn is not called again
// regardless of what it returned.
//
// A fatal error is returned to the caller without retrying, and is still a
// fatal error to that caller.
//
// Any other error is recorded as a failure by a [failure.Tracker] with the
// given description, and fn is called again after the delay that the tracker
// decides.
//
// A deterministic error is logged as an ERROR unless its streak has already
// logged one, and the delay after it is limited to 5 minutes instead of 15
// seconds.
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
	failures := &failure.Tracker{
		Description:  description,
		EngineLogger: engineLogger,
		QuietPeriod:  quietPeriod,
	}
	defer failures.Complete()

	for {
		failures.Succeeded(ctx)
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

		delay := failures.Failed(ctx, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}
