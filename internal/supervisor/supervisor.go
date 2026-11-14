// Package supervisor runs fallible functions until they succeed, retrying
// with exponential backoff when they fail.
package supervisor

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/runkit/internal/failure"
	"github.com/dogmatiq/runkit/internal/x/xerrors"
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
// given policy and description, and fn is called again at the time that the
// tracker returns.
//
// A deterministic error is logged as an ERROR unless its streak has already
// logged one, and the delay after it is limited by the policy's
// DeterministicRetryDelayLimit instead of its TransientRetryDelayLimit.
//
// [Run] can't see whether fn is making progress, so a streak of failures ends
// once fn has run for the quiet period without failing. If fn returns nil
// during a streak, the streak ends without a recovery record.
//
// See [xerrors.IsFatal] and [xerrors.IsDeterministic].
func Run(
	ctx context.Context,
	description string,
	policy failure.Policy,
	engineLogger *slog.Logger,
	fn func(context.Context) error,
) error {
	failures := &failure.Tracker{
		Description:  description,
		QuietPeriod:  quietPeriod,
		Policy:       policy,
		EngineLogger: engineLogger,
	}
	defer failures.Complete()

	for {
		failures.WorkSucceeded(ctx)
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

		nextAttemptAt := failures.WorkFailed(ctx, err)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Until(nextAttemptAt)):
		}
	}
}
