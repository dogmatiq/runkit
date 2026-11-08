package supervisor

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// maxDebugStreak is the longest a streak of non-deterministic failures can
// last with its failures logged only at DEBUG.
const maxDebugStreak = 30 * time.Second

// Work is a piece of ongoing or repeated work that can fail, such as consuming
// an event stream.
//
// It tracks each streak of consecutive failures, decides how long to wait
// before each retry, and writes each failure and recovery record according to
// the rules in the [logging guide].
//
// [logging guide]: https://github.com/dogmatiq/runkit/blob/main/docs/maintainers/logging.md
type Work struct {
	// Description describes the work as a verb phrase in its base form, such
	// as "consume event stream".
	//
	// It completes the log messages "attempt to … failed" and "attempt to …
	// succeeded after failures". EngineLogger's attributes identify which
	// instance of the work it is.
	Description string

	// EngineLogger is the target for the work's records.
	//
	// Its attributes must identify the work.
	EngineLogger *slog.Logger

	// QuietPeriod is how long the work must go without failing after a success
	// before a streak ends.
	//
	// If it's zero, a streak ends at the success itself.
	QuietPeriod time.Duration

	// m guards the fields below it.
	m sync.Mutex

	// failures is the number of failures in the streak in progress, or zero if
	// no streak is in progress.
	failures uint64

	// backoff is the delay before the next attempt, before jitter is applied.
	backoff time.Duration

	// since is the time of the first failure in the streak in progress.
	since time.Time

	// lastFailure is the time of the most recent failure in the streak in
	// progress.
	lastFailure time.Time

	// succeedingSince is the time of the success that began the quiet period
	// in progress.
	succeedingSince time.Time

	// loggedError is true if the streak in progress has written an ERROR
	// record.
	loggedError bool

	// quiet is the timer that ends the streak when the quiet period passes, or
	// nil if no quiet period is running.
	quiet *time.Timer

	// complete is true once Complete has been called.
	complete bool
}

// Failed records a failure of the work.
//
// It returns the delay before the next attempt.
func (w *Work) Failed(ctx context.Context, err error) time.Duration {
	return w.fail(ctx, err, transientMaxDelay, false)
}

// FailedDeterministically records a deterministic failure of the work, which
// retrying alone can't clear.
//
// It returns the delay before the next attempt.
func (w *Work) FailedDeterministically(ctx context.Context, err error) time.Duration {
	return w.fail(ctx, err, deterministicMaxDelay, true)
}

// Succeeded records a success of the work.
//
// If a streak is in progress, it ends at once if [Work.QuietPeriod] is zero.
// Otherwise it ends when the quiet period passes without another failure.
func (w *Work) Succeeded(ctx context.Context) {
	w.m.Lock()
	defer w.m.Unlock()

	if w.failures == 0 || w.complete {
		return
	}

	if w.QuietPeriod == 0 {
		w.succeedingSince = time.Now()
		w.recover(ctx)
		return
	}

	if w.quiet != nil {
		return
	}

	w.succeedingSince = time.Now()

	var timer *time.Timer

	timer = time.AfterFunc(
		w.QuietPeriod,
		func() {
			w.m.Lock()
			defer w.m.Unlock()

			// The work is stopping, so nothing is written, and the streak
			// doesn't end.
			if ctx.Err() != nil {
				return
			}

			// A timer whose quiet period was canceled after it fired is no
			// longer w.quiet. It reads timer only once w.m is held, by which
			// time timer has been assigned.
			if w.quiet == timer && !w.complete {
				w.recover(ctx)
			}
		},
	)

	w.quiet = timer
}

// Complete records that the work is complete, so no more attempts are made.
//
// It cancels any quiet period in progress, and the work writes no more
// records. It's called when the work stops for good, such as at shutdown.
func (w *Work) Complete() {
	w.m.Lock()
	defer w.m.Unlock()

	w.complete = true
	w.cancelQuietPeriod()
}

// fail records a failure of the work.
//
// The backoff is limited to the given duration. It returns the delay before the
// next attempt. It must not be called with w.m held.
func (w *Work) fail(
	ctx context.Context,
	err error,
	limit time.Duration,
	deterministic bool,
) time.Duration {
	w.m.Lock()
	defer w.m.Unlock()

	w.cancelQuietPeriod()

	now := time.Now()
	w.lastFailure = now

	if w.failures == 0 {
		w.since = now
		w.backoff = baseDelay
	} else {
		w.backoff = min(2*w.backoff, limit)
	}

	w.failures++
	delay := Jitter(w.backoff)

	if w.complete {
		return delay
	}

	level := slog.LevelDebug
	attrs := []slog.Attr{
		xslog.Error(err),
		slog.Time("failing_since", w.since),
		slog.Uint64("failures", w.failures),
		slog.Duration("delay", delay),
	}

	needsError := deterministic
	if !now.Before(w.since.Add(maxDebugStreak)) {
		needsError = true
	}

	if needsError && !w.loggedError {
		w.loggedError = true
		level = slog.LevelError
		attrs = append(attrs, slog.Duration("failing_for", now.Sub(w.since)))
	}

	w.EngineLogger.LogAttrs(ctx, level, "attempt to "+w.Description+" failed", attrs...)

	return delay
}

// recover ends the streak in progress.
//
// It writes the streak's recovery record. It must be called with w.m held.
func (w *Work) recover(ctx context.Context) {
	level := slog.LevelDebug
	if w.loggedError {
		level = slog.LevelInfo
	}

	w.EngineLogger.LogAttrs(
		ctx,
		level,
		"attempt to "+w.Description+" succeeded after failures",
		slog.Time("failing_since", w.since),
		slog.Uint64("failures", w.failures),
		slog.Duration("failing_for", w.lastFailure.Sub(w.since)),
		slog.Duration("succeeding_for", time.Since(w.succeedingSince)),
	)

	w.cancelQuietPeriod()
	w.failures = 0
	w.backoff = 0
	w.since = time.Time{}
	w.lastFailure = time.Time{}
	w.succeedingSince = time.Time{}
	w.loggedError = false
}

// cancelQuietPeriod cancels the quiet period in progress, if any.
//
// It must be called with w.m held.
func (w *Work) cancelQuietPeriod() {
	if w.quiet != nil {
		w.quiet.Stop()
		w.quiet = nil
	}
}
