package failure

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

// Tracker keeps the failure streak of one piece of ongoing work in memory, such
// as consuming an event stream.
//
// It decides how long to wait before each retry, and writes each failure and
// recovery record according to the rules in the [logging guide].
//
// [logging guide]: https://github.com/dogmatiq/runkit/blob/main/docs/maintainers/logging.md
type Tracker struct {
	// Description describes the work as a verb phrase in its base form, such
	// as "consume event stream".
	//
	// It completes the log messages "attempt to ... failed" and "attempt to ...
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
func (t *Tracker) Failed(ctx context.Context, err error) time.Duration {
	return t.fail(ctx, err, transientMaxDelay, false)
}

// FailedDeterministically records a deterministic failure of the work, which
// retrying alone can't clear.
//
// It returns the delay before the next attempt.
func (t *Tracker) FailedDeterministically(ctx context.Context, err error) time.Duration {
	return t.fail(ctx, err, deterministicMaxDelay, true)
}

// Succeeded records a success of the work.
//
// If a streak is in progress, it ends at once if [Tracker.QuietPeriod] is zero.
// Otherwise it ends when the quiet period passes without another failure.
func (t *Tracker) Succeeded(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.failures == 0 || t.complete {
		return
	}

	if t.QuietPeriod == 0 {
		t.succeedingSince = time.Now()
		t.recover(ctx)
		return
	}

	if t.quiet != nil {
		return
	}

	t.succeedingSince = time.Now()

	var timer *time.Timer

	timer = time.AfterFunc(
		t.QuietPeriod,
		func() {
			t.m.Lock()
			defer t.m.Unlock()

			// The work is stopping, so nothing is written, and the streak
			// doesn't end.
			if ctx.Err() != nil {
				return
			}

			// A timer whose quiet period was canceled after it fired is no
			// longer t.quiet. It reads timer only once t.m is held, by which
			// time timer has been assigned.
			if t.quiet == timer && !t.complete {
				t.recover(ctx)
			}
		},
	)

	t.quiet = timer
}

// Complete records that the work is complete, so no more attempts are made.
//
// It cancels any quiet period in progress, and the work writes no more
// records. It's called when the work stops for good, such as at shutdown.
func (t *Tracker) Complete() {
	t.m.Lock()
	defer t.m.Unlock()

	t.complete = true
	t.cancelQuietPeriod()
}

// fail records a failure of the work.
//
// The backoff is limited to the given duration. It returns the delay before the
// next attempt. It must not be called with t.m held.
func (t *Tracker) fail(
	ctx context.Context,
	err error,
	limit time.Duration,
	deterministic bool,
) time.Duration {
	t.m.Lock()
	defer t.m.Unlock()

	t.cancelQuietPeriod()

	now := time.Now()
	t.lastFailure = now

	if t.failures == 0 {
		t.since = now
		t.backoff = baseDelay
	} else {
		t.backoff = min(2*t.backoff, limit)
	}

	t.failures++
	delay := Jitter(t.backoff)

	if t.complete {
		return delay
	}

	level := slog.LevelDebug
	attrs := []slog.Attr{
		xslog.Error(err),
		slog.Time("failing_since", t.since),
		slog.Uint64("failures", t.failures),
		slog.Duration("delay", delay),
	}

	needsError := deterministic
	if !now.Before(t.since.Add(maxDebugStreak)) {
		needsError = true
	}

	if needsError && !t.loggedError {
		t.loggedError = true
		level = slog.LevelError
		attrs = append(attrs, slog.Duration("failing_for", now.Sub(t.since)))
	}

	t.EngineLogger.LogAttrs(ctx, level, "attempt to "+t.Description+" failed", attrs...)

	return delay
}

// recover ends the streak in progress.
//
// It writes the streak's recovery record. It must be called with t.m held.
func (t *Tracker) recover(ctx context.Context) {
	level := slog.LevelDebug
	if t.loggedError {
		level = slog.LevelInfo
	}

	t.EngineLogger.LogAttrs(
		ctx,
		level,
		"attempt to "+t.Description+" succeeded after failures",
		slog.Time("failing_since", t.since),
		slog.Uint64("failures", t.failures),
		slog.Duration("failing_for", t.lastFailure.Sub(t.since)),
		slog.Duration("succeeding_for", time.Since(t.succeedingSince)),
	)

	t.cancelQuietPeriod()
	t.failures = 0
	t.backoff = 0
	t.since = time.Time{}
	t.lastFailure = time.Time{}
	t.succeedingSince = time.Time{}
	t.loggedError = false
}

// cancelQuietPeriod cancels the quiet period in progress, if any.
//
// It must be called with t.m held.
func (t *Tracker) cancelQuietPeriod() {
	if t.quiet != nil {
		t.quiet.Stop()
		t.quiet = nil
	}
}
