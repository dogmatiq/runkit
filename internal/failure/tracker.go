package failure

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

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

	// Policy is how the tracker responds to each failure of the work.
	Policy Policy

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

	// streak is the streak in progress, or the zero value if no streak is in
	// progress.
	streak Streak

	// succeedingSince is the time of the success that began the quiet period
	// in progress.
	succeedingSince time.Time

	// quiet is the timer that ends the streak when the quiet period passes, or
	// nil if no quiet period is running.
	quiet *time.Timer

	// complete is true once Complete has been called.
	complete bool
}

// WorkFailed records a failure of the work.
//
// It returns the time at which the next attempt may start. See
// [Streak.Extend].
func (t *Tracker) WorkFailed(ctx context.Context, err error) time.Time {
	t.m.Lock()
	defer t.m.Unlock()

	// No more attempts are made, so there's no time for the next one.
	if t.complete {
		return time.Time{}
	}

	t.cancelQuietPeriod()

	return t.streak.Extend(ctx, t.Description, t.Policy, t.EngineLogger, err)
}

// WorkSucceeded records a success of the work.
//
// If a streak is in progress, it ends at once if [Tracker.QuietPeriod] is zero.
// Otherwise it ends when the quiet period passes without another failure.
func (t *Tracker) WorkSucceeded(ctx context.Context) {
	t.m.Lock()
	defer t.m.Unlock()

	if t.streak.Failures == 0 || t.complete {
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

// recover ends the streak in progress.
//
// It writes the streak's recovery record. It must be called with t.m held.
func (t *Tracker) recover(ctx context.Context) {
	t.cancelQuietPeriod()

	// With a quiet period, the streak ends when the quiet period passes, so the
	// record says when the work began succeeding. Without one, the streak ends
	// at the success itself, and there's nothing to add.
	engineLogger := t.EngineLogger
	if t.QuietPeriod > 0 {
		engineLogger = engineLogger.With(
			slog.Time("succeeding_since", t.succeedingSince),
			slog.Duration("succeeding_for", time.Since(t.succeedingSince)),
		)
	}

	t.streak.End(ctx, t.Description, engineLogger)
	t.succeedingSince = time.Time{}
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
