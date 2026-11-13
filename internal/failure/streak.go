package failure

import (
	"context"
	"log/slog"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
)

// maxDebugStreak is how long a streak of transient failures can last before
// its next failure is logged as an ERROR.
const maxDebugStreak = 30 * time.Second

// Streak is the state of a streak of consecutive failures of one piece of work.
//
// A streak lasts from its first failure until the work recovers. The zero
// value means that no streak is in progress.
//
// Its methods write each failure and recovery record according to the rules in
// the [logging guide].
//
// [logging guide]: https://github.com/dogmatiq/runkit/blob/main/docs/maintainers/logging.md
type Streak struct {
	// Failures is the number of failures in the streak.
	Failures uint64

	// FirstFailedAt is the time of the first failure in the streak.
	FirstFailedAt time.Time

	// HasLoggedError is true if one of the streak's failures has been logged as
	// an ERROR.
	HasLoggedError bool
}

// Extend extends the streak with a failure of the work with the given
// description, which starts the streak if none is in progress. It returns how
// long to wait before the next attempt.
//
// It updates s to include the failure, and writes the failure record to
// engineLogger. It's an ERROR record if none of the streak's failures has been
// logged as an ERROR, and the failure is deterministic or the streak is at
// least 30 seconds old. Otherwise, it's a DEBUG record.
//
// Before [Jitter] is applied, the delay doubles with each failure in the
// streak, from 100 milliseconds up to 15 seconds, or up to 5 minutes if the
// failure is deterministic.
//
// See [xerrors.IsDeterministic].
func (s *Streak) Extend(
	ctx context.Context,
	engineLogger *slog.Logger,
	description string,
	err error,
) time.Duration {
	now := time.Now()
	deterministic := xerrors.IsDeterministic(err)

	limit := transientMaxDelay
	if deterministic {
		limit = deterministicMaxDelay
	}

	// The shift is capped so that it can't overflow. Any shift of 12 or more
	// already exceeds both limits, so the cap doesn't change the delay.
	backoff := min(baseDelay<<min(s.Failures, 32), limit)
	delay := Jitter(backoff)

	if s.Failures == 0 {
		s.FirstFailedAt = now
	}
	s.Failures++

	level := slog.LevelDebug
	if s.qualifiesForError(deterministic, now) {
		s.HasLoggedError = true
		level = slog.LevelError
	}

	engineLogger.LogAttrs(
		ctx,
		level,
		"attempt to "+description+" failed",
		xslog.Error(err),
		slog.Uint64("failures", s.Failures),
		slog.Time("failing_since", s.FirstFailedAt),
		slog.Duration("failing_for", now.Sub(s.FirstFailedAt)),
		slog.Time("next_attempt_at", now.Add(delay)),
		slog.Duration("next_attempt_in", delay),
	)

	return delay
}

// End ends the streak, because the work with the given description has
// recovered.
//
// If a streak is in progress, it writes the streak's recovery record to
// engineLogger. It's an INFO record if one of the streak's failures was logged
// as an ERROR, and a DEBUG record otherwise.
func (s *Streak) End(
	ctx context.Context,
	engineLogger *slog.Logger,
	description string,
) {
	if s.Failures == 0 {
		return
	}

	level := slog.LevelDebug
	if s.HasLoggedError {
		level = slog.LevelInfo
	}

	engineLogger.LogAttrs(
		ctx,
		level,
		"attempt to "+description+" succeeded after failures",
		slog.Uint64("failures", s.Failures),
		slog.Time("failing_since", s.FirstFailedAt),
		slog.Duration("failing_for", time.Since(s.FirstFailedAt)),
	)

	*s = Streak{}
}

// qualifiesForError returns true if a failure at the given time should be
// logged as an ERROR.
//
// At most one of a streak's failures is logged as an ERROR. It must be called
// after s.FirstFailedAt is set for the failure.
func (s *Streak) qualifiesForError(deterministic bool, now time.Time) bool {
	if s.HasLoggedError {
		return false
	}

	if deterministic {
		return true
	}

	return !now.Before(s.FirstFailedAt.Add(maxDebugStreak))
}
