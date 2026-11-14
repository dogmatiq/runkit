package failure

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/dogmatiq/runkit/internal/x/xerrors"
	"github.com/dogmatiq/runkit/internal/x/xslog"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
)

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

	// FailureLoggedAsError is true if one of the streak's failures has been
	// logged as an ERROR.
	FailureLoggedAsError bool
}

// Extend extends the streak with a failure of the work with the given
// description, which starts the streak if none is in progress. It returns the
// time at which the next attempt may start.
//
// It updates s to include the failure, and writes the failure record to
// engineLogger. It's an ERROR record if none of the streak's failures has been
// logged as an ERROR, and the failure is deterministic or the streak is at
// least as old as policy.ErrorLogThreshold. Otherwise, it's a DEBUG record.
//
// The description is a verb phrase in its base form, such as "deliver
// message". It completes the record's log message, "attempt to ... failed".
//
// The time of the next attempt is the time of the failure plus a random delay.
// The delay is between half and all of a backoff, which starts at
// policy.RetryDelay and doubles with each failure in the streak, up to
// policy.TransientRetryDelayLimit, or policy.DeterministicRetryDelayLimit if
// the failure is deterministic.
//
// See [xerrors.IsDeterministic].
func (s *Streak) Extend(
	ctx context.Context,
	description string,
	policy Policy,
	engineLogger *slog.Logger,
	err error,
) time.Time {
	panicOnEngineBug(err)

	now := time.Now()
	deterministic := xerrors.IsDeterministic(err)

	delay := jitter(policy.backoff(s.Failures, deterministic))
	nextAttemptAt := now.Add(delay)

	if s.Failures == 0 {
		s.FirstFailedAt = now
	}
	s.Failures++

	level := slog.LevelDebug
	if s.qualifiesForError(policy, deterministic, now) {
		s.FailureLoggedAsError = true
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
		slog.Time("next_attempt_at", nextAttemptAt),
		slog.Duration("next_attempt_in", delay),
	)

	return nextAttemptAt
}

// End ends the streak, because the work with the given description has
// recovered.
//
// If a streak is in progress, it writes the streak's recovery record to
// engineLogger. It's an INFO record if one of the streak's failures was logged
// as an ERROR, and a DEBUG record otherwise.
//
// The description is a verb phrase in its base form, as for [Streak.Extend].
// It completes the record's log message, "attempt to ... succeeded after
// failures".
func (s *Streak) End(
	ctx context.Context,
	description string,
	engineLogger *slog.Logger,
) {
	if s.Failures == 0 {
		return
	}

	level := slog.LevelDebug
	if s.FailureLoggedAsError {
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
func (s *Streak) qualifiesForError(
	policy Policy,
	deterministic bool,
	now time.Time,
) bool {
	if s.FailureLoggedAsError {
		return false
	}

	if deterministic {
		return true
	}

	return !now.Before(s.FirstFailedAt.Add(policy.ErrorLogThreshold))
}

// panicOnEngineBug panics if err shows a bug in the engine, but only when the
// program is running under go test.
//
// The difference is intentional. In production, the failure is retried like any
// other, so the engine recovers by itself. Under go test, the panic makes a
// developer find out about the bug immediately, instead of seeing only a
// retried failure.
func panicOnEngineBug(err error) {
	if !testing.Testing() {
		return
	}

	// A deadlock means that two pieces of the engine's code lock rows in
	// different orders.
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
		if pgErr.Code == pgerrcode.DeadlockDetected {
			panic(fmt.Sprintf("deadlock: %s", err))
		}
	}
}
