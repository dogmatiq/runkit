package failure

import "time"

// Policy is how the engine responds to the failures of one kind of work: how
// long it waits before each retry, and when it logs a streak of failures as an
// ERROR.
//
// Every field must be positive.
type Policy struct {
	// RetryDelay is the delay before the first retry after a failure.
	//
	// The delay doubles with each failure in the streak, up to a limit.
	RetryDelay time.Duration

	// TransientRetryDelayLimit is the longest that the delay before a retry
	// grows after a failure that is not deterministic.
	TransientRetryDelayLimit time.Duration

	// DeterministicRetryDelayLimit is the longest that the delay before a retry
	// grows after a deterministic failure.
	DeterministicRetryDelayLimit time.Duration

	// ErrorLogThreshold is how long a streak of transient failures can last
	// before its next failure is logged as an ERROR.
	ErrorLogThreshold time.Duration
}

// backoff returns the longest delay before the retry that follows a failure,
// given the number of failures in the streak before it.
//
// The delay is the retry delay doubled for each earlier failure, up to the
// limit for the kind of failure.
func (p Policy) backoff(earlierFailures uint64, deterministic bool) time.Duration {
	limit := p.TransientRetryDelayLimit
	if deterministic {
		limit = p.DeterministicRetryDelayLimit
	}

	backoff := min(p.RetryDelay, limit)

	for range earlierFailures {
		// Doubling would reach or pass the limit, and a loop that doubles
		// until then ends within 63 iterations, however many failures there
		// were.
		if backoff > limit/2 {
			return limit
		}

		backoff *= 2
	}

	return backoff
}
