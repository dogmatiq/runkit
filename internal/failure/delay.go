package failure

import "time"

const (
	// baseDelay is the initial backoff duration.
	baseDelay = 100 * time.Millisecond

	// transientMaxDelay is the maximum backoff duration after a failure that
	// is not deterministic.
	transientMaxDelay = 15 * time.Second

	// deterministicMaxDelay is the maximum backoff duration after a
	// deterministic failure.
	deterministicMaxDelay = 5 * time.Minute
)
