package xtesting

import (
	"time"

	"github.com/dogmatiq/runkit/internal/failure"
)

// FailurePolicy returns a failure policy for the components that a test
// creates, with the values that the engine uses by default.
func FailurePolicy() failure.Policy {
	return failure.Policy{
		RetryDelay:                   100 * time.Millisecond,
		TransientRetryDelayLimit:     15 * time.Second,
		DeterministicRetryDelayLimit: 5 * time.Minute,
		ErrorLogThreshold:            30 * time.Second,
	}
}
