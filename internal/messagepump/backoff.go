package messagepump

import (
	"math/rand/v2"
	"time"
)

// BackoffDelay computes the existing capped, jittered application retry delay
// for a delivery that has failed n times. Component retries use supervisor.
func BackoffDelay(base, maximum time.Duration, n uint64) time.Duration {
	if base <= 0 || maximum <= base {
		return base
	}
	limit := base << n
	if limit <= 0 || limit > maximum {
		limit = maximum
	}
	return base + rand.N(limit-base)
}
