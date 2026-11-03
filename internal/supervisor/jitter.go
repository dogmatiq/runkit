package supervisor

import (
	"math/rand/v2"
	"time"
)

// Jitter returns a random duration between half and all of d.
//
// It is used to spread out delays that would otherwise end at the same time,
// to avoid thundering herds.
func Jitter(d time.Duration) time.Duration {
	return d/2 + rand.N(d/2)
}
