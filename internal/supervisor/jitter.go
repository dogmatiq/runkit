package supervisor

import (
	"math/rand/v2"
	"time"
)

// Jitter returns a random duration between half and all of d, inclusive.
//
// It is used to spread out delays that would otherwise end at the same time,
// to avoid thundering herds.
//
// d must not be negative.
func Jitter(d time.Duration) time.Duration {
	half := d / 2
	return half + rand.N(d-half+1)
}
