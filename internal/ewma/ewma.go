package ewma

import (
	"golang.org/x/exp/constraints"
)

// Compute returns the exponentially weighted moving average of a sample.
//
// avg is the historical average value to update. sample is the new value to
// incorporate.
//
// alpha is the smoothing factor, which determines the weight given to the new
// sample versus the historical average. A value closer to 1 gives more weight
// to the new sample.
func Compute[T constraints.Integer | constraints.Float](
	avg, sample T,
	alpha float64,
) T {
	Update(&avg, sample, alpha)
	return avg
}

// Update updates an exponentially-weighted moving average in place.
//
// avg is a pointer to the historical average value to update. sample is the new
// value to incorporate.
//
// alpha is the smoothing factor, which determines the weight given to the new
// sample versus the historical average. A value closer to 1 gives more weight
// to the new sample.
func Update[T constraints.Integer | constraints.Float](
	avg *T,
	sample T,
	alpha float64,
) {
	a := float64(*avg) * (1 - alpha)
	s := float64(sample) * alpha
	*avg = T(a + s)
}
