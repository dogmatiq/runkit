package ewma

import (
	"math"
	"testing"
)

const (
	alpha     = 0.25
	tolerance = 0.001
)

var samples = []struct {
	Sample float64
	Want   float64
}{
	{200, 50},
	{50, 50},
	{200, 87.5},
	{0, 65.625},
	{100, 74.218},
	{50, 68.164},
}

func TestCompute(t *testing.T) {
	t.Run("it computes the average correctly", func(t *testing.T) {
		avg := 0.0

		for idx, sample := range samples {
			avg = Compute(avg, sample.Sample, alpha)

			if math.Abs(avg-sample.Want) <= tolerance {
				t.Logf("[%d] incorporate %f, average is %f", idx, sample.Sample, avg)
			} else {
				t.Fatalf("[%d] incorporate %f, unexpected average: got %f, want %f", idx, sample.Sample, avg, sample.Want)
			}
		}
	})

	t.Run("it does not allocate", func(t *testing.T) {
		var avg float64

		if allocs := testing.AllocsPerRun(100, func() {
			avg = Compute(avg, 100, alpha)
		}); allocs != 0 {
			t.Fatalf("expected zero allocations, got %f", allocs)
		}
	})
}

func TestUpdate(t *testing.T) {
	t.Run("it updates the average correctly", func(t *testing.T) {
		avg := 0.0

		for idx, sample := range samples {
			Update(&avg, sample.Sample, alpha)

			if math.Abs(avg-sample.Want) <= tolerance {
				t.Logf("[%d] incorporate %f, average is %f", idx, sample.Sample, avg)
			} else {
				t.Fatalf("[%d] incorporate %f, unexpected average: got %f, want %f", idx, sample.Sample, avg, sample.Want)
			}
		}
	})

	t.Run("it does not allocate", func(t *testing.T) {
		var avg float64

		if allocs := testing.AllocsPerRun(100, func() {
			Update(&avg, 100, alpha)
		}); allocs != 0 {
			t.Fatalf("expected zero allocations, got %f", allocs)
		}
	})
}
