package adr10

import "math/rand/v2"

const (
	// SeedForDistributionModel is the seed used by [distributionModel] for
	// generating the event counts of transactions in the benchmark journal.
	//
	// This is a fixed value to ensure that the same distribution is used across
	// all benchmark runs, for reproducibility.
	//
	// The value itself is arbitrary.
	SeedForDistributionModel = 4308934890

	// SeedForQueryModel is the seed used by [queryModel] for generating the
	// target offsets of search queries in the benchmark.
	//
	// This is a fixed value to ensure that the same sequence of queries is used
	// across all benchmark runs, for reproducibility.
	//
	// The value itself is arbitrary.
	SeedForQueryModel = 19540003145
)

// NewRNG returns a new pseudo-random number generator seeded with the given
// seed, for reproducible results.
func NewRNG(seed int) *rand.Rand {
	return rand.New(rand.NewPCG(uint64(seed), 0))
}
