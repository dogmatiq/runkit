package adr10

import (
	"cmp"
	"math/rand/v2"
	"slices"
	"testing"
)

var algorithms = []struct {
	Name   string
	Search func(*Journal, EventOffset) *Record
}{
	{"binary search", BinarySearch},
	{"coarse-fine search", CoarseFineSearch},
	{"exponential search", ExponentialSearch},
	{"fenwick search", FenwickSearch},
	{"interpolation search by EWMA", InterpolationSearchByEWMA},
	{"interpolation search by mean", InterpolationSearchByMean},
	{"interpolation search by secant", InterpolationSearchBySecant},
}

// distributionModel models expected real-world event distribution: mostly
// single-event commands, occasional empty bookkeeping records, and rare small
// batches.
func distributionModel(pos, _ int, r *rand.Rand) uint64 {
	// Hypothetical bookkeeping record with no events every 1k records.
	if pos%1000 == 0 {
		return 0
	}

	prob := r.Float64()

	switch {
	// 0.5%: Very rarely, a command produces a large batch of events, or a large
	// number of append operations get batched into a single transaction.
	case prob < 0.005:
		return r.Uint64N(100)

	// 9.5%: Occasionally a command produces no events, or a small batch.
	case prob < 0.10:
		return r.Uint64N(5)

	// The vast majority of commands produce a single event.
	default:
		return 1
	}
}

// queryModel models expected real-world consumer access patterns:
// mostly tailing the stream, with some random historical access.
func queryModel(j *Journal, r *rand.Rand) EventOffset {
	var (
		_, pEnd = j.Bounds()
		prob    = r.Float64()
	)

	switch {
	// 1%: new consumer at start of stream.
	case prob < 0.01:
		return 0

		// 9%: random access
	case prob < 0.10:
		oEnd := j.Get(pEnd - 1).TransactionHeader.End
		return EventOffset(r.Uint64N(uint64(oEnd)))

		// 25%: random access at a transaction boundary.
	case prob < 0.35:
		return j.Get(JournalPosition(r.Uint64N(uint64(pEnd)))).TransactionHeader.Begin

		// 70%: live consumer resuming from a recent record boundary
	default:
		return j.Get(pEnd - 1 - JournalPosition(r.IntN(5))).TransactionHeader.Begin
	}
}

// journal is a shared fixture for all tests and benchmarks. It is populated
// with synthetic data according to the [distributionModel].
var journal = buildJournal()

func TestAlgorithms(t *testing.T) {
	rng := NewRNG(SeedForQueryModel)

	for range 1000 {
		target := queryModel(journal, rng)
		want := BinarySearch(journal, target)

		for _, algo := range algorithms {
			if algo.Name == "binary search" {
				continue
			}

			got := algo.Search(journal, target)

			if (got == nil) != (want == nil) {
				t.Errorf(
					"%s: offset %d: got %v, want %v",
					algo.Name, target, got, want,
				)
			} else if got != nil {
				if got.ContainsOffset(target) != LookNoFurther {
					t.Errorf(
						"%s: offset %d: pos %d does not contain target offset",
						algo.Name, target, got.Pos,
					)
				} else if got.Pos != want.Pos {
					t.Errorf(
						"%s: offset %d: got pos %d, want %d",
						algo.Name, target, got.Pos, want.Pos,
					)
				}
			}
		}
	}
}

func BenchmarkAlgorithms(b *testing.B) {
	type result struct {
		Algorithm      string
		MeanReadsPerOp float64
		MaxReads       int
	}

	var results []result

	for _, algo := range algorithms {
		b.Run(algo.Name, func(b *testing.B) {
			var (
				rng        = NewRNG(SeedForQueryModel)
				totalReads = 0
				maxReads   = 0
			)

			for b.Loop() {
				targetOffset := queryModel(journal, rng)

				journal.EnableMetrics()
				algo.Search(journal, targetOffset)
				journal.DisableMetrics()

				reads := journal.CaptureMetrics()
				totalReads += reads
				maxReads = max(maxReads, reads)
			}

			meanReads := float64(totalReads) / float64(b.N)
			b.ReportMetric(meanReads, "reads/op")

			results = append(results, result{
				algo.Name,
				meanReads,
				maxReads,
			})
		})
	}

	slices.SortFunc(results, func(a, b result) int {
		if c := cmp.Compare(a.MeanReadsPerOp, b.MeanReadsPerOp); c != 0 {
			return c
		}

		if c := cmp.Compare(a.MaxReads, b.MaxReads); c != 0 {
			return c
		}

		return cmp.Compare(a.Algorithm, b.Algorithm)
	})

	var pad int
	for _, r := range results {
		pad = max(pad, len(r.Algorithm))
	}

	for i, r := range results {
		b.Logf(
			"%2d) %-*s  %5.2f / %3d (mean / max, reads/op)",
			i+1,
			pad,
			r.Algorithm,
			r.MeanReadsPerOp,
			r.MaxReads,
		)
	}
}

// buildJournal returns a journal populated with synthetic data according to the
// [distributionModel].
func buildJournal() *Journal {
	rng := NewRNG(SeedForDistributionModel)

	j := &Journal{
		transactions: make([]Record, JournalSize),
	}

	var (
		eventsPerTransactionEWMA float64
		runningOffset            uint64
	)

	for pos := range j.transactions {
		count := distributionModel(pos, JournalSize, rng)
		countAsFloat := float64(count)

		if pos == 0 {
			eventsPerTransactionEWMA = countAsFloat
		} else {
			const ewmaAlpha = 0.125
			eventsPerTransactionEWMA += ewmaAlpha * (countAsFloat - eventsPerTransactionEWMA)
		}

		fenwickEvents := count
		for span := 1; (pos+1)%(span*2) == 0; span *= 2 {
			fenwickEvents += j.transactions[pos-span].TransactionHeader.FenwickEvents
		}

		txn := Record{Pos: JournalPosition(pos)}
		txn.TransactionHeader.Begin = EventOffset(runningOffset)
		txn.TransactionHeader.End = EventOffset(runningOffset + count)
		txn.TransactionHeader.EventsPerTransactionEWMA = eventsPerTransactionEWMA
		txn.TransactionHeader.FenwickEvents = fenwickEvents
		j.transactions[pos] = txn

		runningOffset += count
	}

	return j
}
