package metrics

import (
	"bytes"
	"cmp"
	"maps"
	"slices"

	"github.com/dogmatiq/protobuf/metricsgrpc"
)

// sampleKey identifies the metric and the subject of a sample. A sample
// replaces the earlier samples that have the same key.
type sampleKey struct {
	MetricIndex uint32
	Subject     string
}

// keyOf returns the key of a sample.
func keyOf(s *metricsgrpc.WatchMetricsResponse_Sample) sampleKey {
	return sampleKey{s.GetMetricIndex(), string(s.GetSubject())}
}

// Snapshot holds the latest sample of each metric and subject that a node has
// measured, as of one point in time.
//
// The zero value is the snapshot before the first measurement. Measuring the
// engine produces each later snapshot with [Snapshot.With]. A snapshot stays
// the same after that, so any number of readers can hold and read the same one.
// A reader that has seen an earlier snapshot, or the zero value, uses
// [Snapshot.ChangesSince] to find which samples are new to it.
type Snapshot struct {
	samples map[sampleKey]*metricsgrpc.WatchMetricsResponse_Sample
}

// ChangesSince returns the samples that are new in the snapshot, or that
// replace a sample of the earlier snapshot. They are in the order of their
// revisions, with the oldest first.
//
// It compares samples by identity. It relies on a sample staying the same once
// it is in a snapshot, so that a new sample replaces it as a whole.
func (s Snapshot) ChangesSince(
	earlier Snapshot,
) []*metricsgrpc.WatchMetricsResponse_Sample {
	var newer []*metricsgrpc.WatchMetricsResponse_Sample

	for key, sample := range s.samples {
		if earlier.samples[key] != sample {
			newer = append(newer, sample)
		}
	}

	slices.SortFunc(
		newer,
		func(a, b *metricsgrpc.WatchMetricsResponse_Sample) int {
			return cmp.Or(
				bytes.Compare(a.GetRevision(), b.GetRevision()),
				cmp.Compare(a.GetMetricIndex(), b.GetMetricIndex()),
				bytes.Compare(a.GetSubject(), b.GetSubject()),
			)
		},
	)

	return newer
}

// With returns a snapshot that has the given samples in addition to the
// snapshot's own, or in place of its own samples with the same keys.
//
// If a given sample has the same value as the snapshot's sample with the same
// key, the new snapshot keeps the earlier sample.
func (s Snapshot) With(
	samples []*metricsgrpc.WatchMetricsResponse_Sample,
) Snapshot {
	next := Snapshot{
		samples: make(
			map[sampleKey]*metricsgrpc.WatchMetricsResponse_Sample,
			len(s.samples)+len(samples),
		),
	}

	maps.Copy(next.samples, s.samples)

	for _, sample := range samples {
		key := keyOf(sample)

		old, ok := s.samples[key]
		if ok && bytes.Equal(old.GetValue(), sample.GetValue()) {
			continue
		}

		next.samples[key] = sample
	}

	return next
}
