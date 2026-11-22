package grpcserver

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"slices"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"google.golang.org/protobuf/proto"
)

// sampleKey identifies the metric and subject that a sample measures. A sample
// replaces the earlier samples that have the same key.
type sampleKey struct {
	MetricIndex uint32
	Subject     string
}

// keyOf returns the key of a sample.
func keyOf(s *metricsgrpc.WatchMetricsResponse_Sample) sampleKey {
	return sampleKey{s.GetMetricIndex(), string(s.GetSubject())}
}

// newSample returns a sample of the metric that value holds, for the given
// subject. The sample has no revision.
//
// It panics if the metric isn't one of those that the service supports.
func newSample(
	subject []byte,
	value proto.Message,
) *metricsgrpc.WatchMetricsResponse_Sample {
	name := string(proto.MessageName(value))

	index, ok := metricIndexes[name]
	if !ok {
		panic(fmt.Sprintf("%s is not a supported metric", name))
	}

	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(value)
	if err != nil {
		// The metric messages hold only UUIDs and numbers, so they always
		// marshal.
		panic(err)
	}

	return metricsgrpc.NewWatchMetricsResponse_SampleBuilder().
		WithMetricIndex(uint32(index)).
		WithSubject(subject).
		WithValue(data).
		Build()
}

// snapshot holds the latest sample of every metric and subject that the node
// has measured.
//
// A snapshot is never changed after it is shared, so every call that watches
// the metrics can read the same one.
type snapshot map[sampleKey]*metricsgrpc.WatchMetricsResponse_Sample

// ChangesSince returns the samples of the snapshot that the earlier snapshot
// doesn't have, because they are new or replaced the earlier ones. The oldest
// is first.
//
// It compares samples by identity, not by value. It relies on a sample being
// replaced as a whole, and never changed, once it is in a snapshot.
func (s snapshot) ChangesSince(
	earlier snapshot,
) []*metricsgrpc.WatchMetricsResponse_Sample {
	var newer []*metricsgrpc.WatchMetricsResponse_Sample

	for key, sample := range s {
		if earlier[key] != sample {
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
// A given sample whose value equals that of the snapshot's sample with the same
// key doesn't replace it, so the new snapshot keeps the earlier sample.
func (s snapshot) With(
	samples []*metricsgrpc.WatchMetricsResponse_Sample,
) snapshot {
	next := make(snapshot, len(s)+len(samples))
	maps.Copy(next, s)

	for _, sample := range samples {
		key := keyOf(sample)

		old, ok := s[key]
		if ok && bytes.Equal(old.GetValue(), sample.GetValue()) {
			continue
		}

		next[key] = sample
	}

	return next
}
