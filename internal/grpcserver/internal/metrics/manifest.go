package metrics

import (
	"fmt"
	"slices"

	"github.com/dogmatiq/protobuf/metricsgrpc"
	"google.golang.org/protobuf/proto"
)

// Manifest is the list of metrics that one reporter measures. A metric's
// position in the list is the metric_index of its samples, and the preamble of
// a call lists the metrics in the same order.
type Manifest struct {
	names   []string
	indexes map[string]int
}

// ManifestOption is an argument of [NewManifest]. Each option adds one metric
// to the manifest.
type ManifestOption func(*Manifest)

// WithMetric returns an option that adds the metric whose samples are messages
// of type M, such as [metricsgrpc.EventStreamLength], to a manifest.
//
// The metric's index is its position in the list, which follows the order of
// the options.
func WithMetric[M any, P interface {
	*M
	proto.Message
}]() ManifestOption {
	name := string(proto.MessageName(P(new(M))))

	return func(m *Manifest) {
		if _, ok := m.indexes[name]; ok {
			panic(fmt.Sprintf("%s is listed twice", name))
		}

		m.indexes[name] = len(m.names)
		m.names = append(m.names, name)
	}
}

// NewManifest returns a manifest that has one metric for each given option, in
// the order of the options.
//
// It panics if two options add the same metric, or if an option is nil.
func NewManifest(options ...ManifestOption) Manifest {
	m := Manifest{
		indexes: make(map[string]int, len(options)),
	}

	for _, apply := range options {
		apply(&m)
	}

	return m
}

// Names returns the full name of each metric's message, in the order of the
// list.
func (m Manifest) Names() []string {
	return slices.Clone(m.names)
}

// NewSample returns a sample of the metric that has the type of the given
// message, with the message as its value and the given subject. The sample's
// revision is unset.
//
// It panics if the manifest lacks the metric.
func (m Manifest) NewSample(
	subject []byte,
	value proto.Message,
) *metricsgrpc.WatchMetricsResponse_Sample {
	name := string(proto.MessageName(value))

	index, ok := m.indexes[name]
	if !ok {
		panic(fmt.Sprintf("%s is not in the manifest", name))
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
