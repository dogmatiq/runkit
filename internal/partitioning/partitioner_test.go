package partitioning_test

import (
	"testing"

	"github.com/dogmatiq/enginekit/collections/maps"
	"github.com/dogmatiq/enginekit/collections/sets"
	"github.com/dogmatiq/enginekit/protobuf/uuidpb"
	. "github.com/dogmatiq/runkit/internal/partitioning"
	"github.com/dogmatiq/runkit/internal/x/xrapid"
	"pgregory.net/rapid"
)

func TestPartitioner(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		partitioner := &Partitioner{}
		targets := sets.NewProto[*uuidpb.UUID]()
		workloads := maps.NewProto[*uuidpb.UUID, *uuidpb.UUID]()

		t.Repeat(map[string]func(*rapid.T){
			"": func(t *rapid.T) {
				for workload, want := range workloads.All() {
					got := partitioner.SelectTarget(workload)

					if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection for workload %q: got %q, want %q",
							workload,
							got,
							want,
						)
					}
				}
			},
			"add new target": func(t *rapid.T) {
				target := uuidpb.Generate()
				partitioner.AddTarget(target)
				targets.Add(target)

				// Ensure that existing workloads either continue to use their
				// previous target, or start using the new target, but do not
				// switch to any other existing target.
				for workload, want := range workloads.All() {
					got := partitioner.SelectTarget(workload)

					if got.Equal(target) {
						workloads.Set(workload, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection for workload %q after addition of target %q: got %q, want %q",
							workload,
							target,
							got,
							want,
						)
					}
				}
			},
			"add existing target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				target := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				partitioner.AddTarget(target)
			},
			"remove existing target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				target := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				partitioner.RemoveTarget(target)
				targets.Remove(target)

				if targets.Len() == 0 {
					workloads.Clear()
					return
				}

				// Ensure that existing workloads that were assigned to the
				// removed target are reassigned to other targets, but no other
				// workloads change target.
				for workload, want := range workloads.All() {
					got := partitioner.SelectTarget(workload)

					if want.Equal(target) {
						workloads.Set(workload, got)
					} else if !got.Equal(want) {
						t.Fatalf(
							"non-deterministic target selection for workload %q after removal of target %q: got %q, want %q",
							workload,
							target,
							got,
							want,
						)
					}
				}
			},
			"remove unknown target": func(t *rapid.T) {
				partitioner.RemoveTarget(uuidpb.Generate())
			},
			"select target for workload": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				workload := uuidpb.Generate()
				got := partitioner.SelectTarget(workload)

				if !targets.Has(got) {
					t.Fatalf(
						"selected target %q is not in the set of known targets",
						got,
					)
				}

				workloads.Set(workload, got)
			},
			"select target for workload with same value as target": func(t *rapid.T) {
				if targets.Len() == 0 {
					t.Skip("no existing targets")
				}

				want := xrapid.SampledFromSeq(targets.All()).Draw(t, "existing target")
				got := partitioner.SelectTarget(want)

				if !got.Equal(want) {
					t.Fatalf(
						"non-deterministic target selection when workload == target: got %q, want %q",
						got,
						want,
					)
				}

				workloads.Set(want, want)
			},
		})
	})
}
